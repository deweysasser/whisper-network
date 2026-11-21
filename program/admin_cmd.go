package program

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/auth"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/server"
	"github.com/deweysasser/whisper-network/internal/store"
)

// AdminCmd administers a server's data directory offline, for bootstrap and
// automation (DESIGN.md §15 phase 1). It opens the SQLite database
// directly; SQLite's locking makes this safe alongside a running server.
// Changes are audited with the actor "admin-cli". Secrets (tokens,
// passwords) are printed once on stdout.
type AdminCmd struct {
	CreateClient  AdminCreateClientCmd  `cmd:"" name:"create-client" help:"Create an API client and print its token"`
	CreateUser    AdminCreateUserCmd    `cmd:"" name:"create-user" help:"Create a UI user"`
	ResetPassword AdminResetPasswordCmd `cmd:"" name:"reset-password" help:"Set a user's password"`
	Grant         AdminGrantCmd         `cmd:"" help:"Grant (or revoke) roles"`
	ListWorkers   AdminListWorkersCmd   `cmd:"" name:"list-workers" help:"List registered workers"`
	ApproveWorker AdminApproveWorkerCmd `cmd:"" name:"approve-worker" help:"Approve a worker for queues"`
}

// adminData is the data-directory flag shared by the admin subcommands.
type adminData struct {
	Data string `default:"/data" env:"WHISPER_DATA" help:"Server data directory"`
}

// adminEnv is an open store plus the audited operations over it.
type adminEnv struct {
	st  *store.Store
	ops *server.Ops
	out io.Writer
}

func (d adminData) open(ctx context.Context, logger zerolog.Logger) (*adminEnv, error) {
	// The same private directory the server makes: the admin command may be
	// the first to create the database.
	if err := prepareDataDir(d.Data, logger); err != nil {
		return nil, err
	}
	st, err := store.Open(ctx, store.Config{Path: server.DBPath(d.Data)})
	if err != nil {
		return nil, err
	}
	params := auth.DefaultPasswordParams()
	if _, err := server.Bootstrap(ctx, st, "", params, os.Stderr); err != nil {
		_ = st.Close()
		return nil, err
	}
	lg := obs.NewLogger(logger, obs.Mode("admin"))
	return &adminEnv{st: st, ops: &server.Ops{Store: st, Log: lg, PasswordParams: params}, out: os.Stdout}, nil
}

// parseGrants parses role=scope flags, e.g. submit=queue:public or admin=*.
func parseGrants(specs []string) ([]api.Grant, error) {
	out := make([]api.Grant, 0, len(specs))
	for _, s := range specs {
		role, scope, ok := strings.Cut(s, "=")
		if !ok || role == "" || scope == "" {
			return nil, fmt.Errorf("grant %q: want role=scope, e.g. submit=queue:public", s)
		}
		g := api.Grant{Role: api.Role(role), Scope: scope}
		if err := auth.ValidateGrant(g); err != nil {
			return nil, fmt.Errorf("grant %q: %w", s, err)
		}
		out = append(out, g)
	}
	return out, nil
}

// AdminCreateClientCmd creates an API client.
type AdminCreateClientCmd struct {
	adminData
	Name  string   `required:"" help:"Client name"`
	Grant []string `help:"role=scope grant, repeatable (e.g. submit=queue:public; submit_url lets a client submit URLs)"`
}

// Run creates the client and prints its token.
func (c *AdminCreateClientCmd) Run(ctx context.Context, logger zerolog.Logger) error {
	grants, err := parseGrants(c.Grant)
	if err != nil {
		return err
	}
	env, err := c.open(ctx, logger)
	if err != nil {
		return err
	}
	defer env.st.Close()
	cl, err := env.ops.CreateClient(ctx, server.CLIActor(), c.Name, grants)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(env.out, "client %s\ntoken %s\n", cl.ID, cl.Token)
	return err
}

// AdminCreateUserCmd creates a UI user.
type AdminCreateUserCmd struct {
	adminData
	Username string   `required:"" help:"Username"`
	Password string   `env:"WHISPER_NEW_PASSWORD" help:"Password (random, and must be changed, if unset)"`
	Grant    []string `help:"role=scope grant, repeatable (e.g. queue_admin=queue:public, submit_url=queue:public)"`
}

// Run creates the user and prints a generated password.
func (c *AdminCreateUserCmd) Run(ctx context.Context, logger zerolog.Logger) error {
	grants, err := parseGrants(c.Grant)
	if err != nil {
		return err
	}
	env, err := c.open(ctx, logger)
	if err != nil {
		return err
	}
	defer env.st.Close()
	u, err := env.ops.CreateUser(ctx, server.CLIActor(), api.CreateUserRequest{
		Username: c.Username, Password: c.Password, Grants: grants})
	if err != nil {
		return err
	}
	if u.Password != "" {
		_, err = fmt.Fprintf(env.out, "user %s\npassword %s\n", u.Username, u.Password)
	} else {
		_, err = fmt.Fprintf(env.out, "user %s\n", u.Username)
	}
	return err
}

// AdminResetPasswordCmd sets a user's password.
type AdminResetPasswordCmd struct {
	adminData
	Username string `required:"" help:"Username"`
	Password string `env:"WHISPER_NEW_PASSWORD" help:"New password (random, and must be changed at next login, if unset)"`
}

// Run resets the password, ends the user's sessions, and prints a
// generated password.
func (c *AdminResetPasswordCmd) Run(ctx context.Context, logger zerolog.Logger) error {
	env, err := c.open(ctx, logger)
	if err != nil {
		return err
	}
	defer env.st.Close()
	pw, err := env.ops.ResetPassword(ctx, server.CLIActor(), c.Username, c.Password)
	if err != nil {
		return err
	}
	if c.Password == "" {
		_, err = fmt.Fprintf(env.out, "password %s\n", pw)
	}
	return err
}

// AdminGrantCmd grants or revokes roles for a principal.
type AdminGrantCmd struct {
	adminData
	Principal string   `required:"" help:"user:<name>, client:<id> or worker:<id>"`
	Grant     []string `required:"" help:"role=scope, repeatable (queue roles: submit, submit_url, view_all, read_all, delete_all, queue_admin; fetch and process are for workers)"`
	Revoke    bool     `help:"Revoke the grants instead"`
}

// Run applies the grants.
func (c *AdminGrantCmd) Run(ctx context.Context, logger zerolog.Logger) error {
	grants, err := parseGrants(c.Grant)
	if err != nil {
		return err
	}
	env, err := c.open(ctx, logger)
	if err != nil {
		return err
	}
	defer env.st.Close()
	for _, g := range grants {
		if c.Revoke {
			err = env.ops.Revoke(ctx, server.CLIActor(), c.Principal, g)
		} else {
			err = env.ops.Grant(ctx, server.CLIActor(), c.Principal, g)
		}
		if err != nil {
			return fmt.Errorf("%s=%s: %w", g.Role, g.Scope, err)
		}
	}
	return nil
}

// AdminListWorkersCmd lists workers.
type AdminListWorkersCmd struct {
	adminData
}

// Run prints a table of workers, pending first.
func (c *AdminListWorkersCmd) Run(ctx context.Context, logger zerolog.Logger) error {
	env, err := c.open(ctx, logger)
	if err != nil {
		return err
	}
	defer env.st.Close()
	ws, err := env.st.ListWorkers(ctx)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(env.out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ID\tSTATE\tHOSTNAME\tSANDBOX\tQUEUES\tLAST SEEN\tDESCRIPTION")
	for i := range ws {
		w := &ws[i]
		gs, err := env.ops.WireGrants(ctx, w.Principal())
		if err != nil {
			return err
		}
		var queues []string
		for _, g := range gs {
			if g.Role == api.RoleProcess || g.Role == api.RoleFetch {
				queues = append(queues, strings.TrimPrefix(g.Scope, "queue:"))
			}
		}
		seen := "-"
		if w.LastSeenAt != nil {
			seen = w.LastSeenAt.Local().Format(time.DateTime)
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", w.ID, w.State, w.Facts.Hostname,
			w.Facts.SandboxLevel, strings.Join(queues, ","), seen, w.Facts.Description)
	}
	return tw.Flush()
}

// AdminApproveWorkerCmd approves a worker for queues.
type AdminApproveWorkerCmd struct {
	adminData
	ID    string   `required:"" help:"Worker ID"`
	Queue []string `help:"Queue the worker may serve, repeatable (grants process to processing workers, fetch to downloaders)"`
}

// Run approves the worker.
func (c *AdminApproveWorkerCmd) Run(ctx context.Context, logger zerolog.Logger) error {
	env, err := c.open(ctx, logger)
	if err != nil {
		return err
	}
	defer env.st.Close()
	return env.ops.ApproveWorker(ctx, server.CLIActor(), c.ID, c.Queue)
}
