package program

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/client"
)

// ClientFlags are shared by the client subcommands.
type ClientFlags struct {
	Server string `required:"" env:"WHISPER_SERVER" help:"Server URL, e.g. https://wn.example"`
	Token  string `required:"" env:"WHISPER_TOKEN" help:"Client API token (wnc_…)"`
}

func (f ClientFlags) client() (*client.Client, error) {
	return client.New(f.Server, client.WithToken(f.Token), client.WithHTTPClient(&http.Client{}),
		client.WithUserAgent("whisper-network-cli/"+Version))
}

func stdout(w io.Writer) io.Writer {
	if w == nil {
		return os.Stdout
	}
	return w
}

// SubmitCmd submits a media file, or a URL for a downloader worker to fetch,
// as a new job.
type SubmitCmd struct {
	ClientFlags    `embed:""`
	Queue          string `default:"public" help:"Queue to submit to"`
	Model          string `default:"small.en" help:"Model, e.g. small.en or large-v3-turbo"`
	Language       string `help:"Language code, or auto (server default: en)"`
	Weight         string `enum:",low,medium,high" default:"" help:"Weight class within the queue: low, medium or high"`
	Prompt         string `help:"Initial prompt (names, vocabulary); at most 800 characters"`
	Label          string `help:"Free-form label"`
	Source         string `help:"Where the media came from (recorded, never fetched; defaults to the URL with --url)"`
	URL            string `name:"url" help:"Media URL for a downloader worker to fetch, instead of FILE"`
	Metadata       string `help:"Opaque JSON object stored with the job"`
	IdempotencyKey string `help:"Retry-safe key: resubmitting with it returns the original job"`
	JSON           bool   `name:"json" help:"Print the job document instead of its ID"`
	File           string `arg:"" optional:"" type:"existingfile" help:"Media file (any audio or video)"`

	out io.Writer
}

// Validate enforces exactly one of FILE or --url (kong's xor does not cover
// positional arguments).
func (c *SubmitCmd) Validate() error {
	switch {
	case c.File == "" && c.URL == "":
		return errors.New("expected one of FILE or --url")
	case c.File != "" && c.URL != "":
		return errors.New("FILE and --url are mutually exclusive")
	}
	return nil
}

// Run submits the file, or the URL for a downloader to fetch.
func (c *SubmitCmd) Run(ctx context.Context) error {
	params := api.JobParams{Model: c.Model, Language: c.Language, Weight: api.Weight(c.Weight),
		Prompt: c.Prompt, Label: c.Label, Source: c.Source}
	if len([]rune(c.Prompt)) > api.MaxPromptLen {
		return fmt.Errorf("--prompt is %d characters; the limit is %d", len([]rune(c.Prompt)), api.MaxPromptLen)
	}
	if c.Metadata != "" {
		if !json.Valid([]byte(c.Metadata)) || !strings.HasPrefix(strings.TrimSpace(c.Metadata), "{") {
			return errors.New("--metadata must be a JSON object")
		}
		params.Metadata = json.RawMessage(c.Metadata)
	}
	cl, err := c.client()
	if err != nil {
		return err
	}
	if c.URL != "" {
		job, err := cl.SubmitURL(ctx, c.Queue, api.FetchRequest{JobParams: params, URL: c.URL}, c.IdempotencyKey)
		if err != nil {
			return err
		}
		return c.print(job)
	}
	f, err := os.Open(c.File)
	if err != nil {
		return err
	}
	defer f.Close()
	job, _, err := cl.Submit(ctx, c.Queue, params, f, client.SubmitOptions{
		IdempotencyKey: c.IdempotencyKey, Filename: filepath.Base(c.File)})
	if err != nil {
		return err
	}
	return c.print(job)
}

func (c *SubmitCmd) print(job *api.Job) error {
	out := stdout(c.out)
	if c.JSON {
		return printJSON(out, job)
	}
	_, err := fmt.Fprintln(out, job.ID)
	return err
}

// StatusCmd shows one job, or the visible jobs of a queue.
type StatusCmd struct {
	ClientFlags `embed:""`
	Queue       string `default:"public" help:"Queue to list when no job ID is given"`
	State       string `enum:",preparing,fetching,queued,running,done,failed" default:"" help:"Only list jobs in this state"`
	Sort        string `enum:"created,newest,state" default:"created" help:"List order: created (oldest first), newest, or state (running, waiting, failed, done)"`
	Limit       int    `default:"50" help:"Maximum jobs to list"`
	JobID       string `arg:"" optional:"" help:"Job ID: print its status document"`

	out io.Writer
}

// Run prints the status.
func (c *StatusCmd) Run(ctx context.Context) error {
	cl, err := c.client()
	if err != nil {
		return err
	}
	out := stdout(c.out)
	if c.JobID != "" {
		job, err := cl.Job(ctx, c.JobID)
		if err != nil {
			return err
		}
		return printJSON(out, job)
	}
	var jobs []api.Job
	opt := client.ListOptions{State: api.JobState(c.State), Limit: c.Limit}
	switch c.Sort {
	case "newest":
		opt.Sort, opt.Order = "created", "desc"
	case "state":
		opt.Sort = "state"
	}
	for len(jobs) < c.Limit || c.Limit <= 0 {
		page, err := cl.ListJobs(ctx, c.Queue, opt)
		if err != nil {
			return err
		}
		jobs = append(jobs, page.Jobs...)
		if page.NextCursor == "" || len(page.Jobs) == 0 {
			break
		}
		opt.Cursor = page.NextCursor
	}
	if c.Limit > 0 && len(jobs) > c.Limit {
		jobs = jobs[:c.Limit]
	}
	return printJobTable(out, jobs)
}

func printJobTable(out io.Writer, jobs []api.Job) error {
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSTATE\tWEIGHT\tMODEL\tTASKS\tAUDIO\tCREATED\tLABEL")
	for _, j := range jobs {
		audio := "-"
		if j.Media.DurationS > 0 {
			audio = (time.Duration(j.Media.DurationS) * time.Second).String()
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d/%d\t%s\t%s\t%s\n", j.ID, j.State, j.Weight, j.Model,
			j.Tasks.Done, j.Tasks.Total, audio, j.CreatedAt.Local().Format("2006-01-02 15:04"), j.Label)
	}
	return tw.Flush()
}

// FetchCmd downloads a finished job's transcript.
type FetchCmd struct {
	ClientFlags `embed:""`
	Output      string `short:"o" type:"path" help:"Write to this file instead of stdout"`
	JSON        bool   `name:"json" help:"Fetch result.json (segments with absolute times) instead of the text transcript"`
	Partial     bool   `help:"For a job that is not done (e.g. failed), fetch what is transcribed so far, gaps marked"`
	JobID       string `arg:"" help:"Job ID"`

	out io.Writer
}

// Run downloads the transcript.
func (c *FetchCmd) Run(ctx context.Context) error {
	cl, err := c.client()
	if err != nil {
		return err
	}
	var body io.Reader
	if c.JSON {
		get := cl.Result
		if c.Partial {
			get = cl.PartialResult
		}
		res, err := get(ctx, c.JobID)
		if err != nil {
			return notDoneHint(err, c.JobID)
		}
		b, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			return err
		}
		body = strings.NewReader(string(b) + "\n")
	} else {
		get := cl.Transcript
		if c.Partial {
			get = cl.PartialTranscript
		}
		rc, err := get(ctx, c.JobID)
		if err != nil {
			return notDoneHint(err, c.JobID)
		}
		defer rc.Close()
		body = rc
	}
	if c.Output == "" {
		_, err := io.Copy(stdout(c.out), body)
		return err
	}
	return writeFileFrom(c.Output, body)
}

func notDoneHint(err error, id string) error {
	if client.IsNotDone(err) {
		return fmt.Errorf("job %s is not done yet; check it with `status %s`, or fetch what exists with --partial: %w",
			id, id, err)
	}
	return err
}

// writeFileFrom writes r to path via a temporary file, so a failed download
// never leaves a truncated transcript under the final name. The file keeps
// os.CreateTemp's mode 0600: a transcript can carry meeting content, so it
// is not made readable to other local users.
func writeFileFrom(path string, r io.Reader) (err error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".part-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp.Name())
		}
	}()
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// RetryCmd retries one failed job, or every failed job of a queue matching
// a filter.
type RetryCmd struct {
	ClientFlags `embed:""`
	Queue       string    `default:"public" help:"Queue of the jobs to retry (without a job ID)"`
	State       string    `enum:"failed" default:"failed" help:"Retry the jobs in this state (without a job ID)"`
	Since       time.Time `help:"Only jobs that failed after this time (RFC 3339)"`
	DryRun      bool      `help:"List what would be retried, change nothing"`
	JSON        bool      `name:"json" help:"Print the server's response document"`
	JobID       string    `arg:"" optional:"" help:"Job ID: retry just this job"`

	out io.Writer
}

// Run retries.
func (c *RetryCmd) Run(ctx context.Context) error {
	cl, err := c.client()
	if err != nil {
		return err
	}
	out := stdout(c.out)
	if c.JobID != "" {
		if c.DryRun {
			return errors.New("--dry-run applies to a queue selection, not a single job")
		}
		job, err := cl.RetryJob(ctx, c.JobID)
		if err != nil {
			return err
		}
		if c.JSON {
			return printJSON(out, job)
		}
		_, err = fmt.Fprintf(out, "%s %s (retry %d)\n", job.ID, job.State, job.Retries)
		return err
	}
	f := &api.BulkFilter{State: api.JobState(c.State)}
	if !c.Since.IsZero() {
		f.Since = &c.Since
	}
	return runBulk(ctx, cl, out, c.Queue, api.BulkRequest{Action: api.BulkRetry, Filter: f, DryRun: c.DryRun}, c.JSON)
}

// DeleteCmd deletes one job, or every job of a queue in a state.
type DeleteCmd struct {
	ClientFlags `embed:""`
	Queue       string    `default:"public" help:"Queue of the jobs to delete (without a job ID)"`
	State       string    `enum:",preparing,fetching,queued,running,done,failed" default:"" help:"Delete the jobs in this state (required without a job ID)"`
	Since       time.Time `help:"Only jobs updated after this time (RFC 3339)"`
	DryRun      bool      `help:"List what would be deleted, change nothing"`
	JSON        bool      `name:"json" help:"Print the server's response document"`
	JobID       string    `arg:"" optional:"" help:"Job ID: delete just this job"`

	out io.Writer
}

// Run deletes.
func (c *DeleteCmd) Run(ctx context.Context) error {
	cl, err := c.client()
	if err != nil {
		return err
	}
	out := stdout(c.out)
	if c.JobID != "" {
		if c.DryRun || c.State != "" {
			return errors.New("--state and --dry-run apply to a queue selection, not a single job")
		}
		if err := cl.DeleteJob(ctx, c.JobID); err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "%s deleted\n", c.JobID)
		return err
	}
	if c.State == "" {
		return errors.New("give a job ID, or --state to delete a queue's jobs in that state")
	}
	f := &api.BulkFilter{State: api.JobState(c.State)}
	if !c.Since.IsZero() {
		f.Since = &c.Since
	}
	return runBulk(ctx, cl, out, c.Queue, api.BulkRequest{Action: api.BulkDelete, Filter: f, DryRun: c.DryRun}, c.JSON)
}

// runBulk sends a filter bulk request, following its cursor through every
// page, and prints the per-job outcomes and a tally.
func runBulk(ctx context.Context, cl *client.Client, out io.Writer, queue string, req api.BulkRequest, asJSON bool) error {
	all := api.BulkResult{Action: req.Action, DryRun: req.DryRun, Jobs: []api.BulkJobResult{}, Counts: map[string]int{}}
	for {
		res, err := cl.Bulk(ctx, queue, req)
		if err != nil {
			return err
		}
		all.Jobs = append(all.Jobs, res.Jobs...)
		for k, n := range res.Counts {
			all.Counts[k] += n
		}
		if res.NextCursor == "" || len(res.Jobs) == 0 {
			break
		}
		f := *req.Filter
		f.Cursor = res.NextCursor
		req.Filter = &f
	}
	if asJSON {
		return printJSON(out, all)
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tOUTCOME\tREASON")
	for _, j := range all.Jobs {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", j.JobID, j.Outcome, j.Reason)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	keys := make([]string, 0, len(all.Counts))
	for k := range all.Counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%d %s", all.Counts[k], strings.ReplaceAll(k, "_", " "))
	}
	if len(parts) == 0 {
		parts = []string{"no matching jobs"}
	}
	_, err := fmt.Fprintln(out, strings.Join(parts, ", "))
	return err
}

func printJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
