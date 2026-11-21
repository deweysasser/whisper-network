package program

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/alecthomas/kong"
	"github.com/mattn/go-colorable"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var exitFunc = os.Exit

// Options is the structure of program options
type Options struct {
	Version kong.VersionFlag `help:"Show program version"`
	// VersionCmd VersionCmd `name:"version" cmd:"" help:"show program version"`

	Debug        bool   `group:"Info" help:"Show debugging information" env:"WHISPER_DEBUG"`
	OutputFormat string `group:"Info" enum:"auto,jsonl,terminal" default:"auto" help:"How to show program output (auto|terminal|jsonl)" env:"WHISPER_OUTPUT_FORMAT"`
	Quiet        bool   `group:"Info" help:"Be less verbose than usual" env:"WHISPER_QUIET"`

	Server             ServerCmd             `cmd:"" help:"Run the coordination server"`
	Worker             WorkerCmd             `cmd:"" help:"Run a transcription worker"`
	Submit             SubmitCmd             `cmd:"" help:"Submit media to a queue"`
	Status             StatusCmd             `cmd:"" help:"Show job status, or list a queue's jobs"`
	Fetch              FetchCmd              `cmd:"" help:"Download a finished transcript"`
	Retry              RetryCmd              `cmd:"" help:"Retry a failed job, or a queue's failed jobs"`
	Delete             DeleteCmd             `cmd:"" help:"Delete a job, or a queue's jobs in a state"`
	Bench              BenchCmd              `cmd:"" help:"Measure whisper-cli speed and memory on this machine"`
	Admin              AdminCmd              `cmd:"" help:"Offline administration of a server data directory"`
	SandboxExec        SandboxExecCmd        `cmd:"" name:"sandbox-exec" hidden:"" help:"Internal: confine and exec a child process"`
	DownloaderSelftest DownloaderSelftestCmd `cmd:"" name:"downloader-selftest" hidden:"" help:"Internal: run yt-dlp and deno under the fetch sandbox"`
}

// Parse calls the CLI parsing routines
func (program *Options) Parse(args []string) (*kong.Context, error) {
	parser, err := kong.New(program,
		kong.ShortUsageOnError(),
		kong.Description("Distribute Whisper transcription across an ad-hoc fleet of workers."),
		kong.Vars{"version": Version},
		kong.Exit(func(code int) { exitFunc(code) }),
	)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return parser.Parse(args)
}

// AfterApply runs after the options are parsed but before anything runs
func (program *Options) AfterApply() error {
	program.initLogging()
	return nil
}

func (program *Options) initLogging() {
	// Emit VictoriaLogs' default field names so /insert/jsonline needs no
	// field mapping (DESIGN.md §10.1).
	zerolog.TimestampFieldName = "_time"
	zerolog.MessageFieldName = "_msg"
	zerolog.LevelFieldName = "level"
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }

	switch {
	case program.Debug:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case program.Quiet:
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	var out io.Writer = os.Stdout

	if os.Getenv("TERM") == "" && runtime.GOOS == "windows" {
		out = colorable.NewColorableStdout()
	}

	if program.OutputFormat == "terminal" ||
		(program.OutputFormat == "auto" && isTerminal(os.Stdout)) {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: out})
	} else {
		log.Logger = log.Output(out)
	}
	hostname, _ := os.Hostname()
	log.Logger = log.Logger.With().
		Str("service", "whisper-network").
		Str("version", Version).
		Str("host", hostname).
		Logger()

	log.Logger.Debug().
		Str("program", os.Args[0]).
		Msg("Starting")
}

// isTerminal returns true if the file given points to a character device (i.e. a terminal)
func isTerminal(file *os.File) bool {
	if fileInfo, err := file.Stat(); err != nil {
		log.Err(err).Msg("Error running stat")
		return false
	} else {
		return (fileInfo.Mode() & os.ModeCharDevice) != 0
	}
}
