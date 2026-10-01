// discord-report performs bounded standalone report delivery, never gateway
// admission, model invocation, scheduler activation, or credential management.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"insane-search-migration/internal/reporting"
)

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 || (args[0] != "inspect" && args[0] != "publish") {
		fmt.Fprintln(stderr, "usage: discord-report inspect --config TARGET.json [--proxy-config PATH]\n       discord-report publish --config TARGET.json --state-dir PRIVATE_DIR --run-id STABLE_ID [--file REPORT.txt|-]")
		return 2
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	config := fs.String("config", "", "exact bot/guild/channel target JSON (required)")
	proxy := fs.String("proxy-config", "", "existing credential-free proxy JSON; omitted uses HTTPS_PROXY (no direct fallback)")
	timeout := fs.Duration("timeout", 5*time.Minute, "bounded total operation timeout (1s..15m)")
	var state, runID, file *string
	if args[0] == "publish" {
		state = fs.String("state-dir", "", "private owner-owned durable ledger directory (required)")
		runID = fs.String("run-id", "", "stable unique report run identifier (required)")
		file = fs.String("file", "-", "prepared UTF-8 report file, or - for stdin")
	}
	if fs.Parse(args[1:]) != nil {
		return 2
	}
	if fs.NArg() != 0 || *config == "" || *timeout < time.Second || *timeout > 15*time.Minute {
		fmt.Fprintln(stderr, "invalid_required_arguments")
		return 2
	}
	if args[0] == "publish" && (*state == "" || *runID == "") {
		fmt.Fprintln(stderr, "state_dir_and_run_id_required")
		return 2
	}
	base, cancelSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelSignal()
	ctx, cancel := context.WithTimeout(base, *timeout)
	defer cancel()
	t, err := reporting.LoadTarget(*config)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	var report []byte
	if args[0] == "publish" {
		input := stdin
		var f *os.File
		if *file != "-" {
			var fd int
			fd, err = syscall.Open(*file, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
			if err != nil {
				fmt.Fprintln(stderr, "report_open_failed")
				return 1
			}
			f = os.NewFile(uintptr(fd), "prepared-report")
			defer f.Close()
			st, e := f.Stat()
			if e != nil || !st.Mode().IsRegular() {
				fmt.Fprintln(stderr, "report_not_regular")
				return 1
			}
			input = f
		}
		report, err = readReport(ctx, input)
		if err != nil || len(report) > reporting.MaxReportBytes {
			fmt.Fprintln(stderr, "report_read_or_size_failed")
			return 1
		}
	}
	client, err := reporting.NewLiveClient(t, *proxy)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer client.Close()
	var out any
	if args[0] == "inspect" {
		out, err = client.Inspect(ctx)
	} else {
		out, err = client.Publish(ctx, *state, *runID, report)
	}
	result := struct {
		Result any    `json:"result"`
		Error  string `json:"error,omitempty"`
	}{Result: out}
	if err != nil {
		result.Error = err.Error()
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if e := enc.Encode(result); e != nil {
		fmt.Fprintln(stderr, "output_write_failed")
		return 1
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return 130
		}
		return 1
	}
	return 0
}

func readReport(ctx context.Context, input io.Reader) ([]byte, error) {
	type result struct {
		b   []byte
		err error
	}
	ready := make(chan result, 1)
	go func() {
		b, err := io.ReadAll(io.LimitReader(input, reporting.MaxReportBytes+1))
		ready <- result{b, err}
	}()
	select {
	case <-ctx.Done():
		return nil, errors.New("report_input_timed_out_or_cancelled")
	case r := <-ready:
		return r.b, r.err
	}
}
