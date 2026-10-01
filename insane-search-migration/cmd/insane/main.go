// insane is the Linux VM collector/orchestration CLI. It does not run a model,
// deliver messages, activate cron, or replace ChatGPT's native web tools.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"insane-search-migration/internal/engine"
	"insane-search-migration/internal/headed"
	"insane-search-migration/internal/search"

	"insane-search-migration/internal/collector"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "insane:", e)
		os.Exit(1)
	}
}
func usage() {
	fmt.Fprintln(os.Stderr, "usage: insane scan|search|fetch|job [options]\n  scan --fixture testdata/scan.json --state /tmp/insane-state.json --report /tmp/insane-report.json\n  scan --dry-run --timeout 20m\n  search --query 'public search terms' --limit 5\n  fetch --url https://example.com --timeout 5m\n  job (prints disabled imported job; never schedules it)")
}
func projectRoot() string {
	if r := os.Getenv("INSANE_ROOT"); r != "" {
		return r
	}
	cwd, _ := os.Getwd()
	for _, start := range []string{cwd, filepath.Dir(os.Args[0])} {
		p, _ := filepath.Abs(start)
		for {
			if _, e := os.Stat(filepath.Join(p, "config/imported-job.disabled.json")); e == nil {
				return p
			}
			parent := filepath.Dir(p)
			if parent == p {
				break
			}
			p = parent
		}
	}
	return cwd
}
func run() error {
	if len(os.Args) < 2 {
		usage()
		return errors.New("command required")
	}
	root := projectRoot()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch os.Args[1] {
	case "scan":
		fs := flag.NewFlagSet("scan", flag.ContinueOnError)
		fixture := fs.String("fixture", "", "offline deterministic fixture JSON")
		statePath := fs.String("state", filepath.Join(root, "data/ai-promotion-scanner/state.json"), "state JSON (created only after a complete scan)")
		reportPath := fs.String("report", "", "optional report JSON path (stdout always receives the report)")
		dry := fs.Bool("dry-run", false, "do not save state; live reads still occur unless --fixture is set")
		timeout := fs.Duration("timeout", 20*time.Minute, "total scan deadline")
		opTimeout := fs.Duration("operation-timeout", 5*time.Minute, "deadline for each native fetch operation")
		profile := fs.String("profile", "wide", "collection profile: wide review queue or legacy parity")
		maxRows := fs.Int("max-scan-per-source", 0, "listing rows per source (1..200; 0 uses profile default)")
		maxPerSource := fs.Int("max-per-source", 0, "detail reads per source (1..24; 0 uses profile default)")
		maxTotal := fs.Int("max-total", 0, "total detail reads/candidates (1..96; 0 uses profile default)")
		maxPages := fs.Int("max-listing-pages", 0, "supported public listing pages per source (1..5; 0 uses profile default)")
		if e := fs.Parse(os.Args[2:]); e != nil {
			return e
		}
		if fs.NArg() != 0 {
			return errors.New("unexpected positional arguments")
		}
		if *timeout <= 0 || *opTimeout <= 0 {
			return errors.New("timeouts must be positive")
		}
		options, e := collector.OptionsForProfile(*profile)
		if e != nil {
			return e
		}
		for _, override := range []struct {
			value  int
			target *int
		}{
			{*maxRows, &options.MaxScanPerSource}, {*maxPerSource, &options.MaxPerSource},
			{*maxTotal, &options.MaxTotal}, {*maxPages, &options.MaxListingPages},
		} {
			if override.value != 0 {
				*override.target = override.value
			}
		}
		if e = options.Validate(); e != nil {
			return e
		}
		ctx, stop := context.WithTimeout(ctx, *timeout)
		defer stop()
		native := collector.NewNativeBackend(nativeFetcher(root, !*dry), *opTimeout)
		native.Options = options
		var backend collector.Backend = native
		if *fixture != "" {
			b, e := os.ReadFile(*fixture)
			if e != nil {
				return e
			}
			if len(b) > collector.BridgeLimit {
				return errors.New("fixture exceeds 16 MiB")
			}
			backend, e = collector.ParseFixture(b)
			if e != nil {
				return e
			}
		}
		if !*dry {
			unlock, err := collector.LockState(*statePath)
			if err != nil {
				return err
			}
			defer unlock()
		}
		state, e := collector.LoadStateStrict(*statePath)
		if e != nil {
			return e
		}
		report, e := collector.ScanWithOptions(ctx, backend, state, options)
		if e != nil {
			return e
		}
		data, e := json.MarshalIndent(report, "", "  ")
		if e != nil {
			return e
		}
		data = append(data, '\n')
		if *reportPath != "" {
			if e = collector.AtomicWrite(*reportPath, data); e != nil {
				return e
			}
		}
		if !*dry {
			if e = state.Save(*statePath); e != nil {
				return e
			}
		}
		_, e = os.Stdout.Write(data)
		return e
	case "search":
		fs := flag.NewFlagSet("search", flag.ContinueOnError)
		query := fs.String("query", "", "public search query")
		limit := fs.Int("limit", 5, "result count, clamped to 1..20")
		timeout := fs.Duration("timeout", 60*time.Second, "total native search deadline")
		browser := fs.Bool("browser", true, "allow guarded headed fallback")
		if e := fs.Parse(os.Args[2:]); e != nil {
			return e
		}
		if fs.NArg() != 0 || strings.TrimSpace(*query) == "" {
			return errors.New("--query is required; no positional arguments")
		}
		if *timeout <= 0 {
			return errors.New("timeout must be positive")
		}
		ctx, stop := context.WithTimeout(ctx, *timeout)
		defer stop()
		fetcher := nativeFetcher(root, true)
		result, e := search.Search(ctx, *query, *limit, func(ctx context.Context, raw string) (string, int, error) {
			request := engine.DefaultRequest(raw)
			request.EnablePhase0 = false
			request.EnableMarkdown = false
			request.EnableMainContent = false
			request.EnableExtraction = false
			request.EnableBrowser = *browser
			request.MaxAttempts = 3
			request.MaxBrowserAttempts = 1
			out, err := fetcher.Fetch(ctx, request)
			if err != nil {
				return "", 0, err
			}
			status := 0
			if len(out.Trace) > 0 {
				status = out.Trace[len(out.Trace)-1].Status
			}
			if !out.OK {
				return "", status, fmt.Errorf("search fetch: %s", out.StopReason)
			}
			return out.Content, status, nil
		})
		if e != nil {
			return e
		}
		raw, e := json.Marshal(result)
		if e != nil {
			return e
		}
		var output map[string]any
		if e = json.Unmarshal(raw, &output); e != nil {
			return e
		}
		output["content"] = engine.WrapContent(string(raw), "")
		output["metadata"] = engine.AnalyzeContent(string(raw))
		return json.NewEncoder(os.Stdout).Encode(output)
	case "fetch":
		fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
		url := fs.String("url", "", "public URL to fetch")
		timeout := fs.Duration("timeout", 5*time.Minute, "total native fetch deadline")
		attempts := fs.Int("max-attempts", 0, "native HTTP attempt cap; zero exhausts supported profile grid")
		browser := fs.Bool("browser", true, "allow guarded headed browser fallback")
		markdown := fs.Bool("markdown", true, "convert HTML to native markdown/main content")
		phase0 := fs.Bool("phase0", true, "try public platform API routes first")
		if e := fs.Parse(os.Args[2:]); e != nil {
			return e
		}
		if *url == "" || fs.NArg() != 0 {
			return errors.New("--url is required; no positional arguments")
		}
		if *timeout <= 0 || *attempts < 0 {
			return errors.New("invalid timeout/attempt budget")
		}
		ctx, stop := context.WithTimeout(ctx, *timeout)
		defer stop()
		request := engine.DefaultRequest(*url)
		request.MaxAttempts = *attempts
		request.EnableBrowser = *browser
		request.EnableMarkdown = *markdown
		request.EnableMainContent = *markdown
		request.EnablePhase0 = *phase0
		result, e := nativeFetcher(root, true).Fetch(ctx, request)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(result.Output())

	case "job":
		b, e := os.ReadFile(filepath.Join(root, "config/imported-job.disabled.json"))
		if e != nil {
			return e
		}
		_, e = os.Stdout.Write(b)
		return e
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func nativeFetcher(root string, learning bool) *engine.Fetcher {
	fetcher := engine.NewFetcher(headed.Client{Queue: filepath.Join(root, "runtime/browser/.queue")})
	if learning && os.Getenv("INSANE_LEARN") != "0" && os.Getenv("INSANE_LEARN") != "false" && os.Getenv("INSANE_LEARN") != "no" {
		path := os.Getenv("INSANE_LEARNED_PATH")
		if path == "" {
			path = filepath.Join(root, "data/engine/learned.json")
		}
		fetcher.Learning = engine.NewLearningStore(path)
		if days, e := strconv.Atoi(os.Getenv("INSANE_LEARN_TTL_DAYS")); e == nil && days > 0 && days <= 36500 {
			fetcher.Learning.TTL = time.Duration(days) * 24 * time.Hour
		}
		if maximum, e := strconv.Atoi(os.Getenv("INSANE_LEARN_MAX")); e == nil && maximum > 0 && maximum <= 100000 {
			fetcher.Learning.MaxEntries = maximum
		}
	}
	return fetcher
}
