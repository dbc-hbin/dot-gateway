// headed-broker manages the verified headed Node/Playwright worker from Go.
// Chromium and Node/Playwright remain; Python is not used by this command.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"insane-search-migration/internal/headed"
	"os"
	"path/filepath"
	"time"
)

func rootPath() string {
	if r := os.Getenv("INSANE_ROOT"); r != "" {
		return r
	}
	cwd, _ := os.Getwd()
	exe, _ := os.Executable()
	for _, start := range []string{cwd, filepath.Dir(exe)} {
		for dir := start; ; dir = filepath.Dir(dir) {
			if _, err := os.Stat(filepath.Join(dir, "runtime/browser/native_service.cjs")); err == nil {
				return dir
			}
			if filepath.Dir(dir) == dir {
				break
			}
		}
	}
	return cwd
}
func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: headed-broker start|status|stop|restart|verify [--root PATH] [--queue PATH] [--proxy-config PATH]")
	}
	flags := flag.NewFlagSet("headed-broker", flag.ContinueOnError)
	root := flags.String("root", rootPath(), "migration project root")
	queue := flags.String("queue", "", "private queue")
	proxy := flags.String("proxy-config", "", "existing credential-free native proxy config")
	timeout := flags.Duration("timeout", 30*time.Second, "command deadline")
	url := flags.String("url", "", "public URL for verify")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *timeout <= 0 {
		return errors.New("invalid arguments")
	}
	if *queue == "" {
		*queue = filepath.Join(*root, "runtime/browser/.queue")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	opts := headed.StartOptions{Root: *root, Queue: *queue, ProxyConfig: *proxy}
	var result any
	var err error
	switch os.Args[1] {
	case "start":
		result, err = headed.StartDetached(ctx, opts)
	case "restart":
		result, err = headed.Restart(ctx, opts)
	case "stop":
		result, err = headed.Stop(ctx, *queue)
	case "status":
		var status headed.Status
		status, err = headed.ReadStatus(*queue)
		result = map[string]any{"healthy": status.Healthy(), "status": status}
	case "verify":
		if *url == "" {
			return errors.New("--url required")
		}
		var page headed.Envelope
		page, err = (headed.Client{Queue: *queue}).Capture(ctx, headed.Request{URL: *url, TimeoutMS: timeout.Milliseconds()})
		result = map[string]any{"status": page.Status, "final_url": page.FinalURL, "backend": page.Automation, "html_bytes": len(page.HTML), "visible_text_bytes": len(page.InnerText), "terminal_reason": page.TerminalReason, "headless": page.Headless, "chromium_sandbox": page.ChromiumSandbox}
	default:
		return errors.New("unknown command")
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "headed-broker:", err)
		os.Exit(1)
	}
}
