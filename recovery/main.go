// dot-recovery is deliberately offline. It does not send HTTP, launch services,
// install packages, transfer secrets, or register any scheduler or startup hook.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"
)

func execute(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: dot-recovery verify-source|pack-source|restore-source|configure-source|snapshot|verify-state|restore-state|snapshot-operations|verify-operations|restore-operations|activation-env|health; mutations require --apply")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	source := f.String("source", "", "reviewed source directory")
	archive := f.String("archive", "", "recovery ZIP")
	pin := f.String("manifest-sha", "", "trusted source manifest SHA-256")
	snap := f.String("snapshot", "", "private snapshot file")
	sp := f.String("snapshot-sha", "", "trusted snapshot SHA-256")
	output := f.String("output", "", "new absolute output file")
	operationRoot := f.String("operations-root", "", "private committed collector/reporting operation directory")
	operationsFile := f.String("operations", "", "private operations snapshot")
	operationsSHA := f.String("operations-sha", "", "trusted operations snapshot SHA-256")
	component := f.String("component", "", "gateway, gateway-receive-only or headed for activation environment")
	stateRoot := f.String("state-root", "", "new restored private state root for generated path references")
	dest := f.String("new-root", "", "new absolute destination root; never existing")
	db := f.String("db", "", "private bridge database")
	reports := f.String("reports", "", "private reporting state directory")
	op := f.String("operation", "", "allowlisted private operation JSON")
	queue := f.String("queue", "", "private headed broker queue")
	token := f.String("token-file", "", "metadata-only Discord credential check")
	gh := f.String("github-credential-file", "", "optional metadata-only GitHub credential check")
	apply := f.Bool("apply", false, "write new output; default verify only")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	result := map[string]any{"ok": true, "command": args[0], "applied": false}
	switch args[0] {
	case "verify-source", "pack-source", "restore-source":
		s, e := loadSource(*source, *archive, *pin)
		if e != nil {
			return e
		}
		result["verified_files"] = len(s.Files)
		result["manifest_sha256"] = *pin
		if args[0] == "pack-source" {
			if *output == "" {
				return errors.New("--output required")
			}
			if *apply {
				b, e := packSource(s)
				if e != nil {
					return e
				}
				if e = atomicFile(*output, b); e != nil {
					return e
				}
				result["archive_sha256"] = digest(b)
				result["applied"] = true
			}
		}
		if args[0] == "restore-source" {
			if *dest == "" {
				return errors.New("--new-root required")
			}
			if *apply {
				if e = restoreSource(s, *dest, -1); e != nil {
					return e
				}
				result["applied"] = true
			}
		}
	case "configure-source":
		s, e := loadSource(*source, *archive, *pin)
		if e != nil {
			return e
		}
		state, e := readSnapshot(*snap, *sp, *pin)
		if e != nil {
			return e
		}
		if *dest == "" || *stateRoot == "" {
			return errors.New("--new-root and --state-root required")
		}
		if *apply {
			if e = configureSource(s, state, *dest, *stateRoot, *sp); e != nil {
				return e
			}
			result["applied"] = true
		}
		result["private_derived_source"] = true
	case "snapshot-operations":
		main, e := readSnapshot(*snap, *sp, *pin)
		if e != nil {
			return e
		}
		if *operationRoot == "" || *output == "" {
			return errors.New("--operations-root and --output required")
		}
		o, e := snapshotOperations(*operationRoot, main)
		if e != nil {
			return e
		}
		result["fingerprints"] = len(o.Collector.Fingerprints)
		result["reviewed_posts"] = len(o.Reviewed.Posts)
		result["delivered_offers"] = len(o.Reviewed.Offers)
		result["first_notification_preserved"] = true
		if *apply {
			b := jsonBytes(o)
			if e = atomicFile(*output, b); e != nil {
				return e
			}
			result["applied"] = true
			result["operations_sha256"] = digest(b)
		}
	case "verify-operations", "restore-operations":
		main, e := readSnapshot(*snap, *sp, *pin)
		if e != nil {
			return e
		}
		o, e := readOperations(*operationsFile, *operationsSHA, main)
		if e != nil {
			return e
		}
		result["fingerprints"] = len(o.Collector.Fingerprints)
		result["delivered_offers"] = len(o.Reviewed.Offers)
		result["first_notification_preserved"] = true
		if args[0] == "restore-operations" {
			if *dest == "" {
				return errors.New("--new-root required")
			}
			if *apply {
				if e = restoreOperations(o, main, *dest, -1); e != nil {
					return e
				}
				result["applied"] = true
			}
		}
	case "activation-env":
		return printActivationEnv(*stateRoot, *source, *sp, *pin, *component)
	case "snapshot":
		if *db == "" || *reports == "" || *op == "" || *output == "" || !hashPattern.MatchString(*pin) {
			return errors.New("snapshot requires --db --reports --operation --manifest-sha --output")
		}
		b, e := readFile(*op, true, 16<<10)
		if e != nil {
			return e
		}
		s := Snapshot{Schema: 1, Created: time.Now().UTC().Format(time.RFC3339Nano), ManifestSHA: *pin}
		if strict(b, &s.Operation) != nil {
			return errors.New("invalid allowlisted operation config")
		}
		if e = snapshotDB(*db, &s); e != nil {
			return e
		}
		if e = snapshotReports(*reports, &s); e != nil {
			return e
		}
		if e = validateSnapshot(s); e != nil {
			return e
		}
		result["events"] = len(s.Events)
		result["ingress"] = len(s.Ingress)
		result["reports"] = len(s.Reports)
		result["diagnostics"] = len(s.Diagnostics)
		b = jsonBytes(s)
		if len(b) > 64<<20 {
			return errors.New("snapshot exceeds supported restore size")
		}
		result["state_counts"] = recoveryMetadataCounts(s)
		if *apply {
			if e = atomicFile(*output, b); e != nil {
				return e
			}
			result["snapshot_sha256"] = digest(b)
			result["applied"] = true
		}
	case "verify-state", "restore-state":
		b, e := readFile(*snap, true, 64<<20)
		if e != nil {
			return e
		}
		if !hashPattern.MatchString(*sp) || digest(b) != *sp {
			return errors.New("trusted snapshot SHA mismatch")
		}
		var s Snapshot
		if strict(b, &s) != nil {
			return errors.New("invalid snapshot JSON")
		}
		if e = validateSnapshot(s); e != nil {
			return e
		}
		if *pin == "" || s.ManifestSHA != *pin {
			return errors.New("source/snapshot manifest binding mismatch")
		}
		result["events"] = len(s.Events)
		result["reports"] = len(s.Reports)
		result["quarantine_on_restore"] = true
		result["state_counts"] = recoveryMetadataCounts(s)
		if args[0] == "restore-state" {
			if *dest == "" {
				return errors.New("--new-root required")
			}
			if *apply {
				if e = restoreState(s, *dest, -1); e != nil {
					return e
				}
				result["applied"] = true
			}
		}
	case "health":
		return json.NewEncoder(os.Stdout).Encode(health(*db, *queue, *token, *gh))
	default:
		return errors.New("unknown command")
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
func main() {
	if e := execute(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, "dot-recovery:", e)
		os.Exit(1)
	}
}
