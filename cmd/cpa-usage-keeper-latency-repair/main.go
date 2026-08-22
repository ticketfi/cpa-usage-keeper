// cpa-usage-keeper-latency-repair is an offline, receipt-bound helper for the
// Keeper latency recovery.  It never opens a listener and never mutates its
// --source database.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"cpa-usage-keeper/internal/repair"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "cpa-usage-keeper-latency-repair:", err)
		os.Exit(2)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return fmt.Errorf("usage: %s <stage|delta> --source PATH --candidate PATH --now RFC3339 --repair-image IMAGE@sha256:HEX [mode receipt flags]", os.Args[0])
	}
	switch arguments[0] {
	case "stage":
		flags := flag.NewFlagSet("stage", flag.ContinueOnError)
		flags.SetOutput(os.Stderr)
		source := flags.String("source", "", "read-only source SQLite file")
		candidate := flags.String("candidate", "", "separate writable candidate SQLite file")
		receipt := flags.String("receipt", "", "new immutable stage receipt path")
		now := flags.String("now", "", "native aggregation timestamp in RFC3339")
		repairImage := flags.String("repair-image", "", "digest-pinned native repair image")
		expiresAt := flags.String("expires-at", "", "receipt expiry in RFC3339, at most 24 hours ahead")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("stage accepts no positional arguments")
		}
		parsedNow, err := parseNow(*now)
		if err != nil {
			return err
		}
		parsedExpiry, err := parseNow(*expiresAt)
		if err != nil {
			return fmt.Errorf("parse --expires-at: %w", err)
		}
		result, err := repair.Stage(repair.StageOptions{SourcePath: *source, CandidatePath: *candidate, ReceiptPath: *receipt, Now: parsedNow, RepairImage: *repairImage, ExpiresAt: parsedExpiry})
		if err != nil {
			return err
		}
		return printJSON(result)
	case "delta":
		flags := flag.NewFlagSet("delta", flag.ContinueOnError)
		flags.SetOutput(os.Stderr)
		source := flags.String("source", "", "read-only current source SQLite file")
		candidate := flags.String("candidate", "", "staged writable candidate SQLite file")
		stageCandidate := flags.String("stage-candidate", "", "post-hot staged candidate SQLite file")
		stageCandidateSHA256 := flags.String("stage-candidate-sha256", "", "exact SHA-256 of post-hot staged candidate")
		receipt := flags.String("receipt", "", "immutable stage receipt path")
		out := flags.String("out", "", "new immutable delta receipt path")
		now := flags.String("now", "", "native aggregation timestamp in RFC3339")
		repairImage := flags.String("repair-image", "", "same digest-pinned native repair image as stage")
		expiresAt := flags.String("expires-at", "", "receipt expiry in RFC3339, at most 24 hours ahead")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("delta accepts no positional arguments")
		}
		parsedNow, err := parseNow(*now)
		if err != nil {
			return err
		}
		parsedExpiry, err := parseNow(*expiresAt)
		if err != nil {
			return fmt.Errorf("parse --expires-at: %w", err)
		}
		result, err := repair.Delta(repair.DeltaOptions{SourcePath: *source, CandidatePath: *candidate, StageCandidatePath: *stageCandidate, StageCandidateSHA256: *stageCandidateSHA256, ReceiptPath: *receipt, OutputPath: *out, Now: parsedNow, RepairImage: *repairImage, ExpiresAt: parsedExpiry})
		if err != nil {
			return err
		}
		return printJSON(result)
	default:
		return fmt.Errorf("unsupported mode %q; expected stage or delta", arguments[0])
	}
}

func parseNow(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("--now is required")
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse --now: %w", err)
	}
	return parsed, nil
}

func printJSON(value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, string(encoded))
	return err
}
