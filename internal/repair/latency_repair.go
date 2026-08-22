// Package repair provides the offline, receipt-bound portion of the Keeper
// latency recovery.  It deliberately does not know how to stop containers or
// swap volumes: callers supply an already-created SQLite candidate and retain
// control of the short listener cutover.
package repair

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"cpa-usage-keeper/internal/entities"
	"cpa-usage-keeper/internal/latency"
	"cpa-usage-keeper/internal/repository"
	"cpa-usage-keeper/internal/repository/latencystore"
	"cpa-usage-keeper/internal/repository/migration"
	"cpa-usage-keeper/internal/timeutil"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const (
	receiptVersion   = "cpa-usage-keeper-latency-repair/v1"
	replayPageSize   = 1000
	forkSourceCommit = "d62cad3f345ae574089a14a4ac75cca023c7ead6"
)

// StageOptions identifies an immutable source database, a separately copied
// candidate database, the output receipt, and the native aggregation instant.
type StageOptions struct {
	SourcePath    string
	CandidatePath string
	ReceiptPath   string
	Now           time.Time
	RepairImage   string
	ExpiresAt     time.Time
}

// DeltaOptions consumes a stage receipt after the stock runner has replayed
// the stage-era hot range, and applies only globally newer raw events.
type DeltaOptions struct {
	SourcePath           string
	CandidatePath        string
	StageCandidatePath   string
	StageCandidateSHA256 string
	ReceiptPath          string
	OutputPath           string
	Now                  time.Time
	RepairImage          string
	ExpiresAt            time.Time
}

// StageReceipt is immutable evidence of a complete S0 replay.  Stage rebuilds
// archive then hot through the native codec so the later delta contract has no
// hidden external runner step.
type StageReceipt struct {
	Version                   string                                      `json:"version"`
	Mode                      string                                      `json:"mode"`
	CreatedAt                 string                                      `json:"createdAt"`
	AggregationAt             string                                      `json:"aggregationAt"`
	Timezone                  string                                      `json:"timezone"`
	SourceSchemaSHA256        string                                      `json:"sourceSchemaSha256"`
	SourceNonLatencySHA256    string                                      `json:"sourceNonLatencySha256"`
	CandidateNonLatencySHA256 string                                      `json:"candidateNonLatencySha256"`
	Partition                 migration.UsageEventReplayPartitionManifest `json:"partition"`
	ArchiveReplayCursor       int64                                       `json:"archiveReplayCursor"`
	StageReplayCursor         int64                                       `json:"stageReplayCursor"`
	StageUnionMaxID           int64                                       `json:"stageUnionMaxId"`
	ForkSourceCommit          string                                      `json:"forkSourceCommit"`
	RepairImage               string                                      `json:"repairImage"`
	ExpiresAt                 string                                      `json:"expiresAt"`
	SourceFileSHA256          string                                      `json:"sourceFileSha256"`
	CandidateInitialSHA256    string                                      `json:"candidateInitialSha256"`
	CandidatePostStageSHA256  string                                      `json:"candidatePostStageSha256"`
}

// DeltaReceipt records the exact extra union range applied after a valid stage
// receipt.  It is intentionally separate: receipts are append-only evidence.
type DeltaReceipt struct {
	Version                     string                             `json:"version"`
	Mode                        string                             `json:"mode"`
	CreatedAt                   string                             `json:"createdAt"`
	AggregationAt               string                             `json:"aggregationAt"`
	Timezone                    string                             `json:"timezone"`
	StageReceiptSHA256          string                             `json:"stageReceiptSha256"`
	SourceSchemaSHA256          string                             `json:"sourceSchemaSha256"`
	NonLatencySHA256            string                             `json:"nonLatencySha256"`
	BaseUnionMaxID              int64                              `json:"baseUnionMaxId"`
	Applied                     migration.UsageEventReplayManifest `json:"applied"`
	FinalLatencyCursor          int64                              `json:"finalLatencyCursor"`
	ForkSourceCommit            string                             `json:"forkSourceCommit"`
	RepairImage                 string                             `json:"repairImage"`
	ExpiresAt                   string                             `json:"expiresAt"`
	SourceFileSHA256            string                             `json:"sourceFileSha256"`
	CandidateInitialSHA256      string                             `json:"candidateInitialSha256"`
	StageCandidatePostHotSHA256 string                             `json:"stageCandidatePostHotSha256"`
	FinalCandidateSHA256        string                             `json:"finalCandidateSha256"`
}

// Stage clears only candidate latency state, replays archive and the fixed S0
// hot partition in native page order, and writes a receipt for delta.
func Stage(options StageOptions) (StageReceipt, error) {
	return withToronto(func() (StageReceipt, error) {
		now, err := requireNow(options.Now)
		if err != nil {
			return StageReceipt{}, err
		}
		if err := validateRepairImage(options.RepairImage); err != nil {
			return StageReceipt{}, err
		}
		createdAt, expiresAt, err := validateReceiptWindow(options.ExpiresAt)
		if err != nil {
			return StageReceipt{}, err
		}
		source, candidate, cleanup, err := openPair(options.SourcePath, options.CandidatePath)
		if err != nil {
			return StageReceipt{}, err
		}
		defer cleanup()
		preflight, err := validatePair(source, candidate)
		if err != nil {
			return StageReceipt{}, err
		}
		sourceFileSHA256, err := fileSHA256(options.SourcePath)
		if err != nil {
			return StageReceipt{}, err
		}
		candidateInitialSHA256, err := fileSHA256(options.CandidatePath)
		if err != nil {
			return StageReceipt{}, err
		}
		if candidateInitialSHA256 != sourceFileSHA256 {
			return StageReceipt{}, errors.New("stage candidate file SHA-256 must exactly match source")
		}
		if err := resetCandidateLatency(candidate, now); err != nil {
			return StageReceipt{}, err
		}
		archiveCursor, err := replayArchive(candidate, preflight.Partition.Archive.MaxID, now)
		if err != nil {
			return StageReceipt{}, err
		}
		if archiveCursor != preflight.Partition.Archive.MaxID {
			return StageReceipt{}, fmt.Errorf("archive replay cursor %d does not reach archive max ID %d", archiveCursor, preflight.Partition.Archive.MaxID)
		}
		stageCursor, err := replayHot(candidate, archiveCursor, preflight.Partition.Union.MaxID, now)
		if err != nil {
			return StageReceipt{}, err
		}
		if stageCursor != preflight.Partition.Union.MaxID {
			return StageReceipt{}, fmt.Errorf("stage replay cursor %d does not reach stage union max ID %d", stageCursor, preflight.Partition.Union.MaxID)
		}
		candidateDigest, err := nonLatencyDigest(candidate)
		if err != nil {
			return StageReceipt{}, err
		}
		if candidateDigest != preflight.SourceNonLatencySHA256 {
			return StageReceipt{}, errors.New("candidate non-latency state changed during stage")
		}
		partition, err := migration.LoadUsageEventReplayPartitionManifest(candidate)
		if err != nil {
			return StageReceipt{}, err
		}
		if partition != preflight.Partition {
			return StageReceipt{}, errors.New("candidate raw event partition changed during stage")
		}
		if err := checkpointAndTruncate(candidate); err != nil {
			return StageReceipt{}, err
		}
		// Close the SQLite handles before recording the candidate digest so the
		// receipt binds the final main database bytes, not an in-flight WAL view.
		cleanup()
		if err := clearCheckpointedSQLiteSidecars(options.CandidatePath); err != nil {
			return StageReceipt{}, err
		}
		if err := rejectNonemptySQLiteSidecars(options.CandidatePath); err != nil {
			return StageReceipt{}, err
		}
		candidatePostStageSHA256, err := fileSHA256(options.CandidatePath)
		if err != nil {
			return StageReceipt{}, err
		}
		receipt := StageReceipt{
			Version:                   receiptVersion,
			Mode:                      "stage",
			CreatedAt:                 timeutil.FormatStorageTime(createdAt),
			AggregationAt:             timeutil.FormatStorageTime(now),
			Timezone:                  "America/Toronto",
			SourceSchemaSHA256:        preflight.SchemaSHA256,
			SourceNonLatencySHA256:    preflight.SourceNonLatencySHA256,
			CandidateNonLatencySHA256: candidateDigest,
			Partition:                 preflight.Partition,
			ArchiveReplayCursor:       archiveCursor,
			StageReplayCursor:         stageCursor,
			StageUnionMaxID:           preflight.Partition.Union.MaxID,
			ForkSourceCommit:          forkSourceCommit,
			RepairImage:               options.RepairImage,
			ExpiresAt:                 timeutil.FormatStorageTime(expiresAt),
			SourceFileSHA256:          sourceFileSHA256,
			CandidateInitialSHA256:    candidateInitialSHA256,
			CandidatePostStageSHA256:  candidatePostStageSHA256,
		}
		if err := writeImmutableJSON(options.ReceiptPath, receipt); err != nil {
			return StageReceipt{}, err
		}
		return receipt, nil
	})
}

// Delta validates both the old receipt prefix and candidate cursor, then uses
// the native codec for only IDs after the stage snapshot.
func Delta(options DeltaOptions) (DeltaReceipt, error) {
	return withToronto(func() (DeltaReceipt, error) {
		now, err := requireNow(options.Now)
		if err != nil {
			return DeltaReceipt{}, err
		}
		stage, stageBytes, err := readStageReceipt(options.ReceiptPath)
		if err != nil {
			return DeltaReceipt{}, err
		}
		if options.RepairImage != stage.RepairImage {
			return DeltaReceipt{}, errors.New("delta repair image must exactly match the stage receipt")
		}
		if err := validateRepairImage(options.RepairImage); err != nil {
			return DeltaReceipt{}, err
		}
		createdAt, expiresAt, err := validateReceiptWindow(options.ExpiresAt)
		if err != nil {
			return DeltaReceipt{}, err
		}
		if err := validateReceiptStillValid(stage); err != nil {
			return DeltaReceipt{}, err
		}
		if !validSHA256(options.StageCandidateSHA256) {
			return DeltaReceipt{}, errors.New("stage candidate SHA-256 is required")
		}
		source, candidate, stageCandidate, cleanup, err := openTriple(options.SourcePath, options.CandidatePath, options.StageCandidatePath)
		if err != nil {
			return DeltaReceipt{}, err
		}
		defer cleanup()
		preflight, err := validatePair(source, candidate)
		if err != nil {
			return DeltaReceipt{}, err
		}
		if preflight.SchemaSHA256 != stage.SourceSchemaSHA256 {
			return DeltaReceipt{}, errors.New("source/candidate SQLite schema drift from stage receipt")
		}
		sourceFileSHA256, err := fileSHA256(options.SourcePath)
		if err != nil {
			return DeltaReceipt{}, err
		}
		candidateInitialSHA256, err := fileSHA256(options.CandidatePath)
		if err != nil {
			return DeltaReceipt{}, err
		}
		stageCandidateSHA256, err := fileSHA256(options.StageCandidatePath)
		if err != nil {
			return DeltaReceipt{}, err
		}
		if stageCandidateSHA256 != options.StageCandidateSHA256 {
			return DeltaReceipt{}, errors.New("stage candidate file SHA-256 does not match required value")
		}
		if stageCandidateSHA256 != stage.CandidatePostStageSHA256 {
			return DeltaReceipt{}, errors.New("stage candidate file SHA-256 does not match stage receipt")
		}
		if candidateInitialSHA256 != sourceFileSHA256 {
			return DeltaReceipt{}, errors.New("frozen S1 candidate file SHA-256 must exactly match source")
		}
		if err := validateStageCandidate(stageCandidate, stage); err != nil {
			return DeltaReceipt{}, err
		}
		sourcePrefix, err := migration.LoadUsageEventReplayPrefixManifest(source, stage.StageUnionMaxID)
		if err != nil {
			return DeltaReceipt{}, err
		}
		if sourcePrefix != stage.Partition.Union {
			return DeltaReceipt{}, errors.New("current source stage-era raw event prefix does not match stage receipt")
		}
		candidateDigest, err := nonLatencyDigest(candidate)
		if err != nil {
			return DeltaReceipt{}, err
		}
		finalMax := preflight.Partition.Union.MaxID
		applied, finalCursor, err := replaceLatencyAndReplayDelta(source, candidate, stageCandidate, stage.StageUnionMaxID, finalMax, now)
		if err != nil {
			return DeltaReceipt{}, err
		}
		if finalCursor != finalMax {
			return DeltaReceipt{}, fmt.Errorf("delta cursor %d does not reach source union max ID %d", finalCursor, finalMax)
		}
		afterDigest, err := nonLatencyDigest(candidate)
		if err != nil {
			return DeltaReceipt{}, err
		}
		if afterDigest != candidateDigest {
			return DeltaReceipt{}, errors.New("candidate non-latency state changed during delta")
		}
		candidatePartition, err := migration.LoadUsageEventReplayPartitionManifest(candidate)
		if err != nil {
			return DeltaReceipt{}, err
		}
		if candidatePartition != preflight.Partition {
			return DeltaReceipt{}, errors.New("candidate raw event partition changed during delta")
		}
		if err := checkpointAndTruncate(candidate); err != nil {
			return DeltaReceipt{}, err
		}
		// Same rule as stage: the final receipt represents stable on-disk bytes.
		cleanup()
		if err := clearCheckpointedSQLiteSidecars(options.CandidatePath); err != nil {
			return DeltaReceipt{}, err
		}
		if err := rejectNonemptySQLiteSidecars(options.CandidatePath); err != nil {
			return DeltaReceipt{}, err
		}
		finalCandidateSHA256, err := fileSHA256(options.CandidatePath)
		if err != nil {
			return DeltaReceipt{}, err
		}
		receipt := DeltaReceipt{
			Version:                     receiptVersion,
			Mode:                        "delta",
			CreatedAt:                   timeutil.FormatStorageTime(createdAt),
			AggregationAt:               timeutil.FormatStorageTime(now),
			Timezone:                    "America/Toronto",
			StageReceiptSHA256:          sha256Hex(stageBytes),
			SourceSchemaSHA256:          preflight.SchemaSHA256,
			NonLatencySHA256:            afterDigest,
			BaseUnionMaxID:              stage.StageUnionMaxID,
			Applied:                     applied,
			FinalLatencyCursor:          finalCursor,
			ForkSourceCommit:            forkSourceCommit,
			RepairImage:                 options.RepairImage,
			ExpiresAt:                   timeutil.FormatStorageTime(expiresAt),
			SourceFileSHA256:            sourceFileSHA256,
			CandidateInitialSHA256:      candidateInitialSHA256,
			StageCandidatePostHotSHA256: stageCandidateSHA256,
			FinalCandidateSHA256:        finalCandidateSHA256,
		}
		if err := writeImmutableJSON(options.OutputPath, receipt); err != nil {
			return DeltaReceipt{}, err
		}
		return receipt, nil
	})
}

type pairPreflight struct {
	SchemaSHA256           string
	SourceNonLatencySHA256 string
	Partition              migration.UsageEventReplayPartitionManifest
}

func validatePair(source, candidate *gorm.DB) (pairPreflight, error) {
	for name, db := range map[string]*gorm.DB{"source": source, "candidate": candidate} {
		if err := requireRepairSchema(db); err != nil {
			return pairPreflight{}, fmt.Errorf("%s schema: %w", name, err)
		}
		hasInbox, err := repository.HasProcessableRedisUsageInbox(context.Background(), db)
		if err != nil {
			return pairPreflight{}, fmt.Errorf("check %s pending inbox: %w", name, err)
		}
		if hasInbox {
			return pairPreflight{}, fmt.Errorf("%s has a pending or process_failed redis usage inbox row", name)
		}
	}
	sourceSchema, err := schemaDigest(source)
	if err != nil {
		return pairPreflight{}, err
	}
	candidateSchema, err := schemaDigest(candidate)
	if err != nil {
		return pairPreflight{}, err
	}
	if sourceSchema != candidateSchema {
		return pairPreflight{}, errors.New("source and candidate SQLite schema drift")
	}
	partition, err := migration.LoadUsageEventReplayPartitionManifest(source)
	if err != nil {
		return pairPreflight{}, err
	}
	candidatePartition, err := migration.LoadUsageEventReplayPartitionManifest(candidate)
	if err != nil {
		return pairPreflight{}, err
	}
	if candidatePartition != partition {
		return pairPreflight{}, errors.New("source and candidate raw event partitions differ")
	}
	sourceDigest, err := nonLatencyDigest(source)
	if err != nil {
		return pairPreflight{}, err
	}
	candidateDigest, err := nonLatencyDigest(candidate)
	if err != nil {
		return pairPreflight{}, err
	}
	if sourceDigest != candidateDigest {
		return pairPreflight{}, errors.New("source and candidate non-latency state differs before repair")
	}
	return pairPreflight{SchemaSHA256: sourceSchema, SourceNonLatencySHA256: sourceDigest, Partition: partition}, nil
}

func validateStageCandidate(db *gorm.DB, stage StageReceipt) error {
	if err := requireRepairSchema(db); err != nil {
		return fmt.Errorf("stage candidate schema: %w", err)
	}
	schema, err := schemaDigest(db)
	if err != nil {
		return err
	}
	if schema != stage.SourceSchemaSHA256 {
		return errors.New("stage candidate schema drift from stage receipt")
	}
	hasInbox, err := repository.HasProcessableRedisUsageInbox(context.Background(), db)
	if err != nil {
		return fmt.Errorf("check stage candidate pending inbox: %w", err)
	}
	if hasInbox {
		return errors.New("stage candidate has a pending or process_failed redis usage inbox row")
	}
	partition, err := migration.LoadUsageEventReplayPartitionManifest(db)
	if err != nil {
		return err
	}
	if partition != stage.Partition {
		return errors.New("stage candidate raw event partition does not match stage receipt")
	}
	digest, err := nonLatencyDigest(db)
	if err != nil {
		return err
	}
	if digest != stage.CandidateNonLatencySHA256 {
		return errors.New("stage candidate non-latency state does not match stage receipt")
	}
	cursor, err := loadLatencyCursor(db)
	if err != nil {
		return err
	}
	if cursor != stage.StageUnionMaxID {
		return fmt.Errorf("stage candidate latency cursor %d must equal stage union max ID %d", cursor, stage.StageUnionMaxID)
	}
	var rows []entities.UsageLatencyStat
	if err := db.Order("id ASC").Find(&rows).Error; err != nil {
		return fmt.Errorf("load stage candidate latency projection: %w", err)
	}
	for _, row := range rows {
		if _, err := latencystore.MergeDiagnosticsRows([]entities.UsageLatencyStat{row}); err != nil {
			return fmt.Errorf("validate stage candidate latency row %d: %w", row.ID, err)
		}
	}
	return nil
}

func replaceLatencyAndReplayDelta(source, candidate, stageCandidate *gorm.DB, afterID, targetID int64, now time.Time) (migration.UsageEventReplayManifest, int64, error) {
	if targetID < afterID {
		return migration.UsageEventReplayManifest{}, 0, fmt.Errorf("delta target ID %d precedes stage union max ID %d", targetID, afterID)
	}
	var stageRows []entities.UsageLatencyStat
	if err := stageCandidate.Order("id ASC").Find(&stageRows).Error; err != nil {
		return migration.UsageEventReplayManifest{}, 0, fmt.Errorf("load stage candidate latency projection: %w", err)
	}
	var stageCheckpoint entities.UsageAggregationCheckpoint
	if err := stageCandidate.Where("name = ?", entities.UsageAggregationCheckpointLatency).Take(&stageCheckpoint).Error; err != nil {
		return migration.UsageEventReplayManifest{}, 0, fmt.Errorf("load stage candidate latency checkpoint: %w", err)
	}
	if stageCheckpoint.LastAggregatedUsageEventID != afterID {
		return migration.UsageEventReplayManifest{}, 0, fmt.Errorf("stage candidate latency cursor %d changed before replacement", stageCheckpoint.LastAggregatedUsageEventID)
	}
	if err := candidate.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("DELETE FROM usage_latency_stats").Error; err != nil {
			return fmt.Errorf("clear frozen candidate latency projection: %w", err)
		}
		if len(stageRows) > 0 {
			if err := tx.CreateInBatches(&stageRows, 100).Error; err != nil {
				return fmt.Errorf("restore exact stage latency projection: %w", err)
			}
		}
		if err := replaceLatencyCheckpoint(tx, stageCheckpoint); err != nil {
			return err
		}
		_, err := replayPagesInTransaction(source, tx, afterID, targetID, now)
		return err
	}); err != nil {
		return migration.UsageEventReplayManifest{}, 0, err
	}
	applied, err := migration.LoadUsageEventReplayRangeManifest(source, afterID, targetID)
	if err != nil {
		return migration.UsageEventReplayManifest{}, 0, err
	}
	return applied, targetID, nil
}

func replaceLatencyCheckpoint(tx *gorm.DB, checkpoint entities.UsageAggregationCheckpoint) error {
	result := tx.Model(&entities.UsageAggregationCheckpoint{}).
		Where("name = ?", entities.UsageAggregationCheckpointLatency).
		Updates(map[string]any{
			"last_aggregated_usage_event_id": checkpoint.LastAggregatedUsageEventID,
			"stats_updated_at":               formatTimePointer(checkpoint.StatsUpdatedAt),
			"created_at":                     timeutil.FormatStorageTime(checkpoint.CreatedAt),
			"updated_at":                     timeutil.FormatStorageTime(checkpoint.UpdatedAt),
		})
	if result.Error != nil {
		return fmt.Errorf("replace candidate latency checkpoint: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("replace candidate latency checkpoint affected %d rows", result.RowsAffected)
	}
	return nil
}

func replayPagesInTransaction(readDB, tx *gorm.DB, afterID, targetID int64, now time.Time) (int64, error) {
	cursor := afterID
	for cursor < targetID {
		events, err := migration.LoadUsageAggregationReplayEventPage(readDB, cursor, targetID, replayPageSize)
		if err != nil {
			return 0, err
		}
		if len(events) == 0 {
			return 0, fmt.Errorf("usage latency delta found an empty page before target %d from cursor %d", targetID, cursor)
		}
		rows, err := latency.BuildRows(events, now)
		if err != nil {
			return 0, fmt.Errorf("build native delta latency rows: %w", err)
		}
		nextCursor := events[len(events)-1].ID
		if err := latencystore.ApplyRows(tx, rows, now); err != nil {
			return 0, fmt.Errorf("apply native delta latency rows: %w", err)
		}
		if err := advanceLatencyCursor(tx, cursor, nextCursor, now); err != nil {
			return 0, err
		}
		cursor = nextCursor
	}
	return cursor, nil
}

func formatTimePointer(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return timeutil.FormatStorageTime(*value)
}

func resetCandidateLatency(db *gorm.DB, now time.Time) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("DELETE FROM usage_latency_stats").Error; err != nil {
			return fmt.Errorf("clear candidate usage latency stats: %w", err)
		}
		result := tx.Model(&entities.UsageAggregationCheckpoint{}).
			Where("name = ?", entities.UsageAggregationCheckpointLatency).
			Updates(map[string]any{
				"last_aggregated_usage_event_id": 0,
				"stats_updated_at":               timeutil.FormatStorageTime(now),
				"updated_at":                     timeutil.FormatStorageTime(now),
			})
		if result.Error != nil {
			return fmt.Errorf("reset candidate latency checkpoint: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("reset candidate latency checkpoint affected %d rows", result.RowsAffected)
		}
		return nil
	})
}

func replayArchive(db *gorm.DB, targetID int64, now time.Time) (int64, error) {
	return replayPages(db, db, 0, targetID, now, migration.LoadUsageAggregationReplayArchivePage)
}

func replayHot(db *gorm.DB, afterID, targetID int64, now time.Time) (int64, error) {
	return replayPages(db, db, afterID, targetID, now, migration.LoadUsageAggregationReplayHotPage)
}

type replayLoader func(*gorm.DB, int64, int64, int) ([]entities.UsageEvent, error)

func replayPages(readDB, writeDB *gorm.DB, afterID, targetID int64, now time.Time, load replayLoader) (int64, error) {
	cursor := afterID
	for cursor < targetID {
		events, err := load(readDB, cursor, targetID, replayPageSize)
		if err != nil {
			return 0, err
		}
		if len(events) == 0 {
			return 0, fmt.Errorf("usage latency repair found an empty page before target %d from cursor %d", targetID, cursor)
		}
		if events[0].ID <= cursor || events[len(events)-1].ID <= cursor {
			return 0, fmt.Errorf("usage latency repair loader returned non-advancing page after %d", cursor)
		}
		rows, err := latency.BuildRows(events, now)
		if err != nil {
			return 0, fmt.Errorf("build native latency rows: %w", err)
		}
		nextCursor := events[len(events)-1].ID
		if err := writeDB.Transaction(func(tx *gorm.DB) error {
			if err := latencystore.ApplyRows(tx, rows, now); err != nil {
				return fmt.Errorf("apply native latency rows: %w", err)
			}
			return advanceLatencyCursor(tx, cursor, nextCursor, now)
		}); err != nil {
			return 0, err
		}
		cursor = nextCursor
	}
	return cursor, nil
}

func advanceLatencyCursor(tx *gorm.DB, expected, next int64, now time.Time) error {
	result := tx.Model(&entities.UsageAggregationCheckpoint{}).
		Where("name = ? AND last_aggregated_usage_event_id = ?", entities.UsageAggregationCheckpointLatency, expected).
		Updates(map[string]any{
			"last_aggregated_usage_event_id": next,
			"stats_updated_at":               timeutil.FormatStorageTime(now),
			"updated_at":                     timeutil.FormatStorageTime(now),
		})
	if result.Error != nil {
		return fmt.Errorf("advance latency repair checkpoint: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("advance latency repair checkpoint from %d changed", expected)
	}
	return nil
}

func loadLatencyCursor(db *gorm.DB) (int64, error) {
	var checkpoint entities.UsageAggregationCheckpoint
	if err := db.Where("name = ?", entities.UsageAggregationCheckpointLatency).Take(&checkpoint).Error; err != nil {
		return 0, fmt.Errorf("load latency checkpoint: %w", err)
	}
	return checkpoint.LastAggregatedUsageEventID, nil
}

func requireRepairSchema(db *gorm.DB) error {
	for _, table := range []string{"usage_events", "usage_events_archive", "usage_latency_stats", "usage_aggregation_checkpoints", "redis_usage_inboxes"} {
		if !db.Migrator().HasTable(table) {
			return fmt.Errorf("required table %q is missing", table)
		}
	}
	storageColumns := strings.Split(entities.UsageEventStorageColumns, ", ")
	for _, table := range []string{"usage_events", "usage_events_archive"} {
		if err := requireColumns(db, table, storageColumns); err != nil {
			return err
		}
	}
	if err := requireColumns(db, "usage_latency_stats", []string{"id", "bucket_type", "bucket_start", "api_group_key", "sample_count", "max_ttft_ms", "max_latency_ms", "format_version", "ttft_sketch", "latency_sketch", "sample_points", "created_at", "updated_at"}); err != nil {
		return err
	}
	if err := requireColumns(db, "usage_aggregation_checkpoints", []string{"name", "last_aggregated_usage_event_id", "stats_updated_at", "created_at", "updated_at"}); err != nil {
		return err
	}
	if err := requireColumns(db, "redis_usage_inboxes", []string{"id", "status"}); err != nil {
		return err
	}
	var latencyCount int64
	if err := db.Model(&entities.UsageAggregationCheckpoint{}).Where("name = ?", entities.UsageAggregationCheckpointLatency).Count(&latencyCount).Error; err != nil {
		return fmt.Errorf("check latency checkpoint: %w", err)
	}
	if latencyCount != 1 {
		return fmt.Errorf("expected exactly one latency checkpoint, got %d", latencyCount)
	}
	return nil
}

func requireColumns(db *gorm.DB, table string, expected []string) error {
	columns, err := tableColumns(db, table)
	if err != nil {
		return err
	}
	actual := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		actual[column] = struct{}{}
	}
	for _, column := range expected {
		if _, ok := actual[column]; !ok {
			return fmt.Errorf("required table %s has schema drift: missing column %s", table, column)
		}
	}
	return nil
}

func schemaDigest(db *gorm.DB) (string, error) {
	type schemaRow struct{ Type, Name, TableName, SQL string }
	var rows []schemaRow
	if err := db.Raw(`SELECT type, name, tbl_name AS table_name, COALESCE(sql, '') AS sql
		FROM sqlite_master
		WHERE type IN ('table', 'index', 'trigger', 'view') AND name NOT LIKE 'sqlite_%'
		ORDER BY type, name`).Scan(&rows).Error; err != nil {
		return "", fmt.Errorf("load SQLite schema digest: %w", err)
	}
	hash := sha256.New()
	for _, row := range rows {
		fmt.Fprintf(hash, "%s\x00%s\x00%s\x00%s\n", row.Type, row.Name, row.TableName, row.SQL)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func nonLatencyDigest(db *gorm.DB) (string, error) {
	tables, err := userTables(db)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	for _, table := range tables {
		if table == "usage_latency_stats" {
			continue
		}
		where := ""
		if table == "usage_aggregation_checkpoints" {
			where = " WHERE name <> 'latency'"
		}
		columns, err := tableColumns(db, table)
		if err != nil {
			return "", err
		}
		quoted := make([]string, 0, len(columns))
		for _, column := range columns {
			quoted = append(quoted, "quote("+quoteIdentifier(column)+")")
		}
		query := "SELECT " + strings.Join(quoted, ", ") + " FROM " + quoteIdentifier(table) + where + " ORDER BY rowid"
		if err := hashQueryRows(db, hash, table, query); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func userTables(db *gorm.DB) ([]string, error) {
	var tables []string
	if err := db.Raw("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name").Scan(&tables).Error; err != nil {
		return nil, fmt.Errorf("list SQLite tables: %w", err)
	}
	return tables, nil
}

func tableColumns(db *gorm.DB, table string) ([]string, error) {
	type column struct{ Name string }
	var columns []column
	if err := db.Raw("PRAGMA table_info(" + quoteIdentifier(table) + ")").Scan(&columns).Error; err != nil {
		return nil, fmt.Errorf("list %s columns: %w", table, err)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s has no columns", table)
	}
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names, nil
}

func hashQueryRows(db *gorm.DB, hash io.Writer, table, query string) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("open %s digest database: %w", table, err)
	}
	rows, err := sqlDB.Query(query)
	if err != nil {
		return fmt.Errorf("query %s digest: %w", table, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("read %s digest columns: %w", table, err)
	}
	fmt.Fprintf(hash, "table:%s\n", table)
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		destinations := make([]any, len(values))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return fmt.Errorf("scan %s digest: %w", table, err)
		}
		for _, value := range values {
			fmt.Fprintf(hash, "%d:", len(value.String))
			if _, err := io.WriteString(hash, value.String); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(hash, "\n"); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s digest: %w", table, err)
	}
	return nil
}

func openPair(sourcePath, candidatePath string) (*gorm.DB, *gorm.DB, func(), error) {
	sourcePath, candidatePath, err := validatedDistinctPaths(sourcePath, candidatePath)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := rejectNonemptySQLiteSidecars(sourcePath, candidatePath); err != nil {
		return nil, nil, nil, err
	}
	source, err := openSQLiteReadOnly(sourcePath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open source SQLite database: %w", err)
	}
	candidate, err := gorm.Open(sqlite.Open(sqliteFileURI(candidatePath, "mode=rw&_busy_timeout=5000")), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		closeGormDatabase(source)
		return nil, nil, nil, fmt.Errorf("open candidate SQLite database: %w", err)
	}
	cleanup := func() {
		if sqlDB, err := source.DB(); err == nil {
			_ = sqlDB.Close()
		}
		if sqlDB, err := candidate.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	return source, candidate, cleanup, nil
}

func openTriple(sourcePath, candidatePath, stageCandidatePath string) (*gorm.DB, *gorm.DB, *gorm.DB, func(), error) {
	sourcePath, candidatePath, stageCandidatePath, err := validatedDistinctTriple(sourcePath, candidatePath, stageCandidatePath)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if err := rejectNonemptySQLiteSidecars(sourcePath, candidatePath, stageCandidatePath); err != nil {
		return nil, nil, nil, nil, err
	}
	source, err := openSQLiteReadOnly(sourcePath)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open source SQLite database: %w", err)
	}
	candidate, err := gorm.Open(sqlite.Open(sqliteFileURI(candidatePath, "mode=rw&_busy_timeout=5000")), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		closeGormDatabase(source)
		return nil, nil, nil, nil, fmt.Errorf("open candidate SQLite database: %w", err)
	}
	stageCandidate, err := openSQLiteReadOnly(stageCandidatePath)
	if err != nil {
		closeGormDatabase(source)
		closeGormDatabase(candidate)
		return nil, nil, nil, nil, fmt.Errorf("open stage candidate SQLite database: %w", err)
	}
	cleanup := func() {
		closeGormDatabase(source)
		closeGormDatabase(candidate)
		closeGormDatabase(stageCandidate)
	}
	return source, candidate, stageCandidate, cleanup, nil
}

func openSQLiteReadOnly(path string) (*gorm.DB, error) {
	// Immutable and query-only make the source/stage inputs a read surface: no
	// journal creation, lock promotion, or accidental write pragma is allowed.
	return gorm.Open(sqlite.Open(sqliteFileURI(path, "mode=ro&immutable=1&_query_only=1&_busy_timeout=5000")), &gorm.Config{SkipDefaultTransaction: true})
}

func sqliteFileURI(path, rawQuery string) string {
	return sqliteFileURIForGOOS(path, rawQuery, runtime.GOOS)
}

// sqliteFileURIForGOOS expects an absolute path. The repair entrypoints enforce
// that contract before opening any database. Windows separators must only be
// rewritten for Windows paths: a backslash is a valid filename byte on POSIX,
// and rewriting it there could alias two files that passed the distinct-path
// safety checks.
func sqliteFileURIForGOOS(path, rawQuery, goos string) string {
	uriPath := filepath.ToSlash(path)
	if goos == "windows" {
		uriPath = strings.ReplaceAll(path, `\`, "/")
	}
	if looksLikeWindowsAbsoluteDrivePath(uriPath) && !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	return (&url.URL{Scheme: "file", Path: uriPath, RawQuery: rawQuery}).String()
}

func looksLikeWindowsAbsoluteDrivePath(path string) bool {
	if len(path) < 3 || path[1] != ':' || path[2] != '/' {
		return false
	}
	drive := path[0]
	return ('A' <= drive && drive <= 'Z') || ('a' <= drive && drive <= 'z')
}

func closeGormDatabase(db *gorm.DB) {
	if db == nil {
		return
	}
	if sqlDB, err := db.DB(); err == nil {
		_ = sqlDB.Close()
	}
}

func checkpointAndTruncate(db *gorm.DB) error {
	if db == nil {
		return errors.New("database is nil")
	}
	if err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)").Error; err != nil {
		return fmt.Errorf("checkpoint candidate WAL: %w", err)
	}
	return nil
}

func rejectNonemptySQLiteSidecars(paths ...string) error {
	for _, path := range paths {
		for _, suffix := range []string{"-wal", "-shm"} {
			_, err := os.Lstat(path + suffix)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return fmt.Errorf("lstat SQLite sidecar %s: %w", path+suffix, err)
			}
			return fmt.Errorf("SQLite input sidecar %s must be absent", path+suffix)
		}
	}
	return nil
}

func clearCheckpointedSQLiteSidecars(path string) error {
	walPath := path + "-wal"
	if info, err := os.Lstat(walPath); err == nil {
		if !info.Mode().IsRegular() || info.Size() != 0 {
			return fmt.Errorf("SQLite WAL %s was not empty after checkpoint", walPath)
		}
		if err := os.Remove(walPath); err != nil {
			return fmt.Errorf("remove empty SQLite WAL: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat checkpointed SQLite WAL: %w", err)
	}
	shmPath := path + "-shm"
	if info, err := os.Lstat(shmPath); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("SQLite shared-memory sidecar %s is not a regular file", shmPath)
		}
		if err := os.Remove(shmPath); err != nil {
			return fmt.Errorf("remove checkpointed SQLite shared-memory sidecar: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat checkpointed SQLite shared-memory sidecar: %w", err)
	}
	return nil
}

func validatedDistinctPaths(sourcePath, candidatePath string) (string, string, error) {
	if strings.TrimSpace(sourcePath) == "" || strings.TrimSpace(candidatePath) == "" {
		return "", "", errors.New("source and candidate paths are required")
	}
	sourcePath, err := filepath.Abs(sourcePath)
	if err != nil {
		return "", "", fmt.Errorf("resolve source path: %w", err)
	}
	candidatePath, err = filepath.Abs(candidatePath)
	if err != nil {
		return "", "", fmt.Errorf("resolve candidate path: %w", err)
	}
	sourceInfo, err := lstatRegularFile(sourcePath, "source")
	if err != nil {
		return "", "", err
	}
	candidateInfo, err := lstatRegularFile(candidatePath, "candidate")
	if err != nil {
		return "", "", err
	}
	if sourcePath == candidatePath || os.SameFile(sourceInfo, candidateInfo) {
		return "", "", errors.New("source and candidate must be distinct files; in-place repair is forbidden")
	}
	return sourcePath, candidatePath, nil
}

func validatedDistinctTriple(sourcePath, candidatePath, stageCandidatePath string) (string, string, string, error) {
	sourcePath, candidatePath, err := validatedDistinctPaths(sourcePath, candidatePath)
	if err != nil {
		return "", "", "", err
	}
	if strings.TrimSpace(stageCandidatePath) == "" {
		return "", "", "", errors.New("stage candidate path is required")
	}
	stageCandidatePath, err = filepath.Abs(stageCandidatePath)
	if err != nil {
		return "", "", "", fmt.Errorf("resolve stage candidate path: %w", err)
	}
	stageInfo, err := lstatRegularFile(stageCandidatePath, "stage candidate")
	if err != nil {
		return "", "", "", err
	}
	sourceInfo, err := lstatRegularFile(sourcePath, "source")
	if err != nil {
		return "", "", "", err
	}
	candidateInfo, err := lstatRegularFile(candidatePath, "candidate")
	if err != nil {
		return "", "", "", err
	}
	if os.SameFile(stageInfo, sourceInfo) || os.SameFile(stageInfo, candidateInfo) {
		return "", "", "", errors.New("stage candidate must be a third distinct regular SQLite file")
	}
	return sourcePath, candidatePath, stageCandidatePath, nil
}

func lstatRegularFile(path, label string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s path: %w", label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular non-symlink SQLite file", label)
	}
	return info, nil
}

func readStageReceipt(path string) (StageReceipt, []byte, error) {
	bytes, err := readPrivateRegularFile(path, "stage receipt")
	if err != nil {
		return StageReceipt{}, nil, err
	}
	var receipt StageReceipt
	decoder := json.NewDecoder(bytesReader(bytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return StageReceipt{}, nil, fmt.Errorf("parse stage receipt: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return StageReceipt{}, nil, errors.New("stage receipt contains trailing JSON values")
	}
	if receipt.Version != receiptVersion || receipt.Mode != "stage" || receipt.Timezone != "America/Toronto" || receipt.ForkSourceCommit != forkSourceCommit || receipt.StageUnionMaxID != receipt.Partition.Union.MaxID || receipt.ArchiveReplayCursor != receipt.Partition.Archive.MaxID || receipt.StageReplayCursor != receipt.StageUnionMaxID || !validSHA256(receipt.SourceSchemaSHA256) || !validSHA256(receipt.SourceNonLatencySHA256) || !validSHA256(receipt.CandidateNonLatencySHA256) || !validSHA256(receipt.Partition.Union.SHA256) || !validSHA256(receipt.SourceFileSHA256) || !validSHA256(receipt.CandidateInitialSHA256) || !validSHA256(receipt.CandidatePostStageSHA256) || !validRepairImage(receipt.RepairImage) {
		return StageReceipt{}, nil, errors.New("stage receipt is invalid or unsupported")
	}
	if _, err := time.Parse(time.RFC3339Nano, receipt.AggregationAt); err != nil {
		return StageReceipt{}, nil, fmt.Errorf("parse stage receipt aggregation time: %w", err)
	}
	if err := validateReceiptStillValid(receipt); err != nil {
		return StageReceipt{}, nil, err
	}
	return receipt, bytes, nil
}

func writeImmutableJSON(path string, value any) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("receipt output path is required")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve receipt output path: %w", err)
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode receipt: %w", err)
	}
	encoded = append(encoded, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create immutable receipt: %w", err)
	}
	if _, err := file.Write(encoded); err != nil {
		return fmt.Errorf("write receipt: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync receipt: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close receipt: %w", err)
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open receipt parent directory: %w", err)
	}
	defer parent.Close()
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("sync receipt parent directory: %w", err)
	}
	return nil
}

func requireNow(now time.Time) (time.Time, error) {
	if now.IsZero() {
		return time.Time{}, errors.New("native aggregation time is required")
	}
	return timeutil.NormalizeStorageTime(now), nil
}

func validateRepairImage(value string) error {
	if !validRepairImage(value) {
		return errors.New("repair image must be digest pinned with @sha256:<64 hex>")
	}
	return nil
}
func validRepairImage(value string) bool {
	index := strings.LastIndex(value, "@sha256:")
	return index > 0 && validSHA256(value[index+len("@sha256:"):])
}

func validateReceiptWindow(expiresAt time.Time) (time.Time, time.Time, error) {
	createdAt := time.Now().In(time.Local)
	if expiresAt.IsZero() {
		return time.Time{}, time.Time{}, errors.New("receipt expiry is required")
	}
	expiresAt = expiresAt.In(time.Local)
	if !expiresAt.After(createdAt) || expiresAt.Sub(createdAt) > 24*time.Hour {
		return time.Time{}, time.Time{}, errors.New("receipt expiry must be after creation and no more than 24 hours away")
	}
	return createdAt, expiresAt, nil
}

func validateReceiptStillValid(receipt StageReceipt) error {
	createdAt, err := time.Parse(time.RFC3339Nano, receipt.CreatedAt)
	if err != nil {
		return fmt.Errorf("parse receipt creation time: %w", err)
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, receipt.ExpiresAt)
	if err != nil {
		return fmt.Errorf("parse receipt expiry time: %w", err)
	}
	if !expiresAt.After(createdAt) || expiresAt.Sub(createdAt) > 24*time.Hour {
		return errors.New("receipt expiry window is invalid")
	}
	if !time.Now().After(expiresAt) {
		return nil
	}
	return errors.New("stage receipt is expired")
}

func readPrivateRegularFile(path, label string) ([]byte, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%s path is required", label)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", label, err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s must be a regular non-symlink file", label)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s permissions must be 0600 or stricter", label)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", label, err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat opened %s: %w", label, err)
	}
	if !os.SameFile(info, opened) {
		return nil, fmt.Errorf("%s changed while opening", label)
	}
	return io.ReadAll(io.LimitReader(file, 4<<20))
}

func bytesReader(value []byte) *bytes.Reader { return bytes.NewReader(value) }

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file for SHA-256: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash file: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func withToronto[T any](fn func() (T, error)) (T, error) {
	previous := time.Local
	location, err := time.LoadLocation("America/Toronto")
	if err != nil {
		var zero T
		return zero, fmt.Errorf("load America/Toronto: %w", err)
	}
	time.Local = location
	defer func() { time.Local = previous }()
	return fn()
}
func quoteIdentifier(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
func sha256Hex(value []byte) string       { sum := sha256.Sum256(value); return hex.EncodeToString(sum[:]) }
func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
