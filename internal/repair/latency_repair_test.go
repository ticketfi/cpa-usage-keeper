package repair

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cpa-usage-keeper/internal/entities"
	"cpa-usage-keeper/internal/repository/migration"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestStageThenDeltaUsesNativeReplayAndPreservesNonLatencyState(t *testing.T) {
	location, err := time.LoadLocation("America/Toronto")
	if err != nil {
		t.Fatalf("load Toronto: %v", err)
	}
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, location)
	sourcePath, stageCandidatePath := makeRepairPair(t, now)
	sourceBefore := fileDigest(t, sourcePath)
	stagePath := filepath.Join(t.TempDir(), "stage.json")
	repairImage := testRepairImage()
	stage, err := Stage(StageOptions{SourcePath: sourcePath, CandidatePath: stageCandidatePath, ReceiptPath: stagePath, Now: now, RepairImage: repairImage, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatalf("Stage returned error: %v", err)
	}
	if stage.ArchiveReplayCursor != 2 || stage.StageReplayCursor != 4 || stage.StageUnionMaxID != 4 || stage.Partition.Archive.Count != 2 || stage.Partition.Hot.Count != 2 {
		t.Fatalf("unexpected stage receipt: %+v", stage)
	}
	if stage.CandidatePostStageSHA256 != fileDigest(t, stageCandidatePath) {
		t.Fatal("stage receipt did not bind the standalone post-stage candidate bytes")
	}
	if aggregationAt, err := time.Parse(time.RFC3339Nano, stage.AggregationAt); err != nil || !aggregationAt.Equal(now) {
		t.Fatalf("stage aggregationAt=%q err=%v, want operator --now %s", stage.AggregationAt, err, now)
	}
	assertStandaloneSQLite(t, stageCandidatePath)
	assertStandaloneSQLite(t, sourcePath)
	if sourceAfter := fileDigest(t, sourcePath); sourceAfter != sourceBefore {
		t.Fatal("stage mutated the explicit source SQLite file")
	}
	info, err := os.Stat(stagePath)
	if err != nil {
		t.Fatalf("stat stage receipt: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("stage receipt mode=%#o, want 0600", info.Mode().Perm())
	}

	stageCandidate := openReadRepairDB(t, stageCandidatePath)
	if cursor, err := loadLatencyCursor(stageCandidate); err != nil || cursor != stage.StageUnionMaxID {
		t.Fatalf("stage cursor=%d err=%v, want %d", cursor, err, stage.StageUnionMaxID)
	}
	closeRepairDB(stageCandidate)
	if err := clearCheckpointedSQLiteSidecars(stageCandidatePath); err != nil {
		t.Fatalf("clear stage candidate read-only sidecars: %v", err)
	}

	source := openRepairDB(t, sourcePath)
	trueValue := true
	if err := source.Create(&entities.UsageEvent{ID: 5, EventKey: "hot-5", APIGroupKey: "group-a", Timestamp: now.Add(-time.Minute), Generate: &trueValue, TTFTMS: int64Pointer(150), LatencyMS: 800}).Error; err != nil {
		t.Fatalf("append current hot event: %v", err)
	}
	settingValue := "survives-delta"
	if err := source.Create(&entities.AppSetting{SettingKey: "repair-test", Value: &settingValue, ValueType: entities.AppSettingValueTypeJSON, CreatedAt: now, UpdatedAt: now}).Error; err != nil {
		t.Fatalf("append unrelated S1 state: %v", err)
	}
	closeRepairDB(source)
	frozenCandidatePath := copySQLiteFile(t, sourcePath, "frozen-s1.db")

	deltaPath := filepath.Join(t.TempDir(), "delta.json")
	delta, err := Delta(DeltaOptions{SourcePath: sourcePath, CandidatePath: frozenCandidatePath, StageCandidatePath: stageCandidatePath, StageCandidateSHA256: fileDigest(t, stageCandidatePath), ReceiptPath: stagePath, OutputPath: deltaPath, Now: now, RepairImage: repairImage, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatalf("Delta returned error: %v", err)
	}
	if delta.Applied.Count != 1 || delta.Applied.MinID != 5 || delta.Applied.MaxID != 5 || delta.FinalLatencyCursor != 5 {
		t.Fatalf("unexpected delta receipt: %+v", delta)
	}
	if delta.FinalCandidateSHA256 != fileDigest(t, frozenCandidatePath) {
		t.Fatal("delta receipt did not bind the standalone final candidate bytes")
	}
	assertStandaloneSQLite(t, frozenCandidatePath)
	assertStandaloneSQLite(t, sourcePath)
	assertStandaloneSQLite(t, stageCandidatePath)
	if aggregationAt, err := time.Parse(time.RFC3339Nano, delta.AggregationAt); err != nil || !aggregationAt.Equal(now) {
		t.Fatalf("delta aggregationAt=%q err=%v, want operator --now %s", delta.AggregationAt, err, now)
	}

	candidate := openRepairDB(t, frozenCandidatePath)
	defer closeRepairDB(candidate)
	if cursor, err := loadLatencyCursor(candidate); err != nil || cursor != 5 {
		t.Fatalf("candidate cursor=%d err=%v, want 5", cursor, err)
	}
	var rows []entities.UsageLatencyStat
	if err := candidate.Order("bucket_type, bucket_start, api_group_key").Find(&rows).Error; err != nil {
		t.Fatalf("load repaired latency rows: %v", err)
	}
	if len(rows) != 2 || rows[0].SampleCount != 5 || rows[1].SampleCount != 5 {
		t.Fatalf("expected native hour/day rows for all five events, got %+v", rows)
	}
	var storedSetting entities.AppSetting
	if err := candidate.Where("setting_key = ?", "repair-test").Take(&storedSetting).Error; err != nil || storedSetting.Value == nil || *storedSetting.Value != settingValue {
		t.Fatalf("expected unrelated S1 state to survive candidate promotion: row=%+v err=%v", storedSetting, err)
	}
	var eventCount int64
	if err := candidate.Model(&entities.UsageEvent{}).Where("id = ?", 5).Count(&eventCount).Error; err != nil || eventCount != 1 {
		t.Fatalf("expected raw S1 event to survive candidate promotion: count=%d err=%v", eventCount, err)
	}

	if _, err := Delta(DeltaOptions{SourcePath: sourcePath, CandidatePath: copySQLiteFile(t, sourcePath, "wrong-hash-s1.db"), StageCandidatePath: stageCandidatePath, StageCandidateSHA256: strings.Repeat("0", 64), ReceiptPath: stagePath, OutputPath: filepath.Join(t.TempDir(), "wrong-hash.json"), Now: now, RepairImage: repairImage, ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
		t.Fatal("expected delta to reject an incorrect stage-candidate hash")
	}
	if _, err := Delta(DeltaOptions{SourcePath: sourcePath, CandidatePath: mutateCandidateNonLatency(t, sourcePath, now), StageCandidatePath: stageCandidatePath, StageCandidateSHA256: fileDigest(t, stageCandidatePath), ReceiptPath: stagePath, OutputPath: filepath.Join(t.TempDir(), "source-mismatch.json"), Now: now, RepairImage: repairImage, ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
		t.Fatal("expected delta to reject a frozen candidate that differs from source S1")
	}
	stageCandidate = openRepairDB(t, stageCandidatePath)
	driftValue := "drift"
	if err := stageCandidate.Create(&entities.AppSetting{SettingKey: "stage-candidate-drift", Value: &driftValue, ValueType: entities.AppSettingValueTypeJSON, CreatedAt: now, UpdatedAt: now}).Error; err != nil {
		t.Fatalf("mutate stage candidate: %v", err)
	}
	closeRepairDB(stageCandidate)
	if _, err := Delta(DeltaOptions{SourcePath: sourcePath, CandidatePath: copySQLiteFile(t, sourcePath, "stage-drift-s1.db"), StageCandidatePath: stageCandidatePath, StageCandidateSHA256: fileDigest(t, stageCandidatePath), ReceiptPath: stagePath, OutputPath: filepath.Join(t.TempDir(), "stage-drift.json"), Now: now, RepairImage: repairImage, ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
		t.Fatal("expected delta to reject stage-candidate semantic drift")
	}
}

func TestStageRefusesInPlaceMutationAndInterleavedPartition(t *testing.T) {
	location, err := time.LoadLocation("America/Toronto")
	if err != nil {
		t.Fatalf("load Toronto: %v", err)
	}
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, location)
	sourcePath, _ := makeRepairPair(t, now)
	if _, err := Stage(StageOptions{SourcePath: sourcePath, CandidatePath: sourcePath, ReceiptPath: filepath.Join(t.TempDir(), "in-place.json"), Now: now, RepairImage: testRepairImage(), ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
		t.Fatal("expected in-place stage to fail")
	}
	db := openRepairDB(t, sourcePath)
	defer closeRepairDB(db)
	trueValue := true
	if err := db.Create(&entities.UsageEvent{ID: 1, EventKey: "duplicate-hot", APIGroupKey: "group-a", Timestamp: now, Generate: &trueValue, TTFTMS: int64Pointer(100), LatencyMS: 700}).Error; err != nil {
		t.Fatalf("seed duplicate hot ID: %v", err)
	}
	if _, err := migration.LoadUsageEventReplayPartitionManifest(db); err == nil {
		t.Fatal("expected duplicate archive/hot ID to be rejected")
	}
}

func TestReceiptAndStageInputGuards(t *testing.T) {
	location, err := time.LoadLocation("America/Toronto")
	if err != nil {
		t.Fatalf("load Toronto: %v", err)
	}
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, location)
	sourcePath, candidatePath := makeRepairPair(t, now)
	if _, err := Stage(StageOptions{SourcePath: sourcePath, CandidatePath: candidatePath, ReceiptPath: filepath.Join(t.TempDir(), "bad-image.json"), Now: now, RepairImage: "not-digest-pinned", ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
		t.Fatal("expected stage to reject a non-digest-pinned image")
	}
	if _, err := Stage(StageOptions{SourcePath: sourcePath, CandidatePath: candidatePath, ReceiptPath: filepath.Join(t.TempDir(), "expired.json"), Now: now, RepairImage: testRepairImage(), ExpiresAt: time.Now().Add(-time.Minute)}); err == nil {
		t.Fatal("expected stage to reject an expired receipt window")
	}
	symlinkPath := filepath.Join(t.TempDir(), "candidate-link.db")
	if err := os.Symlink(candidatePath, symlinkPath); err != nil {
		t.Fatalf("make candidate symlink: %v", err)
	}
	if _, err := Stage(StageOptions{SourcePath: sourcePath, CandidatePath: symlinkPath, ReceiptPath: filepath.Join(t.TempDir(), "symlink.json"), Now: now, RepairImage: testRepairImage(), ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
		t.Fatal("expected stage to reject a symlinked candidate")
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		target := filepath.Join(t.TempDir(), "empty"+suffix)
		if err := os.WriteFile(target, nil, 0o600); err != nil {
			t.Fatalf("write empty sidecar target: %v", err)
		}
		link := sourcePath + suffix
		if err := os.Symlink(target, link); err != nil {
			t.Fatalf("make source %s sidecar symlink: %v", suffix, err)
		}
		if _, err := Stage(StageOptions{SourcePath: sourcePath, CandidatePath: candidatePath, ReceiptPath: filepath.Join(t.TempDir(), "sidecar"+suffix+".json"), Now: now, RepairImage: testRepairImage(), ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
			t.Fatalf("expected stage to reject a %s sidecar symlink", suffix)
		}
		if err := os.Remove(link); err != nil {
			t.Fatalf("remove source %s sidecar symlink: %v", suffix, err)
		}
		if err := os.WriteFile(link, nil, 0o600); err != nil {
			t.Fatalf("write empty source %s sidecar: %v", suffix, err)
		}
		if _, err := Stage(StageOptions{SourcePath: sourcePath, CandidatePath: candidatePath, ReceiptPath: filepath.Join(t.TempDir(), "empty-sidecar"+suffix+".json"), Now: now, RepairImage: testRepairImage(), ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
			t.Fatalf("expected stage to reject an empty %s input sidecar", suffix)
		}
		if err := os.Remove(link); err != nil {
			t.Fatalf("remove empty source %s sidecar: %v", suffix, err)
		}
	}
	privatePath := filepath.Join(t.TempDir(), "private.json")
	if err := os.WriteFile(privatePath, []byte(`{"unexpected":true}`), 0o600); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	if _, _, err := readStageReceipt(privatePath); err == nil {
		t.Fatal("expected unknown receipt JSON to fail")
	}
	if err := os.Chmod(privatePath, 0o644); err != nil {
		t.Fatalf("chmod receipt: %v", err)
	}
	if _, _, err := readStageReceipt(privatePath); err == nil {
		t.Fatal("expected world-readable receipt to fail")
	}
	if err := os.Chmod(privatePath, 0o600); err != nil {
		t.Fatalf("restore receipt mode: %v", err)
	}
	receiptLink := filepath.Join(t.TempDir(), "receipt-link.json")
	if err := os.Symlink(privatePath, receiptLink); err != nil {
		t.Fatalf("make receipt symlink: %v", err)
	}
	if _, _, err := readStageReceipt(receiptLink); err == nil {
		t.Fatal("expected symlink receipt to fail")
	}
}

func TestReplayManifestUsesExplicitStorageColumnsAcrossSchemaExtension(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "replay.db")), &gorm.Config{})
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	defer closeRepairDB(db)
	columns := strings.Split(entities.UsageEventStorageColumns, ", ")
	definitions := make([]string, 0, len(columns))
	for _, column := range columns {
		definition := column + " TEXT"
		if column == "id" {
			definition = "id INTEGER PRIMARY KEY"
		}
		definitions = append(definitions, definition)
	}
	for _, table := range []string{"usage_events_archive", "usage_events"} {
		if err := db.Exec(fmt.Sprintf("CREATE TABLE %s (%s)", table, strings.Join(definitions, ", "))).Error; err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
	}
	// An archive-only later column is legitimate schema evolution.  A raw
	// SELECT * UNION would fail its arity check; the manifest remains bound to
	// the explicit persisted-event storage contract instead.
	if err := db.Exec("ALTER TABLE usage_events_archive ADD COLUMN future_archive_only TEXT").Error; err != nil {
		t.Fatalf("extend archive schema: %v", err)
	}
	if err := db.Exec("INSERT INTO usage_events_archive (id, future_archive_only) VALUES (1, 'extra')").Error; err != nil {
		t.Fatalf("insert archive: %v", err)
	}
	if err := db.Exec("INSERT INTO usage_events (id) VALUES (2)").Error; err != nil {
		t.Fatalf("insert hot: %v", err)
	}
	manifest, err := migration.LoadUsageEventReplayPartitionManifest(db)
	if err != nil {
		t.Fatalf("manifest with archive-only extension: %v", err)
	}
	if manifest.Union.Count != 2 || manifest.Union.MinID != 1 || manifest.Union.MaxID != 2 {
		t.Fatalf("unexpected explicit-column union manifest: %+v", manifest.Union)
	}
}

func TestSQLiteFileURIHandlesWindowsDrivePaths(t *testing.T) {
	uri := sqliteFileURIForGOOS(`D:\a\_temp\source.db`, "mode=ro&immutable=1", "windows")
	if uri != "file:///D:/a/_temp/source.db?mode=ro&immutable=1" {
		t.Fatalf("unexpected Windows file URI: %s", uri)
	}
}

func TestSQLiteFileURIKeepsPosixBackslashDistinct(t *testing.T) {
	withBackslash := sqliteFileURIForGOOS(`/tmp/a\b.db`, "mode=ro&immutable=1", "linux")
	withSlash := sqliteFileURIForGOOS(`/tmp/a/b.db`, "mode=ro&immutable=1", "linux")
	if withBackslash == withSlash {
		t.Fatalf("distinct POSIX paths aliased to one URI: %s", withBackslash)
	}
	if !strings.Contains(withBackslash, `%5C`) {
		t.Fatalf("POSIX backslash was not preserved as path data: %s", withBackslash)
	}
}

func makeRepairPair(t *testing.T, now time.Time) (string, string) {
	t.Helper()
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	db := openRepairDB(t, sourcePath)
	if err := db.AutoMigrate(entities.All()...); err != nil {
		t.Fatalf("migrate source: %v", err)
	}
	trueValue := true
	archive := []entities.UsageEventArchive{
		{ID: 1, EventKey: "archive-1", APIGroupKey: "group-a", Timestamp: now.Add(-5 * time.Minute), Generate: &trueValue, TTFTMS: int64Pointer(100), LatencyMS: 700},
		{ID: 2, EventKey: "archive-2", APIGroupKey: "group-a", Timestamp: now.Add(-4 * time.Minute), Generate: &trueValue, TTFTMS: int64Pointer(110), LatencyMS: 710},
	}
	hot := []entities.UsageEvent{
		{ID: 3, EventKey: "hot-3", APIGroupKey: "group-a", Timestamp: now.Add(-3 * time.Minute), Generate: &trueValue, TTFTMS: int64Pointer(120), LatencyMS: 720},
		{ID: 4, EventKey: "hot-4", APIGroupKey: "group-a", Timestamp: now.Add(-2 * time.Minute), Generate: &trueValue, TTFTMS: int64Pointer(130), LatencyMS: 730},
	}
	if err := db.Create(&archive).Error; err != nil {
		t.Fatalf("seed archive: %v", err)
	}
	if err := db.Create(&hot).Error; err != nil {
		t.Fatalf("seed hot: %v", err)
	}
	for _, name := range []entities.UsageAggregationCheckpointName{entities.UsageAggregationCheckpointOverview, entities.UsageAggregationCheckpointActivity, entities.UsageAggregationCheckpointLatency} {
		if err := db.Create(&entities.UsageAggregationCheckpoint{Name: name, LastAggregatedUsageEventID: 4, CreatedAt: now, UpdatedAt: now}).Error; err != nil {
			t.Fatalf("seed %s checkpoint: %v", name, err)
		}
	}
	// Stage must be able to replace a corrupt persisted Latency row without
	// decoding it.  The malformed bytes simulate the live failure class.
	if err := db.Exec(`INSERT INTO usage_latency_stats (bucket_type, bucket_start, api_group_key, sample_count, max_ttft_ms, max_latency_ms, format_version, ttft_sketch, latency_sketch, sample_points, created_at, updated_at) VALUES ('hour', ?, 'group-a', 1, 100, 700, 1, x'00', x'00', x'', ?, ?)`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)).Error; err != nil {
		t.Fatalf("seed corrupt latency row: %v", err)
	}
	closeRepairDB(db)
	candidatePath := filepath.Join(t.TempDir(), "candidate.db")
	bytes, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatalf("read source copy: %v", err)
	}
	if err := os.WriteFile(candidatePath, bytes, 0o600); err != nil {
		t.Fatalf("write candidate copy: %v", err)
	}
	return sourcePath, candidatePath
}

func openRepairDB(t *testing.T, path string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(path+"?_busy_timeout=5000"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	if err := db.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
		t.Fatalf("enable WAL for %s: %v", path, err)
	}
	return db
}

func openReadRepairDB(t *testing.T, path string) *gorm.DB {
	t.Helper()
	db, err := openSQLiteReadOnly(path)
	if err != nil {
		t.Fatalf("open immutable/query-only %s: %v", path, err)
	}
	return db
}

func closeRepairDB(db *gorm.DB) {
	_ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)").Error
	if sqlDB, err := db.DB(); err == nil {
		_ = sqlDB.Close()
	}
}
func int64Pointer(value int64) *int64 { return &value }
func fileDigest(t *testing.T, path string) string {
	t.Helper()
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:])
}

func copySQLiteFile(t *testing.T, sourcePath, name string) string {
	t.Helper()
	destination := filepath.Join(t.TempDir(), name)
	bytes, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatalf("read source copy: %v", err)
	}
	if err := os.WriteFile(destination, bytes, 0o600); err != nil {
		t.Fatalf("write source copy: %v", err)
	}
	return destination
}

func mutateCandidateNonLatency(t *testing.T, sourcePath string, now time.Time) string {
	t.Helper()
	path := copySQLiteFile(t, sourcePath, "mismatched-s1.db")
	db := openRepairDB(t, path)
	defer closeRepairDB(db)
	value := "candidate-only"
	if err := db.Create(&entities.AppSetting{SettingKey: "candidate-only", Value: &value, ValueType: entities.AppSettingValueTypeJSON, CreatedAt: now, UpdatedAt: now}).Error; err != nil {
		t.Fatalf("mutate candidate S1: %v", err)
	}
	return path
}

func testRepairImage() string {
	return "ghcr.io/ticketfi/cpa-usage-keeper@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}

func assertStandaloneSQLite(t *testing.T, path string) {
	t.Helper()
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("expected standalone SQLite database without %s sidecar, err=%v", suffix, err)
		}
	}
}
