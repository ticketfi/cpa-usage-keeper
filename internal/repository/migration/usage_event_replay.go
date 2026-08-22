package migration

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"cpa-usage-keeper/internal/entities"

	"gorm.io/gorm"
)

const usageAggregationReplayPageLimit = 1000

// UsageEventReplayManifest is a deterministic, content-addressed description
// of one raw usage-event range.  It intentionally hashes SQLite's quoted
// stored values rather than decoded Go values, so a repair can prove it did
// not silently reinterpret a nullable or historical storage representation.
type UsageEventReplayManifest struct {
	Count  int64  `json:"count"`
	MinID  int64  `json:"minId"`
	MaxID  int64  `json:"maxId"`
	SHA256 string `json:"sha256"`
}

// UsageEventReplayPartitionManifest describes the archive/hot split used by
// a latency repair.  Archive and hot may each be empty, but when both have
// rows all archive IDs must precede all hot IDs.
type UsageEventReplayPartitionManifest struct {
	Archive UsageEventReplayManifest `json:"archive"`
	Hot     UsageEventReplayManifest `json:"hot"`
	Union   UsageEventReplayManifest `json:"union"`
}

// LoadUsageAggregationReplayTargetEventID 固定 archive 与 hot 在 migration 启动时的全局最大 ID。
func LoadUsageAggregationReplayTargetEventID(db *gorm.DB) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("database is nil")
	}
	var targetID int64
	if err := db.Raw(`
		SELECT MAX(
			COALESCE((SELECT MAX(id) FROM usage_events_archive), 0),
			COALESCE((SELECT MAX(id) FROM usage_events), 0)
		)`).Scan(&targetID).Error; err != nil {
		return 0, fmt.Errorf("load usage event replay target: %w", err)
	}
	return targetID, nil
}

// LoadUsageAggregationReplayEventPage 只供启动 migration 按全局 ID 顺序重放 archive 与 hot 事件。
func LoadUsageAggregationReplayEventPage(db *gorm.DB, afterID, targetID int64, limit int) ([]entities.UsageEvent, error) {
	if db == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if afterID < 0 || targetID < 0 {
		return nil, fmt.Errorf("usage event replay bounds must be non-negative: after=%d target=%d", afterID, targetID)
	}
	if targetID <= afterID {
		return []entities.UsageEvent{}, nil
	}
	if limit <= 0 {
		return nil, fmt.Errorf("usage event replay page limit must be positive")
	}
	if limit > usageAggregationReplayPageLimit {
		limit = usageAggregationReplayPageLimit
	}

	// 两个分支都按 INTEGER PRIMARY KEY 范围读取，SQLite 会用 MERGE UNION 恢复全局入库顺序。
	columns := entities.UsageEventStorageColumns
	query := fmt.Sprintf(`
		SELECT %s FROM (
			SELECT %s FROM usage_events_archive WHERE id > ? AND id <= ?
			UNION ALL
			SELECT %s FROM usage_events WHERE id > ? AND id <= ?
		) AS usage_event_replay
		ORDER BY id ASC
		LIMIT ?`, columns, columns, columns)
	var events []entities.UsageEvent
	if err := db.Raw(query, afterID, targetID, afterID, targetID, limit).Scan(&events).Error; err != nil {
		return nil, fmt.Errorf("load usage event replay page: %w", err)
	}
	return events, nil
}

// LoadUsageAggregationReplayArchivePage reads only the cold archive in
// primary-key order.  The latency repair uses this before the stock runner
// resumes the hot partition; it must not reimplement the event projection.
func LoadUsageAggregationReplayArchivePage(db *gorm.DB, afterID, targetID int64, limit int) ([]entities.UsageEvent, error) {
	return loadUsageAggregationReplaySingleTablePage(db, "usage_events_archive", afterID, targetID, limit)
}

// LoadUsageAggregationReplayHotPage reads only the current hot partition in
// primary-key order.  It is exported for receipt validation and focused tests.
func LoadUsageAggregationReplayHotPage(db *gorm.DB, afterID, targetID int64, limit int) ([]entities.UsageEvent, error) {
	return loadUsageAggregationReplaySingleTablePage(db, "usage_events", afterID, targetID, limit)
}

func loadUsageAggregationReplaySingleTablePage(db *gorm.DB, table string, afterID, targetID int64, limit int) ([]entities.UsageEvent, error) {
	if db == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if afterID < 0 || targetID < 0 {
		return nil, fmt.Errorf("usage event replay bounds must be non-negative: after=%d target=%d", afterID, targetID)
	}
	if targetID <= afterID {
		return []entities.UsageEvent{}, nil
	}
	if limit <= 0 {
		return nil, fmt.Errorf("usage event replay page limit must be positive")
	}
	if limit > usageAggregationReplayPageLimit {
		limit = usageAggregationReplayPageLimit
	}
	if table != "usage_events" && table != "usage_events_archive" {
		return nil, fmt.Errorf("unsupported usage event replay table %q", table)
	}
	var events []entities.UsageEvent
	query := fmt.Sprintf("SELECT %s FROM %s WHERE id > ? AND id <= ? ORDER BY id ASC LIMIT ?", entities.UsageAggregationEventProjectionColumns, table)
	if err := db.Raw(query, afterID, targetID, limit).Scan(&events).Error; err != nil {
		return nil, fmt.Errorf("load %s usage event replay page: %w", table, err)
	}
	return events, nil
}

// LoadUsageEventReplayPartitionManifest validates that archive and hot form a
// strict, duplicate-free partition and returns deterministic manifests for
// each side and their globally ordered union.
func LoadUsageEventReplayPartitionManifest(db *gorm.DB) (UsageEventReplayPartitionManifest, error) {
	if db == nil {
		return UsageEventReplayPartitionManifest{}, fmt.Errorf("database is nil")
	}
	archive, err := loadUsageEventReplayManifest(db, "usage_events_archive", "")
	if err != nil {
		return UsageEventReplayPartitionManifest{}, err
	}
	hot, err := loadUsageEventReplayManifest(db, "usage_events", "")
	if err != nil {
		return UsageEventReplayPartitionManifest{}, err
	}
	if archive.Count > 0 && hot.Count > 0 && archive.MaxID >= hot.MinID {
		return UsageEventReplayPartitionManifest{}, fmt.Errorf("usage event archive/hot ranges interleave: archive=%d..%d hot=%d..%d", archive.MinID, archive.MaxID, hot.MinID, hot.MaxID)
	}
	var duplicateCount int64
	if err := db.Raw(`SELECT COUNT(*) FROM (
		SELECT id FROM usage_events_archive
		INTERSECT
		SELECT id FROM usage_events
	)`).Scan(&duplicateCount).Error; err != nil {
		return UsageEventReplayPartitionManifest{}, fmt.Errorf("check usage event partition duplicate IDs: %w", err)
	}
	if duplicateCount != 0 {
		return UsageEventReplayPartitionManifest{}, fmt.Errorf("usage event archive/hot partition has %d duplicate IDs", duplicateCount)
	}
	columns := entities.UsageEventStorageColumns
	union, err := loadUsageEventReplayManifest(db, "", fmt.Sprintf(`SELECT %s FROM usage_events_archive UNION ALL SELECT %s FROM usage_events`, columns, columns))
	if err != nil {
		return UsageEventReplayPartitionManifest{}, err
	}
	if union.Count != archive.Count+hot.Count {
		return UsageEventReplayPartitionManifest{}, fmt.Errorf("usage event union count %d does not match archive+hot %d", union.Count, archive.Count+hot.Count)
	}
	return UsageEventReplayPartitionManifest{Archive: archive, Hot: hot, Union: union}, nil
}

// LoadUsageEventReplayPrefixManifest returns the exact globally ordered raw
// prefix through targetID.  Delta uses it to refuse a source whose stage-era
// rows changed even if its current maximum ID looks plausible.
func LoadUsageEventReplayPrefixManifest(db *gorm.DB, targetID int64) (UsageEventReplayManifest, error) {
	if db == nil {
		return UsageEventReplayManifest{}, fmt.Errorf("database is nil")
	}
	if targetID < 0 {
		return UsageEventReplayManifest{}, fmt.Errorf("usage event replay prefix target must be non-negative")
	}
	columns := entities.UsageEventStorageColumns
	return loadUsageEventReplayManifest(db, "", fmt.Sprintf(`SELECT %s FROM (
		SELECT %s FROM usage_events_archive WHERE id <= %d
		UNION ALL
		SELECT %s FROM usage_events WHERE id <= %d
	)`, columns, columns, targetID, columns, targetID))
}

// LoadUsageEventReplayRangeManifest returns the exact globally ordered raw
// event range (afterID, targetID].  It is used for a delta receipt, whose hash
// must describe the same stored values consumed by the native replay codec.
func LoadUsageEventReplayRangeManifest(db *gorm.DB, afterID, targetID int64) (UsageEventReplayManifest, error) {
	if db == nil {
		return UsageEventReplayManifest{}, fmt.Errorf("database is nil")
	}
	if afterID < 0 || targetID < afterID {
		return UsageEventReplayManifest{}, fmt.Errorf("invalid usage event replay range (%d, %d]", afterID, targetID)
	}
	columns := entities.UsageEventStorageColumns
	return loadUsageEventReplayManifest(db, "", fmt.Sprintf(`SELECT %s FROM (
		SELECT %s FROM usage_events_archive WHERE id > %d AND id <= %d
		UNION ALL
		SELECT %s FROM usage_events WHERE id > %d AND id <= %d
	)`, columns, columns, afterID, targetID, columns, afterID, targetID))
}

func loadUsageEventReplayManifest(db *gorm.DB, table, sourceSQL string) (UsageEventReplayManifest, error) {
	if table != "" && table != "usage_events" && table != "usage_events_archive" {
		return UsageEventReplayManifest{}, fmt.Errorf("unsupported usage event manifest table %q", table)
	}
	from := table
	if sourceSQL != "" {
		from = "(" + sourceSQL + ")"
	}
	// quote() retains NULL, text, integer and blob boundaries in a deterministic
	// SQLite representation.  Length-prefixing prevents delimiter ambiguity.
	columns := strings.Split(entities.UsageEventStorageColumns, ", ")
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, "quote("+column+")")
	}
	query := "SELECT " + strings.Join(quoted, ", ") + " FROM " + from + " ORDER BY id ASC"
	sqlDB, err := db.DB()
	if err != nil {
		return UsageEventReplayManifest{}, fmt.Errorf("open usage event manifest database: %w", err)
	}
	rows, err := sqlDB.Query(query)
	if err != nil {
		return UsageEventReplayManifest{}, fmt.Errorf("query usage event manifest: %w", err)
	}
	defer rows.Close()
	hash := sha256.New()
	manifest := UsageEventReplayManifest{}
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		destinations := make([]any, len(values))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return UsageEventReplayManifest{}, fmt.Errorf("scan usage event manifest: %w", err)
		}
		var id int64
		if _, err := fmt.Sscan(values[0].String, &id); err != nil {
			return UsageEventReplayManifest{}, fmt.Errorf("parse usage event manifest ID %q: %w", values[0].String, err)
		}
		if manifest.Count == 0 {
			manifest.MinID = id
		}
		manifest.MaxID = id
		manifest.Count++
		for _, value := range values {
			part := value.String
			fmt.Fprintf(hash, "%d:", len(part))
			hash.Write([]byte(part))
		}
		hash.Write([]byte{'\n'})
	}
	if err := rows.Err(); err != nil {
		return UsageEventReplayManifest{}, fmt.Errorf("iterate usage event manifest: %w", err)
	}
	manifest.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return manifest, nil
}
