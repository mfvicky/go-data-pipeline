package repository

import (
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

//go:embed migrations/*.sql
var migrations embed.FS

type postgresStore struct {
	db    *gorm.DB
	sqlDB *sql.DB
}

type jobRow struct {
	ID   string          `gorm:"column:id;primaryKey;type:text"`
	Data json.RawMessage `gorm:"column:data;type:jsonb;serializer:json;not null"`
}

// OpenPostgres uses GORM's pgx-backed PostgreSQL driver, without CGO/libpq.
// The existing jobs(id,data JSONB) format is preserved during migration.
func OpenPostgres(dsn string) (*JobRepository, error) { return OpenPostgresWithRecovery(dsn, false) }

// recoverPending keeps in-flight jobs queued for RabbitMQ redelivery after restart.
// In standalone memory-queue mode the previous behavior (mark interrupted jobs
// failed) remains the default so jobs never stay indefinitely pending.
func OpenPostgresWithRecovery(dsn string, recoverPending bool) (*JobRepository, error) {
	if strings.TrimSpace(dsn) == "" && os.Getenv("PGHOST") == "" {
		return nil, fmt.Errorf("set DATABASE_URL or PGHOST for PostgreSQL storage")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{TranslateError: true, SkipDefaultTransaction: false})
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL ORM: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(12)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	store := &postgresStore{db: db, sqlDB: sqlDB}
	if err := store.migrate(); err != nil {
		_ = store.close()
		return nil, err
	}
	repo := NewJobRepository()
	repo.store = store
	if err := repo.loadPersistent(recoverPending); err != nil {
		_ = store.close()
		return nil, err
	}
	return repo, nil
}

func (s *postgresStore) migrate() error {
	if err := s.db.Exec("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)").Error; err != nil {
		return err
	}
	var versions []int
	if err := s.db.Table("schema_migrations").Pluck("version", &versions).Error; err != nil {
		return err
	}
	applied := make(map[int]bool, len(versions))
	for _, v := range versions {
		applied[v] = true
		if v > 3 {
			return fmt.Errorf("database version %d is newer than supported version 3", v)
		}
	}
	names, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(names, func(i, j int) bool { return names[i].Name() < names[j].Name() })
	for _, file := range names {
		name := file.Name()
		if file.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		v, err := strconv.Atoi(strings.SplitN(name, "_", 2)[0])
		if err != nil {
			return fmt.Errorf("invalid migration %q", name)
		}
		if applied[v] {
			continue
		}
		contents, err := migrations.ReadFile(filepath.ToSlash(filepath.Join("migrations", name)))
		if err != nil {
			return err
		}
		if err := s.db.Transaction(func(tx *gorm.DB) error {
			// pgx extended protocol accepts one statement per prepared command.
			// Our numbered SQL migrations contain simple DDL statements only.
			for _, statement := range strings.Split(string(contents), ";") {
				statement = strings.TrimSpace(statement)
				if statement == "" {
					continue
				}
				if err := tx.Exec(statement).Error; err != nil {
					return fmt.Errorf("migration %d: %w", v, err)
				}
			}
			return tx.Exec("INSERT INTO schema_migrations (version) VALUES (?) ON CONFLICT DO NOTHING", v).Error
		}); err != nil {
			return err
		}
	}
	return nil
}
func (s *postgresStore) put(id string, snapshot jobSnapshot) error {
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	return s.db.Table("jobs").Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoUpdates: clause.AssignmentColumns([]string{"data"})}).Create(&jobRow{ID: id, Data: raw}).Error
}
func (s *postgresStore) remove(id string) error {
	return s.db.Table("jobs").Where("id = ?", id).Delete(&jobRow{}).Error
}
func (s *postgresStore) all() (map[string]jobSnapshot, error) {
	var rows []jobRow
	if err := s.db.Table("jobs").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]jobSnapshot, len(rows))
	for _, row := range rows {
		var value jobSnapshot
		if err := json.Unmarshal(row.Data, &value); err != nil {
			return nil, fmt.Errorf("decode job %s: %w", row.ID, err)
		}
		out[row.ID] = value
	}
	return out, nil
}
func (s *postgresStore) close() error {
	if s.sqlDB == nil {
		return nil
	}
	return s.sqlDB.Close()
}
func (s *postgresStore) ping() error {
	if s.sqlDB == nil {
		return errors.New("database closed")
	}
	return s.sqlDB.Ping()
}
