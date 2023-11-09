package ydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/sirupsen/logrus"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

const (
	defaultMaxIdleConns = 2 // copied from database/sql
	defaultDSN          = "grpc://localhost:2135/root"
)

var (
	schema = []string{
		`CREATE TABLE kine_id
			(
				id Int64,
				next_value Int64,
				PRIMARY KEY (id)
			)`,
		`CREATE TABLE kine (
 				id Int64,
				name String,
				created Int64,
				deleted Int64,
 				create_revision Int64,
 				prev_revision Int64,
 				lease Int64,
 				value Bytes,
 				old_value Bytes,
				INDEX kine_name_index GLOBAL ON (name),
				INDEX kine_name_id_index GLOBAL ON (name, id),
				INDEX kine_id_deleted_index GLOBAL ON (id, deleted),
				INDEX kine_prev_revision_index GLOBAL ON (prev_revision),
				INDEX kine_name_prev_revision_uindex GLOBAL ON (name, prev_revision),
				PRIMARY KEY (id)
			)
			WITH (
				AUTO_PARTITIONING_BY_LOAD = ENABLED
			)`,
	}

	initQuery = []string{
		`UPSERT INTO kine_id (id, next_value) VALUES (1, 1)`,
	}

	columns         = "kv.id, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"
	idSQL           = "$id = (SELECT MAX(rkv.id) FROM kine AS rkv);"
	prevRevisionSQL = "$prev_revision = (SELECT MAX(crkv.prev_revision) FROM kine AS crkv WHERE crkv.name = 'compact_rev_key');"
	maxkvSQL        = "$maxkv = (SELECT MAX(mkv.id) FROM kine AS mkv WHERE mkv.name LIKE ? %s GROUP BY mkv.name);"
	ikvSQL          = "$ikv = (SELECT MAX(ikv.id) FROM kine AS ikv WHERE ikv.name = ? AND ikv.id <= ?);"
	initSQL         = `
		PRAGMA Warning("disable", "4527");
		DECLARE $id AS Int64;
		DECLARE $prev_revision AS Int64;
		DECLARE $mkv AS Int64;
		DECLARE $ikv AS Int64;
	`

	selectSQL = fmt.Sprintf(`SELECT $id, $prev_revision, %s FROM kine as kv`, columns)
	listSQL   = fmt.Sprintf(`%s WHERE $maxkv = kv.id AND (kv.deleted = 0 OR ?) ORDER BY kv.id ASC`, selectSQL)
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	parsedDSN, err := prepareDSN(dataSourceName, tlsInfo)
	if err != nil {
		return nil, err
	}

	dialect, err := Open(ctx, "ydb", parsedDSN, connPoolConfig, metricsRegisterer)
	if err != nil {
		return nil, err
	}

	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(ydb.Error); ok && Ydb.StatusIds_StatusCode(err.Code()) == Ydb.StatusIds_PRECONDITION_FAILED {
			return server.ErrKeyExists
		}
		return err
	}

	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(ydb.Error); ok {
			return err.Name()
		}
		return err.Error()
	}

	if err := setup(ctx, dialect.DB); err != nil {
		return nil, err
	}

	return logstructured.New(sqllog.New(dialect)), nil
}

func setup(ctx context.Context, db *sql.DB) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")
	err := retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		exists := true
		err := cc.Raw(func(drvConn interface{}) (err error) {
			q, ok := drvConn.(interface {
				IsTableExists(context.Context, string) (bool, error)
			})

			if !ok {
				return errors.New("drvConn does not implement extended API")
			}

			exists, err = q.IsTableExists(ctx, "kine")
			return err
		})
		if err != nil {
			return err
		}
		if !exists {
			for _, stmt := range schema {
				logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
				_, err = cc.ExecContext(
					ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
					stmt,
				)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "create kine tables failed: %v", err)
					return err
				}
			}
			for _, stmt := range initQuery {
				logrus.Tracef("INIT EXEC : %v", util.Stripped(stmt))
				_, err = cc.ExecContext(
					ydb.WithQueryMode(ctx, ydb.DataQueryMode),
					stmt,
				)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "initialize kine tables failed: %v", err)
					return err
				}
			}
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil
}

func prepareDSN(dataSourceName string, tlsInfo tls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		dataSourceName = defaultDSN
	}

	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}

	if len(u.Path) == 0 || u.Path == "/" {
		u.Path = "/kubernetes"
	}

	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", err
	}

	params := url.Values{}
	for k, v := range queryMap {
		params.Add(k, v[0])
	}
	u.RawQuery = params.Encode()

	return u.String(), nil
}

func openAndTest(ctx context.Context, driverName, dataSourceName string) (*sql.DB, error) {
	nativeDriver, err := ydb.Open(
		ctx,
		dataSourceName,
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		return nil, err
	}

	connector, err := ydb.Connector(nativeDriver, ydb.WithAutoDeclare(), ydb.WithPositionalArgs())
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)

	ctxConn, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if err := db.PingContext(ctxConn); err != nil {
			db.Close()
			nativeDriver.Close(ctx)
			return nil, err
		}
	}

	return db, nil
}

func Open(ctx context.Context, driverName, dataSourceName string, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (*generic.Generic, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = openAndTest(ctx, driverName, dataSourceName)
		if err == nil {
			break
		}

		logrus.Errorf("failed to ping connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	configureConnectionPooling(connPoolConfig, db, driverName)

	if metricsRegisterer != nil {
		metricsRegisterer.MustRegister(collectors.NewDBStatsCollector(db, "kine"))
	}

	return &generic.Generic{
		DB: db,

		GetSizeSQL: "SELECT COUNT(*)",

		GetRevisionSQL: fmt.Sprintf(`SELECT 0, 0, %s FROM kine AS kv WHERE kv.id = ?`, columns),

		GetCurrentSQL:        initSQL + idSQL + prevRevisionSQL + fmt.Sprintf(maxkvSQL, "") + listSQL,
		ListRevisionStartSQL: initSQL + idSQL + prevRevisionSQL + fmt.Sprintf(maxkvSQL, "AND mkv.id <= ?") + listSQL,
		GetRevisionAfterSQL:  initSQL + idSQL + prevRevisionSQL + fmt.Sprintf(maxkvSQL, "AND mkv.id <= ?") + ikvSQL + listSQL,

		CountSQL: initSQL + idSQL + prevRevisionSQL + fmt.Sprintf(maxkvSQL, "") + fmt.Sprintf(`SELECT $id, COUNT(c.id) FROM (%s) AS c`, listSQL),

		AfterSQL: initSQL + idSQL + prevRevisionSQL + fmt.Sprintf(`%s WHERE kv.name LIKE ? AND kv.id > ? ORDER BY kv.id ASC`, selectSQL),

		DeleteSQL: `DELETE FROM kine AS kv WHERE kv.id = ?`,

		UpdateCompactSQL: `UPDATE kine SET prev_revision = ? WHERE name = 'compact_rev_key'`,

		CompactSQL: `
			DELETE FROM kine AS kv
			WHERE kv.id IN (
				SELECT kp.prev_revision AS id
				FROM kine AS kp
				WHERE
					kp.name != 'compact_rev_key' AND
					kp.prev_revision != 0 AND
					kp.id <= ?
				UNION ALL
				SELECT kd.id AS id
				FROM kine AS kd
				WHERE
					kd.deleted != 0 AND
					kd.id <= ?
			)`,

		InsertSQL: `
			DECLARE $id AS Int64;
			$id = (SELECT next_value FROM kine_id);
			UPSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value) VALUES ($id, ?, ?, ?, ?, ?, ?, ?, ?);
			UPSERT INTO kine_id(id, next_value) VALUES (1, $id + 1);
			SELECT * FROM $id
		`,

		FillSQL: `
			INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
	}, err
}

func configureConnectionPooling(connPoolConfig generic.ConnectionPoolConfig, db *sql.DB, driverName string) {
	// behavior copied from database/sql - zero means defaultMaxIdleConns; negative means 0
	if connPoolConfig.MaxIdle < 0 {
		connPoolConfig.MaxIdle = 0
	} else if connPoolConfig.MaxIdle == 0 {
		connPoolConfig.MaxIdle = defaultMaxIdleConns
	}

	logrus.Infof("Configuring %s database connection pooling: maxIdleConns=%d, maxOpenConns=%d, connMaxLifetime=%s", driverName, connPoolConfig.MaxIdle, connPoolConfig.MaxOpen, connPoolConfig.MaxLifetime)
	db.SetMaxIdleConns(connPoolConfig.MaxIdle)
	db.SetMaxOpenConns(connPoolConfig.MaxOpen)
	db.SetConnMaxLifetime(connPoolConfig.MaxLifetime)
}
