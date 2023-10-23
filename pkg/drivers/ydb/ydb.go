package ydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"

	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

const (
	defaultDSN = "grpc://localhost:2135/root?go_query_bind=declare,positional"
)

var (
	schema = []string{
		`CREATE TABLE kine_id
			(
				next_value Uint64,
				PRIMARY KEY (next_value)
			);`,
		`CREATE TABLE kine
 			(
 				id Uint64,
				name String,
				created UInt64,
				deleted UInt64,
 				create_revision UInt64,
 				prev_revision UInt64,
 				lease UInt64,
 				value Bytes,
 				old_value Bytes,
				PRIMARY KEY (id),
				INDEX kine_name_index GLOBAL ON ( name ),
				INDEX kine_name_index GLOBAL ON ( name ),
				INDEX kine_name_id_index GLOBAL ON kine ( name, id ),
				INDEX kine_id_deleted_index GLOBAL ON ( id, deleted ),
				INDEX kine_prev_revision_index GLOBAL ON ( prev_revision ),
				INDEX kine_name_prev_revision_uindex ON ( name, prev_revision )
			)
			WITH (
					AUTO_PARTITIONING_BY_LOAD = ENABLED
			);`,
	}

	initQuery = []string{
		`UPSERT INTO kine_id (next_value) VALUES (1);`,
	}
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	parsedDSN, err := prepareDSN(dataSourceName, tlsInfo)
	if err != nil {
		return nil, err
	}

	dialect, err := generic.Open(ctx, "ydb", parsedDSN, connPoolConfig, "$", true, metricsRegisterer)
	if err != nil {
		return nil, err
	}

	dialect.LastInsertID = true

	// TODO (apkobzev): find approach to determine size of YDB table, maybe SELECT COUNT(*)
	dialect.GetSizeSQL = "SELECT 1"

	dialect.CompactSQL = `DELETE FROM kine AS kv
		WHERE
		kv.id IN (
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
		)
	`

	dialect.InsertLastInsertIDSQL = `BEGIN;
		DECLARE $id AS Uint64;
		$id = (SELECT next_value FROM kine_id);
		INSERT INTO kine(uuid, name, created, deleted, create_revision, prev_revision, lease, value, old_value) values(?, ?, ?, ?, ?, ?, ?, ?, ?);
		UPDATE next_value SET $id + 1;
		COMMIT;
	`

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

	dialect.Migrate(context.Background())
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
					ydb.WithQueryMode(ctx, ydb.ScanQueryMode),
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
