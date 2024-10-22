package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/lib/pq"

	"github.com/rtk-tickets/common/models/events"
	"github.com/rtk-tickets/common/services/vault"
	"github.com/rtk-tickets/common/util"
	"github.com/rtk-tickets/common/util/database/tables"
	dbTypes "github.com/rtk-tickets/common/util/database/types"
	"github.com/rtk-tickets/common/util/logging"
)

func connectDefaultDB(vault vault.Vault) (*sql.DB, error) {
	host, err := vault.GetSecret(context.Background(), logging.DefaultLogger, "postgres.host")
	if err != nil {
		return nil, err
	}

	username, err := vault.GetSecret(context.Background(), logging.DefaultLogger, "postgres.username")
	if err != nil {
		return nil, err
	}

	password, err := vault.GetSecret(context.Background(), logging.DefaultLogger, "postgres.password")
	if err != nil {
		return nil, err
	}

	dbname, err := vault.GetSecret(context.Background(), logging.DefaultLogger, "postgres.name")
	if err != nil {
		return nil, err
	}

	connectionString := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", host, username, password, dbname)

	dbPostgres, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}

	pingErr := dbPostgres.Ping()
	if pingErr != nil {
		return nil, pingErr
	}
	fmt.Println("Connected to Postgres DB!")

	return dbPostgres, nil
}

func connectDefaultVault() (vault.Vault, error) {
	var err error
	vault, err := vault.NewAWSVault(logging.DefaultLogger, vault.AWSVaultConfig{
		Region: "us-east-1",
		SecretLocations: map[string]vault.AWSSecretLocator{
			"postgres.username": {
				Name: "RTK_Tickets_Postgres",
				Key:  "username",
			},
			"postgres.password": {
				Name: "RTK_Tickets_Postgres",
				Key:  "password",
			},
			"postgres.host": {
				Name: "RTK_Tickets_Postgres",
				Key:  "host",
			},
			"postgres.name": {
				Name: "RTK_Tickets_Postgres",
				Key:  "dbname",
			},
		},
	})
	if err != nil {
		return nil, err
	}

	return vault, nil
}

func find[T any](vs []T, f func(T) bool) (int, T) {
	for i, v := range vs {
		if f(v) {
			return i, v
		}
	}
	var t T
	return -1, t
}

func updateCanonical(dbPostgres *sql.DB, id uint64, diff events.Event) error {
	logger, _ := logging.NewBasicLogger(logging.BasicLoggerConfig{
		Level: logging.LogLevelDebug,
	})

	if diff.IsEmpty() {
		return nil
	}

	colMap, _ := util.MapBy([]dbTypes.TableSpecificColumn{
		"id",
		"name",
		"start_date_utc",
		"start_date_est",
		"venue_id",
		"venue_name",
		"delayed_delivery",
		"accepted_offers",
		"failed_offers",
		"type",
		"active",
		"deleted",
		"on_offers",
		"on_snooze",
		"payment_method",
		"on_sapi_pricing",
		"using_sapi_pricing",
		"marketplaces",
	}, func(c dbTypes.TableSpecificColumn) (dbTypes.TableAgnosticColumn, error) {
		return dbTypes.TableAgnosticColumn(c), nil
	})

	table := tables.NewTable[*events.Event, uint64](dbPostgres, "rtk_events_prod", "e", colMap)
	_, err := table.Update(logger, events.EventMatcher{
		ID: &id,
	}, &diff)
	return err
}

func markNonCanonicalsForDelete(dbPostgres *sql.DB, ids []uint64) error {
	logger, _ := logging.NewBasicLogger(logging.BasicLoggerConfig{
		Level: logging.LogLevelDebug,
	})

	if len(ids) == 0 {
		return nil
	}

	colMap, _ := util.MapBy([]dbTypes.TableSpecificColumn{
		"id",
		"name",
		"start_date_utc",
		"start_date_est",
		"venue_id",
		"venue_name",
		"delayed_delivery",
		"accepted_offers",
		"failed_offers",
		"type",
		"active",
		"deleted",
		"on_offers",
		"on_snooze",
		"payment_method",
		"on_sapi_pricing",
		"using_sapi_pricing",
		"marketplaces",
	}, func(c dbTypes.TableSpecificColumn) (dbTypes.TableAgnosticColumn, error) {
		return dbTypes.TableAgnosticColumn(c), nil
	})

	table := tables.NewTable[*events.Event, uint64](dbPostgres, "rtk_events_prod", "e", colMap)
	_, err := table.Update(logger, dbTypes.NewMapQuery(map[dbTypes.TableAgnosticColumn]interface{}{
		"e.id": goqu.Op{"in": ids},
	}), &events.Event{
		Deleted: util.PointerTo(true),
	})
	return err
}

func mapSlice[T, U any](vs []T, f func(T) U) []U {
	us := make([]U, len(vs))
	for i, v := range vs {
		us[i] = f(v)
	}
	return us
}

func prepForMerge(e events.Event) events.Event {
	e.ID = nil
	e.Deleted = nil
	return e
}
