package main

import (
	_ "github.com/lib/pq"

	"github.com/rtk-tickets/common/models/events"
	"github.com/rtk-tickets/common/services/vault"
	"github.com/rtk-tickets/common/util/logging"
)

func connectVault() (vault.Vault, error) {
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
