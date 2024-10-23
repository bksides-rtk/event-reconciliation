package main

import (
	"encoding/json"
	"fmt"

	"github.com/rtk-tickets/common/models/events"
	"github.com/rtk-tickets/common/util"
	"github.com/rtk-tickets/common/util/database"
	dbTypes "github.com/rtk-tickets/common/util/database/types"
	"github.com/rtk-tickets/common/util/logging"
	"github.com/rtk-tickets/common/util/nothing"
)

func main() {
	vault, err := connectVault()
	if err != nil {
		panic(err)
	}

	dbPostgres, err := connectDB(vault)
	if err != nil {
		panic(err)
	}

	duplicatedEvents, err := getDuplicatedEvents(dbPostgres)
	if err != nil {
		panic(err)
	}

	for tdid, rtkInfos := range duplicatedEvents {
		fmt.Printf("Tradedesk ID: %d\n", tdid)

		// map by event name so we only merge events that share the same name
		byName, _ := util.BucketBy(rtkInfos, func(info events.Event) (string, error) {
			return *info.Name, nil
		})

		// for each name and its associated events
		for name, rtkInfos := range byName {
			fmt.Printf("\tName: %s\n", name)

			// find an undeleted event to serve as the canonical event record
			canonicalIdx, canonicalInfo := find(rtkInfos, func(info events.Event) bool {
				return !*info.Deleted
			})

			// if no undeleted event is found, use the first event as the canonical record
			if canonicalIdx == -1 {
				canonicalIdx, canonicalInfo = 0, rtkInfos[0]
			}
			// remove the canonical event from the list of events to merge
			rtkInfos = append(rtkInfos[:canonicalIdx], rtkInfos[canonicalIdx+1:]...)

			// log the canonical event and the non-canonical events
			rtkInfoJson, err := json.MarshalIndent(canonicalInfo, "\t", "\t")
			if err != nil {
				panic(err)
			}
			fmt.Printf("\t\tCanonical RTK Info: %+v\n", string(rtkInfoJson))
			fmt.Printf("\t\tNon-Canonical Duplicates:\n")

			// merge the non-canonical events together
			var mergedDups events.Event
			for _, rtkInfo := range rtkInfos {
				rtkInfoJson, err := json.MarshalIndent(rtkInfo, "\t\t\t", "\t")
				if err != nil {
					panic(err)
				}

				fmt.Printf("\t\t\t%s\n", string(rtkInfoJson))

				_, err = mergedDups.Merge(rtkInfo)
				if err != nil {
					panic(err)
				}
			}

			// prep the merged dups for the final merge into the canonical purchase
			mergedDups = prepForMerge(mergedDups)
			diff, err := canonicalInfo.Merge(mergedDups)
			if err != nil {
				panic(err)
			}
			if len(diff.Marketplaces) != 0 {
				diff.Marketplaces = canonicalInfo.Marketplaces
			}

			// log the merged canonical event and the diff
			rtkInfoJson, err = json.MarshalIndent(canonicalInfo, "", "\t\t")
			if err != nil {
				panic(err)
			}
			fmt.Printf("\t\tMerged Canonical RTK Info: %+v\n", string(rtkInfoJson))

			rtkInfoJson, err = json.MarshalIndent(diff, "", "\t\t")
			if err != nil {
				panic(err)
			}
			fmt.Printf("\t\tDiff: %+v\n", string(rtkInfoJson))

			// in a transaction, update the canonical event and mark the non-canonical events for deletion
			_, err = database.Transact(logging.NewNopLogger(), dbTypes.DBUserFromIface(dbPostgres), func(dbPostgresUser dbTypes.DBIfaceUser) (nothing.Nothing, error) {
				dbPostgres := dbPostgresUser.GetDB()

				err := updateCanonical(dbPostgres, *canonicalInfo.ID, diff)
				if err != nil {
					return nothing.Nothing{}, err
				}

				err = markNonCanonicalsForDelete(dbPostgres, mapSlice(rtkInfos, func(info events.Event) uint64 {
					return *info.ID
				}))
				if err != nil {
					return nothing.Nothing{}, err
				}

				return nothing.Nothing{}, nil
			})
			if err != nil {
				panic(err)
			}
		}

		// delimiter between Tradedesk IDs
		fmt.Println("\n----------------------------------------\n")
	}
}
