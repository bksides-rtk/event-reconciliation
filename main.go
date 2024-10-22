package main

import (
	"encoding/json"
	"fmt"

	"github.com/rtk-tickets/common/models/events"
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

		canonicalIdx, canonicalInfo := find(rtkInfos, func(info events.Event) bool {
			return !*info.Deleted
		})

		if canonicalIdx == -1 {
			canonicalIdx, canonicalInfo = 0, rtkInfos[0]
		}
		rtkInfos = append(rtkInfos[:canonicalIdx], rtkInfos[canonicalIdx+1:]...)

		rtkInfoJson, err := json.MarshalIndent(canonicalInfo, "\t", "\t")
		if err != nil {
			panic(err)
		}

		fmt.Printf("\tCanonical RTK Info: %+v\n", string(rtkInfoJson))
		fmt.Printf("\tNon-Canonical Duplicates:\n")

		var mergedDups events.Event
		for _, rtkInfo := range rtkInfos {
			rtkInfoJson, err := json.MarshalIndent(rtkInfo, "\t\t", "\t")
			if err != nil {
				panic(err)
			}

			fmt.Printf("\t\t%s\n", string(rtkInfoJson))

			_, err = mergedDups.Merge(rtkInfo)
			if err != nil {
				panic(err)
			}
		}

		mergedDups = prepForMerge(mergedDups)
		diff, err := canonicalInfo.Merge(mergedDups)
		if err != nil {
			panic(err)
		}
		if len(diff.Marketplaces) != 0 {
			diff.Marketplaces = canonicalInfo.Marketplaces
		}

		rtkInfoJson, err = json.MarshalIndent(canonicalInfo, "", "\t")
		if err != nil {
			panic(err)
		}
		fmt.Printf("\tMerged Canonical RTK Info: %+v\n", string(rtkInfoJson))

		rtkInfoJson, err = json.MarshalIndent(diff, "", "\t")
		if err != nil {
			panic(err)
		}
		fmt.Printf("\tDiff: %+v\n", string(rtkInfoJson))

		updateCanonical(dbPostgres, *canonicalInfo.ID, diff)
		markNonCanonicalsForDelete(dbPostgres, mapSlice(rtkInfos, func(info events.Event) uint64 {
			return *info.ID
		}))
	}
}
