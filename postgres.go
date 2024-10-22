package main

import (
	"database/sql"
	"strconv"

	"github.com/rtk-tickets/common/models/events"
	"github.com/rtk-tickets/common/util/database/tables"
	dbTypes "github.com/rtk-tickets/common/util/database/types"
	"github.com/rtk-tickets/common/util/logging"
)

func getDuplicatedEvents(dbPostgres *sql.DB) (map[uint64][]events.Event, error) {
	q := `
	with
		marketplaces as (select *, jsonb_array_elements(marketplaces) as marketplace_info
			from rtk_events_prod),
	    events_by_tdid as (select *, marketplaces.marketplace_info ->> 'item_id' as tdid
	        from marketplaces where marketplaces.marketplace_info ->> 'name' = 'tradedesk'),
	    dup_count_per_event as (select tdid, count(distinct(id)) as count, array_agg(id) as ids from events_by_tdid group by tdid),
	    events as (select * from events_by_tdid left join dup_count_per_event on events_by_tdid.tdid = dup_count_per_event.tdid where count > 1)
    select id, name, start_date_utc, start_date_est, venue_id, venue_name, delayed_delivery, accepted_offers, failed_offers, type, active, deleted, on_offers, on_snooze, payment_method, on_sapi_pricing, using_sapi_pricing, marketplaces
		from events;
	`

	rows, err := dbPostgres.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	duplicatedEvents := make(map[uint64][]events.Event)
	for rows.Next() {
		var rtkEvent events.Event

		tables.Scan(logging.NewNopLogger(), []dbTypes.TableAgnosticColumn{
			"e.id",
			"e.name",
			"e.start_date_utc",
			"e.start_date_est",
			"e.venue_id",
			"e.venue_name",
			"e.delayed_delivery",
			"e.accepted_offers",
			"e.failed_offers",
			"e.type",
			"e.active",
			"e.deleted",
			"e.on_offers",
			"e.on_snooze",
			"e.payment_method",
			"e.on_sapi_pricing",
			"e.using_sapi_pricing",
			"e.marketplaces",
		}, rows, &rtkEvent)

		tdInfo, ok := rtkEvent.Marketplaces[events.EventSource{
			Name:            "tradedesk",
			MarketplaceName: "ticketmaster",
		}]
		if !ok {
			continue
		}

		tdId, err := strconv.ParseUint(*tdInfo.ItemID, 10, 64)
		if err != nil {
			return nil, err
		}

		duplicatedEvents[tdId] = append(duplicatedEvents[tdId], rtkEvent)
	}

	return duplicatedEvents, nil
}
