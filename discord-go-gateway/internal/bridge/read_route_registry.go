package bridge

import (
	"encoding/json"
	"errors"
)

// Route metadata is bounded and content-free. Historical discovery is performed
// once on schema upgrade, never on every reconnect. Future verified promotion
// refreshes it. The exact-route API also has an indexed single-row fallback.
func initReadRouteRegistry(db *storeConn) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS read_route_registry(channel_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,route TEXT NOT NULL,seen REAL NOT NULL);
 CREATE INDEX IF NOT EXISTS read_route_registry_owner ON read_route_registry(owner_id,seen DESC);
 CREATE INDEX IF NOT EXISTS inbound_read_route_lookup ON inbound(platform,json_extract(envelope,'$.conversation_id'),json_extract(envelope,'$.sender_id'),created DESC) WHERE json_valid(envelope);
 CREATE INDEX IF NOT EXISTS inbound_active_source_order ON inbound(platform,json_extract(envelope,'$.conversation_id'),length(json_extract(envelope,'$.event_id')),json_extract(envelope,'$.event_id')) WHERE state IN ('pending','claimed') AND json_valid(envelope);
 CREATE INDEX IF NOT EXISTS ingress_active_source_order ON ingress_validation(platform,json_extract(envelope,'$.conversation_id'),length(json_extract(envelope,'$.event_id')),json_extract(envelope,'$.event_id'),created) WHERE state IN ('pending','blocked') AND json_valid(envelope);`)
	if err != nil {
		return err
	}
	var done int
	if err = db.QueryRow(`SELECT count(*) FROM runtime WHERE key='read_route_registry_v1'`).Scan(&done); err != nil || done != 0 {
		return err
	}
	rows, err := db.Query(`SELECT envelope,max(created) FROM inbound WHERE json_valid(envelope) AND platform='discord' AND json_extract(envelope,'$.route_kind') IN ('dm','guild_thread') GROUP BY json_extract(envelope,'$.conversation_id') ORDER BY max(created) DESC LIMIT ?`, catchupRouteLimit)
	if err != nil {
		return err
	}
	var seeds []Envelope
	for rows.Next() {
		var raw string
		var at float64
		if err = rows.Scan(&raw, &at); err != nil {
			rows.Close()
			return err
		}
		var e Envelope
		if json.Unmarshal([]byte(raw), &e) != nil {
			rows.Close()
			return errors.New("read_route_registry_invalid")
		}
		seeds = append(seeds, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, e := range seeds {
		if err = rememberReadRouteDB(db, e); err != nil {
			return err
		}
	}
	_, err = db.Exec(`INSERT INTO runtime(key,value)VALUES('read_route_registry_v1','complete')`)
	return err
}
func rememberReadRouteDB(db *storeConn, e Envelope) error {
	if e.Platform != "discord" || !Snowflake(e.ConversationID) || !Snowflake(e.SenderID) || e.SenderIsBot || (e.RouteKind != "dm" && e.RouteKind != "guild_thread") {
		return nil
	}
	route := ReadRoute{e.ConversationID, e.GuildID, e.RouteKind, e.ParentChannelID, e.ThreadType}
	raw, err := json.Marshal(route)
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO read_route_registry(channel_id,owner_id,route,seen)SELECT ?,?,?,? WHERE (SELECT count(*) FROM read_route_registry)<? OR EXISTS(SELECT 1 FROM read_route_registry WHERE channel_id=?) ON CONFLICT(channel_id)DO UPDATE SET owner_id=excluded.owner_id,route=excluded.route,seen=excluded.seen`, e.ConversationID, e.SenderID, string(raw), epoch(), catchupRouteLimit, e.ConversationID)
	return err
}
