package bridge

import (
	"database/sql"
	"errors"
	"regexp"
)

var followupKey = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)

// QueueFollowup appends immutable text chunks to an already delivered ordinary
// reply. The retained original claim binds this continuation to its real owner
// source and exact conversation; it cannot choose a destination or renew work.
func (s *Store) QueueFollowup(id, claim, key, text string) (string, error) {
	if !followupKey.MatchString(key) {
		return "", errors.New("invalid_followup_key")
	}
	parts, err := SplitText(text)
	if err != nil {
		return "", err
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			rows, err := readInbound(db, "SELECT id,state,claim,lease_until,envelope FROM inbound WHERE id=?", id)
			if err != nil {
				return nil, err
			}
			if len(rows) != 1 {
				return nil, ErrClaim
			}
			r := rows[0]
			if claim == "" || r.claim != claim || r.state != "replied" || !isMessageSource(r.event) || !s.policy.Accepts(r.event) {
				return nil, ErrClaim
			}
			current, err := sourceCurrentDB(db, r.event)
			if err != nil {
				return nil, err
			}
			if !current {
				return nil, ErrClaim
			}
			var rid string
			if err = db.QueryRow("SELECT id FROM replies WHERE inbound_id=?", id).Scan(&rid); err != nil {
				return nil, err
			}
			cancelled, err := replyCancelled(db, rid)
			if err != nil {
				return nil, err
			}
			if cancelled {
				return nil, errors.New("followup_reply_cancelled")
			}
			var old string
			err = db.QueryRow("SELECT text FROM reply_followups WHERE reply_id=? AND key=?", rid, key).Scan(&old)
			if err == nil {
				if old != text {
					return nil, errors.New("followup_key_content_conflict")
				}
				return rid, nil
			}
			if err != sql.ErrNoRows {
				return nil, err
			}
			var unfinished, start int
			if err = db.QueryRow("SELECT count(*) FROM chunks WHERE reply_id=? AND state!='sent'", rid).Scan(&unfinished); err != nil {
				return nil, err
			}
			if unfinished != 0 {
				return nil, errors.New("followup_requires_confirmed_delivery")
			}
			if err = db.QueryRow("SELECT COALESCE(max(idx),-1)+1 FROM chunks WHERE reply_id=?", rid).Scan(&start); err != nil {
				return nil, err
			}
			if start == 0 {
				return nil, errors.New("followup_requires_confirmed_delivery")
			}
			if _, err = db.Exec("INSERT INTO reply_followups(reply_id,key,text,start_idx,chunk_count) VALUES(?,?,?,?,?)", rid, key, text, start, len(parts)); err != nil {
				return nil, err
			}
			for i, p := range parts {
				if _, err = db.Exec("INSERT INTO chunks(reply_id,idx,text) VALUES(?,?,?)", rid, start+i, p); err != nil {
					return nil, err
				}
			}
			return rid, nil
		})
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}
