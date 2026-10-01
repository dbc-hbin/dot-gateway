package bridge

import "errors"

// ReplyCancellation distinguishes deliberate non-delivery from successful send.
type ReplyCancellation struct {
	At   float64 `json:"at"`
	Code string  `json:"code"`
}

func replyCancelled(db *storeConn, id string) (bool, error) {
	var n int
	err := db.QueryRow("SELECT count(*) FROM reply_cancellations WHERE reply_id=?", id).Scan(&n)
	return n != 0, err
}

// cancelReplyIfUnsent must run inside the caller's transaction. False means the
// reply cannot safely be abandoned (sending/uncertain, or already fully sent).
// True also covers an existing cancellation, without changing its original code.
func cancelReplyIfUnsent(db *storeConn, id, code string) (bool, error) {
	if code == "" || symbolicCode(code) != code {
		return false, errors.New("invalid cancellation code")
	}
	d, err := delivery(db, id)
	if err != nil {
		return false, err
	}
	if d.State == "cancelled" {
		return true, nil
	}
	unsent := false
	for _, chunk := range d.Chunks {
		switch chunk.State {
		case "sending", "uncertain":
			return false, nil
		case "pending", "failed":
			unsent = true
		case "sent", "cancelled":
		default:
			return false, errors.New("unknown chunk state cannot be cancelled")
		}
	}
	if !unsent {
		return false, nil
	}
	if _, err = db.Exec("INSERT INTO reply_cancellations(reply_id,cancelled_at,code) VALUES(?,?,?)", id, epoch(), code); err != nil {
		return false, err
	}
	if _, err = db.Exec("UPDATE chunks SET state='cancelled',code=? WHERE reply_id=? AND state='pending'", code, id); err != nil {
		return false, err
	}
	return true, nil
}

// CancelReply durably abandons the unsent remainder of a reply. It preserves
// failed and sent chunks as evidence, including their attempts, codes and IDs.
// The cancellation and pending-chunk transitions commit atomically with queue
// advancement. It never deletes data, marks a failure sent, or retries a POST.
//
// Sending or uncertain chunks must be reconciled first; cancellation must not
// release later replies while an earlier POST may still be in flight or unacked.
// A cancelled reply can never be requeued, including after restart.
func (s *Store) CancelReply(id string) (Delivery, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			cancelled, err := cancelReplyIfUnsent(db, id, "operator_cancelled")
			if err != nil {
				return nil, err
			}
			if !cancelled {
				return nil, errors.New("only unsent replies without sending or uncertain chunks can be cancelled")
			}
			return delivery(db, id)
		})
	})
	if err != nil {
		return Delivery{}, err
	}
	return v.(Delivery), nil
}
