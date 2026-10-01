package collector

import (
	"encoding/json"
	"fmt"
)

const wideReviewField = "wide_reviewed_v1"

// Optional bounded migration/refresh history. It records evaluation order, not
// approval or delivery. Unknown legacy Fields continue to round-trip unchanged.
func loadWideReviewHistory(state *State) (map[string]uint64, uint64, error) {
	history := map[string]uint64{}
	if raw, ok := state.Fields[wideReviewField]; ok {
		if len(raw) == 0 || raw[0] != '{' || json.Unmarshal(raw, &history) != nil || history == nil {
			return nil, 0, fmt.Errorf("invalid wide review history; refusing state update")
		}
	}
	var last uint64
	for key, sequence := range history {
		if sequence == 0 || sequence == ^uint64(0) {
			return nil, 0, fmt.Errorf("invalid wide review sequence; refusing state update")
		}
		if _, retained := state.Fingerprints[key]; !retained {
			delete(history, key)
			continue
		}
		last = max(last, sequence)
	}
	// Reserve enough increments for the hard detail budget; overflow is never
	// silently wrapped or allowed to change the ordering.
	if last > ^uint64(0)-96 {
		return nil, 0, fmt.Errorf("wide review sequence exhausted; refusing state update")
	}
	return history, last, nil
}

func saveWideReviewHistory(state *State, history map[string]uint64) error {
	for key := range history {
		if _, retained := state.Fingerprints[key]; !retained {
			delete(history, key)
		}
	}
	raw, err := json.Marshal(history)
	if err == nil {
		state.Fields[wideReviewField] = raw
	}
	return err
}
