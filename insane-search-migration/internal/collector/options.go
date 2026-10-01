package collector

import "fmt"

// ScanOptions bounds discovery only. A candidate is untrusted material for
// assistant review, never an accepted offer or authorization to publish it.
type ScanOptions struct {
	Profile          string `json:"profile"`
	MaxScanPerSource int    `json:"max_scan_per_source"`
	MaxPerSource     int    `json:"max_per_source"`
	MaxTotal         int    `json:"max_total"`
	MaxListingPages  int    `json:"max_listing_pages"`
}

func LegacyScanOptions() ScanOptions {
	return ScanOptions{"legacy", MaxScanPerSource, MaxPerSource, MaxTotal, 1}
}

func WideScanOptions() ScanOptions {
	return ScanOptions{"wide", 120, 12, 48, 3}
}

func OptionsForProfile(profile string) (ScanOptions, error) {
	switch profile {
	case "legacy":
		return LegacyScanOptions(), nil
	case "wide":
		return WideScanOptions(), nil
	default:
		return ScanOptions{}, fmt.Errorf("unknown collection profile %q (use wide or legacy)", profile)
	}
}

func (o ScanOptions) Validate() error {
	if _, err := OptionsForProfile(o.Profile); err != nil {
		return err
	}
	if o.MaxScanPerSource < 1 || o.MaxScanPerSource > 200 || o.MaxPerSource < 1 || o.MaxPerSource > 24 || o.MaxTotal < 1 || o.MaxTotal > 96 || o.MaxListingPages < 1 || o.MaxListingPages > 5 {
		return fmt.Errorf("collection limits must be rows 1..200, per-source details 1..24, total details 1..96, pages 1..5")
	}
	return nil
}

type CollectionDiagnostic struct {
	Options         ScanOptions `json:"options"`
	RowsScanned     int         `json:"rows_scanned"`
	EligibleUnique  int         `json:"eligible_unique"`
	DetailsSelected int         `json:"details_selected"`
	Unchanged       int         `json:"unchanged"`
	ReviewOnly      bool        `json:"review_only"`
}

func (o ScanOptions) worth(title, excerpt, source, rawURL string) bool {
	if o.Profile == "wide" {
		return WorthReviewing(title, excerpt, source, rawURL)
	}
	return WorthCollecting(title, excerpt, source, rawURL)
}

func (o ScanOptions) rejected(title, excerpt, rawURL string) string {
	if o.Profile == "wide" {
		return ReviewBlocked(title, excerpt, rawURL)
	}
	return ListingRejected(title, excerpt, rawURL)
}
