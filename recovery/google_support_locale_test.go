package main

import (
	"path/filepath"
	"testing"
)

func TestMetadataGoogleOneKoreanLocaleStrictAllowlist(t *testing.T) {
	accepted := "https://support.google.com/googleone/answer/17422238?hl=ko"
	if !metadataURL(accepted) {
		t.Fatal("current official locale URL rejected")
	}
	for _, s := range []string{
		"http://support.google.com/googleone/answer/17422238?hl=ko",
		"https://support.google.com.evil.invalid/googleone/answer/17422238?hl=ko",
		"https://support.google.com:443/googleone/answer/17422238?hl=ko",
		"https://user@support.google.com/googleone/answer/17422238?hl=ko",
		"https://support.google.com/googleone/answer/17422238?hl=ko#",
		"https://support.google.com/googleone/answer/17422238?hl=ko#fragment",
		"https://support.google.com/googleone/answer/17422238?hl=ko&api_key=SECRET",
		"https://support.google.com/googleone/answer/17422238?hl=ko&token=SECRET",
		"https://support.google.com/googleone/answer/17422238?hl=ko&hl=en",
		"https://support.google.com/googleone/answer/17422238?hl=en",
		"https://support.google.com/googleone/answer/17422238?hl=secret",
		"https://support.google.com/googleone/answer/17422238?hl=k%6f",
		"https://support.google.com/googleone/answer/17422238?%68l=ko",
		"https://support.google.com/googleone/answer/17422238?hl=ko&",
		"https://support.google.com/googleone/answer/17422238?hl=ko;token=SECRET",
		"https://support.google.com/googleone/answer/%31%37%34%32%32%32%33%38?hl=ko",
		"https://support.google.com/googleone/answer/17422238/?hl=ko",
		"https://support.google.com/googleone/answer/../17422238?hl=ko",
		"https://support.google.com/googleone/answer/not-an-id?hl=ko",
		"https://support.google.com/googleone/answer/017422238?hl=ko",
		"https://support.google.com/accounts/answer/17422238?hl=ko",
	} {
		if metadataURL(s) {
			t.Fatal("unsafe or unreviewed locale URL accepted", s)
		}
	}
	// Existing unrelated restrictions stay intact.
	if !metadataURL("https://gall.dcinside.com/mgallery/board/view/?id=ai_utilize&no=123") {
		t.Fatal("existing exact DCInside exception regressed")
	}
	if metadataURL("https://example.invalid/path?hl=ko") {
		t.Fatal("generic locale query allowed")
	}
}

func TestGoogleLocaleOfferSnapshotRestoreKeepsDeliveryProof(t *testing.T) {
	state := stateFixture()
	want := operationsFixture(state)
	want.Reviewed.Offers[0].CanonicalURL = "https://support.google.com/googleone/answer/17422238?hl=ko"
	source := privateTemp(t)
	writeOperationsInputs(t, source, want)
	exported, err := snapshotOperations(source, state)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(privateTemp(t), "restored")
	if err = restoreOperations(exported, state, dest, -1); err != nil {
		t.Fatal(err)
	}
	restored, err := snapshotOperations(dest, state)
	if err != nil {
		t.Fatal(err)
	}
	if string(jsonBytes(restored.Reviewed)) != string(jsonBytes(want.Reviewed)) || restored.First != want.First {
		t.Fatal("offer URL, receipt proof or notification dedup changed")
	}
	restored.Reviewed.Offers[0].PayloadHash = string(make([]byte, 64))
	if operationsValid(restored, state) == nil {
		t.Fatal("locale exception weakened payload proof")
	}
}
