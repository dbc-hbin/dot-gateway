package collector

import (
	"regexp"
	"strings"
)

var relayCommercialDetail = regexp.MustCompile(`倍率|线路|充值|首充|到账|全模型`)

// Separate additions keep immutable Python-policy parity testable. These are
// lexical discovery clues, not claims that a product has an active promotion.
var reviewAITerms = []string{
	"人工智能", "大模型", "生成式", "생성형", "클로드", "제미나이", "챗지피티", "챗gpt", "지피티",
	"gpt", "antigravity", "notebooklm", "nano banana", "groq", "cerebras", "together ai", "fireworks ai",
	"replit", "lovable", "bolt.new", "trae", "siliconflow", "硅基流动", "智谱", "通义", "豆包", "千问",
	"kling", "可灵", "hailuo", "海螺", "chatglm", "stable diffusion",
}
var reviewDealTerms = []string{
	"credits", "offer", "offers", "sale", "grant", "grants", "sponsorship", "bonus", "price drop",
	"half price", "free access", "early access", "beta access", "launch offer", "무료사용", "체험권",
	"증정", "지급", "지원금", "반값", "특별가", "限时", "赠额", "赠金", "补贴", "降价", "立减", "打折",
}

func ReviewMatches(text string) (ai, strong, weak []string) {
	ai, strong, weak = Matches(text)
	ai = append(ai, matchList(text, reviewAITerms)...)
	weak = append(weak, matchList(text, reviewDealTerms)...)
	return ai, strong, weak
}

// Never admit shared credentials/accounts, account sales or proxy/relay pools
// just to increase recall. Login/trust-gated text is suppressed separately; its
// narrow public-title exception still requires concrete value in that title.
func ReviewBlocked(title, excerpt, rawURL string) string {
	blob := title + " " + excerpt
	switch {
	case search("KEY_SHARE_RE", blob):
		return "public_key_or_account_pool"
	case search("ACCOUNT_SELL_RE", blob) || search("NON_AI_AFF_RE", blob):
		return "affiliate_or_account_sale"
	case search("TRUST_GATE_RE", blob) && !search("CONCRETE_VALUE_RE", title):
		return "inaccessible_trust_level"
	case !FirstParty(rawURL) && search("RELAY_RE", blob):
		return "relay_or_self_promo"
	case !FirstParty(rawURL) && search("RELAY_COMMERCIAL_RE", blob):
		if !(search("OFFICIAL_RIDE_RE", blob) && !relayCommercialDetail.MatchString(blob)) {
			return "relay_or_self_promo"
		}
	}
	return ""
}

func WorthReviewing(title, excerpt, source, rawURL string) bool {
	blob := title + " " + excerpt
	ai, strong, weak := ReviewMatches(blob)
	if !specificAI(ai) && !MentionsVendor(blob) && !strings.HasPrefix(source, "dcinside") {
		return false
	}
	if ReviewBlocked(title, excerpt, rawURL) != "" {
		return false
	}
	return len(strong) > 0 || len(weak) > 0 || search("CONCRETE_BENEFIT_RE", blob)
}
