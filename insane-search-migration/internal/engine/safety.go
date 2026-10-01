package engine

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
)

type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}
type URLGuard struct {
	Resolver             Resolver
	AllowPrivateForTests bool
}
type SafetyError struct {
	Reason        string
	DNSUnresolved bool
}

func (e *SafetyError) Error() string { return "ssrf_blocked:" + e.Reason }

var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"), netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("192.168.0.0/16"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/4"), netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("::/128"), netip.MustParsePrefix("::1/128"), netip.MustParsePrefix("100::/64"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("fc00::/7"), netip.MustParsePrefix("fe80::/10"), netip.MustParsePrefix("ff00::/8"),
}

func BlockedIP(ip netip.Addr) bool {
	if !ip.IsValid() {
		return true
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return true
	}
	for _, p := range blockedPrefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}
func (g URLGuard) Resolve(ctx context.Context, raw string) (*url.URL, []netip.Addr, error) {
	u, e := url.Parse(raw)
	if e != nil {
		return nil, nil, &SafetyError{Reason: "parse_error"}
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, nil, &SafetyError{Reason: "scheme:" + u.Scheme}
	}
	if u.User != nil {
		return nil, nil, &SafetyError{Reason: "credentials_in_url_forbidden"}
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" {
		return nil, nil, &SafetyError{Reason: "no_host"}
	}
	if strings.Contains(host, "%") || strings.ContainsAny(host, "\\\x00\r\n") {
		return nil, nil, &SafetyError{Reason: "invalid_host"}
	}
	if ip, e := netip.ParseAddr(host); e == nil {
		if !g.AllowPrivateForTests && BlockedIP(ip) {
			return nil, nil, &SafetyError{Reason: "ip_blocked:" + host}
		}
		return u, []netip.Addr{ip.Unmap()}, nil
	}
	if !g.AllowPrivateForTests && (host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal")) {
		return nil, nil, &SafetyError{Reason: "internal_hostname"}
	}
	resolver := g.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ips, e := resolver.LookupNetIP(ctx, "ip", host)
	if e != nil {
		return nil, nil, &SafetyError{Reason: "dns_resolution_failed", DNSUnresolved: true}
	}
	if len(ips) == 0 {
		return nil, nil, &SafetyError{Reason: "dns_no_addresses", DNSUnresolved: true}
	}
	for _, ip := range ips {
		if !g.AllowPrivateForTests && BlockedIP(ip) {
			return nil, nil, &SafetyError{Reason: fmt.Sprintf("resolves_internal:%s", host)}
		}
	}
	return u, ips, nil
}
