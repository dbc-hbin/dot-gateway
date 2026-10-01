"""Explicit, credential-free proxy routing shared by REST and Gateway.

Only HTTP CONNECT proxies are supported. Routing decisions never retry through a
different path, disable TLS verification, or copy bot authentication to CONNECT.
"""
from __future__ import annotations

import http.client
import ipaddress
import os
import re
import ssl
from dataclasses import dataclass, field
from typing import Mapping
from urllib.parse import urlsplit
from urllib.request import proxy_bypass_environment

from .config import ConfigError


def _proxy_url(value: str) -> str:
    # urlsplit strips some controls; reject them before parsing instead.
    if not isinstance(value, str) or not value or any(ord(char) <= 32 or ord(char) >= 127 for char in value):
        raise ConfigError("proxy URL must be a nonempty ASCII URL without whitespace")
    try:
        parsed = urlsplit(value)
        if parsed.scheme != "http":
            raise ConfigError("only unauthenticated http CONNECT proxies are supported")
        if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
            raise ConfigError("proxy credentials are not supported; use a credential-free runtime proxy")
        if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ConfigError("proxy URL cannot contain a path, query, or fragment")
        host = parsed.hostname
        port = 80 if parsed.port is None else parsed.port
        if parsed.netloc.endswith(":"):
            raise ValueError
        if not host or not 1 <= port <= 65535 or "%" in host:
            raise ValueError
        if ":" in host:
            ipaddress.IPv6Address(host)
            authority = f"[{host}]:{port}"
        else:
            if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?", host):
                raise ValueError
            if ".." in host:
                raise ValueError
            authority = f"{host}:{port}"
        return "http://" + authority
    except ConfigError:
        raise
    except (TypeError, ValueError):
        raise ConfigError("invalid proxy URL or port") from None


@dataclass(frozen=True)
class ProxyConfig:
    url: str | None = field(default=None, repr=False)
    no_proxy: str = field(default="", repr=False)
    _host_patterns: str = field(default="", init=False, repr=False)
    _networks: tuple = field(default=(), init=False, repr=False)

    def __post_init__(self):
        if self.url is not None:
            object.__setattr__(self, "url", _proxy_url(self.url))
        if (not isinstance(self.no_proxy, str)
                or any(ord(char) < 32 or ord(char) >= 127 for char in self.no_proxy)):
            raise ConfigError("NO_PROXY must contain only ordinary ASCII host patterns")
        patterns = [pattern.strip() for pattern in self.no_proxy.split(",") if pattern.strip()]
        networks, hosts = [], []
        for pattern in patterns:
            if "/" in pattern:
                try:
                    networks.append(ipaddress.ip_network(pattern, strict=False))
                except ValueError:
                    raise ConfigError("NO_PROXY contains an invalid IP network") from None
            elif "*" in pattern and pattern != "*":
                raise ConfigError("NO_PROXY does not support partial wildcard patterns")
            else:
                hosts.append(pattern)
        object.__setattr__(self, "no_proxy", "*" if "*" in patterns else ",".join(patterns))
        object.__setattr__(self, "_host_patterns", "*" if "*" in hosts else ",".join(hosts))
        object.__setattr__(self, "_networks", tuple(networks))

    @classmethod
    def load(cls, env: Mapping[str, str] | None = None) -> "ProxyConfig":
        env = os.environ if env is None else env
        if "BRIDGE_HTTPS_PROXY" in env:
            raw = env["BRIDGE_HTTPS_PROXY"]
            if not raw:
                raise ConfigError("unset BRIDGE_HTTPS_PROXY instead of providing an empty override")
        else:
            # Lowercase takes precedence, including empty lower-case overrides.
            raw = env.get("https_proxy", env.get("HTTPS_PROXY", ""))
            if not raw:
                raw = env.get("all_proxy", env.get("ALL_PROXY", ""))
        bypass = env.get("BRIDGE_NO_PROXY", env.get("no_proxy", env.get("NO_PROXY", "")))
        return cls(raw if raw else None, bypass)

    def for_host(self, host: str, port: int = 443) -> str | None:
        if self.url is None or proxy_bypass_environment(f"{host}:{port}", {"no": self._host_patterns}):
            return None
        # CIDRs apply only to literal IP destinations. Never resolve a DNS name
        # to expand bypasses or reroute Discord around a proxy access decision.
        try:
            literal = host[1:-1] if host.startswith("[") and host.endswith("]") else host
            address = ipaddress.ip_address(literal)
        except ValueError:
            address = None
        if address is not None and any(address in network for network in self._networks):
            return None
        return self.url

    def discord_route(self) -> str | None:
        """discord.py exposes one shared proxy; reject mixed NO_PROXY routes."""
        rest = self.for_host("discord.com")
        if rest != self.for_host("gateway.discord.gg"):
            raise ConfigError("NO_PROXY must route Discord REST and Gateway consistently")
        return rest

    def validate_gateway(self, url: str, route: str | None) -> None:
        try:
            raw = str(url)
            if any(ord(char) <= 32 or ord(char) >= 127 for char in raw):
                raise ValueError
            target = urlsplit(raw)
            host = target.hostname or ""
            if (target.scheme != "wss" or target.username is not None or target.password is not None
                    or target.port not in {None, 443} or target.fragment
                    or target.netloc.endswith(":")
                    or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", host)
                    or ".." in host or not host.endswith(".discord.gg")):
                raise ValueError
        except (ValueError, TypeError):
            raise ConfigError("unexpected Discord Gateway endpoint") from None
        if self.for_host(host) != route:
            raise ConfigError("dynamic Gateway NO_PROXY route differs from configured Discord route")

    def https_connection(self, host: str, *, timeout: float):
        if host != "discord.com":
            raise ConfigError("unexpected Discord REST host")
        route = self.for_host(host)
        if route is None:
            return http.client.HTTPSConnection(host, timeout=timeout, context=ssl.create_default_context())
        proxy = urlsplit(route)
        # HTTPSConnection connects to the HTTP proxy in cleartext, sends CONNECT
        # without credentials, then verifies TLS against the tunnel's discord.com.
        connection = http.client.HTTPSConnection(proxy.hostname, proxy.port, timeout=timeout,
                                                  context=ssl.create_default_context())
        connection.set_tunnel(host, 443, headers={})
        return connection
