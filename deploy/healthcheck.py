#!/usr/bin/env python3
"""Health check: verify Redis connectivity for OMS services."""
import sys
import os
import configparser
import redis


def check():
    config_paths = [
        "../config.ini",
        "config.ini",
    ]

    cfg = configparser.ConfigParser()
    for p in config_paths:
        if os.path.exists(p):
            cfg.read(p)
            break
    else:
        # No config found — cannot verify, assume unhealthy
        sys.exit(1)

    for section in ("dbParams", "infraParams"):
        if not cfg.has_section(section):
            continue
        host = os.environ.get("REDIS_HOST", cfg.get(section, "redisHost", fallback="localhost"))
        port = int(os.environ.get("REDIS_PORT", cfg.get(section, "redisPort", fallback="6379")))
        pw = cfg.get(section, "redisPass", fallback="")
        try:
            r = redis.Redis(host=host, port=port, password=pw, socket_timeout=3)
            r.ping()
        except Exception:
            sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    check()
