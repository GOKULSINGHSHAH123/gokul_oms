"""
db_helpers.py
-------------
Shared Redis + MongoDB connection manager for the firewall package.
Lazy-initialised, cached per process.
"""

import configparser
import logging
import os
import redis
import pymongo
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Load config.ini (same directory as this file)
# ─────────────────────────────────────────────────────────────

_cfg = configparser.ConfigParser()
_cfg_path = os.path.join(os.path.dirname(__file__), "config.ini")
_cfg.read(_cfg_path)

def _get(section: str, key: str, env_var: str | None = None) -> str:
    if env_var and os.getenv(env_var):
        return os.environ[env_var]
    return _cfg.get(section, key)

def _getint(section: str, key: str, env_var: str | None = None) -> int:
    if env_var and os.getenv(env_var):
        return int(os.environ[env_var])
    return _cfg.getint(section, key)

# ─────────────────────────────────────────────────────────────
# Configuration (env vars override config.ini)
# ─────────────────────────────────────────────────────────────

REDIS_DATA_HOST: str     = _get("dbParams",    "redisHost", "REDIS_DATA_HOST")
REDIS_DATA_PORT: int     = _getint("dbParams", "redisPort", "REDIS_DATA_PORT")
REDIS_DATA_PASSWORD: str = _get("dbParams",    "redisPass", "REDIS_DATA_PASSWORD")

REDIS_INFRA_HOST: str     = _get("infraParams",    "redisHost", "REDIS_INFRA_HOST")
REDIS_INFRA_PORT: int     = _getint("infraParams", "redisPort", "REDIS_INFRA_PORT")
REDIS_INFRA_PASSWORD: str = _get("infraParams",    "redisPass", "REDIS_INFRA_PASSWORD")

MONGO_URI: str = _get("dbParams", "MONGO_URI", "MONGO_URI")

# ─────────────────────────────────────────────────────────────
# Redis DB constants — sourced from config.ini
# ─────────────────────────────────────────────────────────────

class RedisDB:
    SYMBOL_MAPPING = _cfg.getint("dbParams",    "SYMBOL_MAPPING")
    QUALITY_STOCKS = _cfg.getint("dbParams",    "QUALITY_STOCKS")
    MARKET_DATA    = _cfg.getint("dbParams",    "MARKET_DATA")
    AUTH_TOKENS    = _cfg.getint("infraParams", "AUTH_TOKENS")
    MASTERFILE     = _cfg.getint("infraParams", "MASTERFILE")
    CONFIG_CACHE   = _cfg.getint("infraParams", "CONFIG_CACHE")
    LIVE_STREAMS   = _cfg.getint("infraParams", "LIVE_STREAMS")
    THROTTLER      = _cfg.getint("infraParams", "THROTTLER")


# ─────────────────────────────────────────────────────────────
# Connection pools
# ─────────────────────────────────────────────────────────────

_mongo_client: Optional[pymongo.MongoClient] = None

_pool_cache: dict = {}

def get_redis(db: int) -> redis.Redis:
    if db not in _pool_cache:
        # MASTERFILE is under infraParams — include it in infra routing
        infra_dbs = {RedisDB.AUTH_TOKENS, RedisDB.CONFIG_CACHE,
                     RedisDB.LIVE_STREAMS, RedisDB.THROTTLER,
                     RedisDB.MASTERFILE}

        host     = REDIS_INFRA_HOST if db in infra_dbs else REDIS_DATA_HOST
        port     = REDIS_INFRA_PORT if db in infra_dbs else REDIS_DATA_PORT
        password = REDIS_INFRA_PASSWORD if db in infra_dbs else REDIS_DATA_PASSWORD

        _pool_cache[db] = redis.ConnectionPool(
            host=host,
            port=port,
            password=password,
            db=db,
            decode_responses=True,
            max_connections=20,
        )
    return redis.Redis(connection_pool=_pool_cache[db])

# ─────────────────────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────────────────────

def get_mongo() -> pymongo.MongoClient:
    global _mongo_client

    if _mongo_client is None:
        _mongo_client = pymongo.MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
        )
        logger.info("MongoDB client initialised (%s)", MONGO_URI)

    return _mongo_client
