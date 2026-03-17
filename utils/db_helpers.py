"""
db_helpers.py
-------------
Shared Redis + MongoDB connection manager.
Lazy-initialised, cached per process. All pipeline stages import from here.

Redis instances:
  - data_redis : DBs 0, 2, 9, 11
  - live_redis : DBs 10, 12, 14

MongoDB:
  - Single MongoClient, databases accessed by name via schema.py
"""

import logging
import os
import redis
import pymongo
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Configuration (override via environment variables)
# ─────────────────────────────────────────────────────────────

REDIS_DATA_HOST: str = os.getenv("REDIS_DATA_HOST", "101.53.133.122")
REDIS_DATA_PORT: int = int(os.getenv("REDIS_DATA_PORT", 6379))
REDIS_DATA_PASSWORD: str = os.getenv("REDIS_DATA_PASSWORD", "Algo_infra_stock_options")

REDIS_LIVE_HOST: str = os.getenv("REDIS_LIVE_HOST", "101.53.133.122")
REDIS_LIVE_PORT: int = int(os.getenv("REDIS_LIVE_PORT", 6379))
REDIS_LIVE_PASSWORD: str = os.getenv("REDIS_LIVE_PASSWORD", "Algo_infra_stock_options")

MONGO_URI: str = os.getenv(
    "MONGO_URI",
    "mongodb://mudraksh:NewStrongPassword@13.235.28.235:27017/"
)

# ─────────────────────────────────────────────────────────────
# Redis DB constants — single source of truth
# ─────────────────────────────────────────────────────────────

class RedisDB:
    SYMBOL_MAPPING = 0
    QUALITY_STOCKS = 2
    MARKET_DATA = 9
    AUTH_TOKENS = 10
    MASTERFILE = 11
    CONFIG_CACHE = 12
    LIVE_STREAMS = 12
    THROTTLER =14
    

# ─────────────────────────────────────────────────────────────
# Connection pools
# ─────────────────────────────────────────────────────────────

_data_pool: Optional[redis.ConnectionPool] = None
_live_pool: Optional[redis.ConnectionPool] = None
_mongo_client: Optional[pymongo.MongoClient] = None


def _get_data_pool() -> redis.ConnectionPool:
    global _data_pool

    if _data_pool is None:
        _data_pool = redis.ConnectionPool(
            host=REDIS_DATA_HOST,
            port=REDIS_DATA_PORT,
            password=REDIS_DATA_PASSWORD,
            decode_responses=True,
            max_connections=20,
        )

        logger.info(
            "Redis DATA pool initialised (%s:%s)",
            REDIS_DATA_HOST,
            REDIS_DATA_PORT
        )

    return _data_pool


def _get_live_pool() -> redis.ConnectionPool:
    global _live_pool

    if _live_pool is None:
        _live_pool = redis.ConnectionPool(
            host=REDIS_LIVE_HOST,
            port=REDIS_LIVE_PORT,
            password=REDIS_LIVE_PASSWORD,
            decode_responses=True,
            max_connections=20,
        )

        logger.info(
            "Redis LIVE pool initialised (%s:%s)",
            REDIS_LIVE_HOST,
            REDIS_LIVE_PORT
        )

    return _live_pool


_pool_cache: dict = {}

def get_redis(db: int) -> redis.Redis:
    if db not in _pool_cache:
        live_dbs = {RedisDB.AUTH_TOKENS, RedisDB.CONFIG_CACHE, 
                    RedisDB.LIVE_STREAMS, RedisDB.THROTTLER}
        
        host     = REDIS_LIVE_HOST if db in live_dbs else REDIS_DATA_HOST
        port     = REDIS_LIVE_PORT if db in live_dbs else REDIS_DATA_PORT
        password = REDIS_LIVE_PASSWORD if db in live_dbs else REDIS_DATA_PASSWORD

        _pool_cache[db] = redis.ConnectionPool(
            host=host,
            port=port,
            password=password,
            db=db,                  # ← db goes HERE, on the pool
            decode_responses=True,
            max_connections=20,
        )
    return redis.Redis(connection_pool=_pool_cache[db])  # ← no db= here
# ─────────────────────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────────────────────

def get_mongo() -> pymongo.MongoClient:
    """
    Return shared MongoClient (lazy singleton)
    """

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


# ─────────────────────────────────────────────────────────────
# Graceful shutdown
# ─────────────────────────────────────────────────────────────

def close_connections() -> None:
    """
    Close all DB connections.
    """

    global _data_pool, _live_pool, _mongo_client

    if _data_pool:
        _data_pool.disconnect()
        _data_pool = None

    if _live_pool:
        _live_pool.disconnect()
        _live_pool = None

    if _mongo_client:
        _mongo_client.close()
        _mongo_client = None

    logger.info("All DB connections closed.")