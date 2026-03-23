"""
Authentication and connection management module.

Provides factory functions for establishing connections to Redis and MongoDB databases
using both synchronous and asynchronous interfaces. Handles connection pooling, error
handling, and multi-database support.

Functions:
    get_redis_conn: Create synchronous Redis connection for any database index
    get_data_redis_conn: Create synchronous Redis connection to data Redis instance
    get_redis_conn_async: Create asynchronous Redis connection
    get_mongo_conn: Create synchronous MongoDB connection
    get_mongo_conn_async: Create asynchronous MongoDB connection
"""

from redis import Redis
from pymongo import MongoClient
from urllib.parse import quote_plus
from configparser import ConfigParser

# Import async libraries only when needed to maintain backwards compatibility
try:
    import redis.asyncio as aioredis

    import motor.motor_asyncio
    ASYNC_LIBRARIES_AVAILABLE = True
except ImportError:
    ASYNC_LIBRARIES_AVAILABLE = False


def get_redis_conn(config: ConfigParser, logger, db_index: int, purpose: str = None):
    """
    Get Redis connection for any database index.

    Args:
        config: ConfigParser object with Redis configuration
        logger: Logger instance
        db_index: Redis database index
        purpose: Optional description of service for logging

    Returns:
        Redis connection object
    """
    purpose_str = f" for {purpose}" if purpose else ""
    try:
        redisHost = config['infraParams']['redisHost']
        redisPort = config['infraParams']['redisPort']
        redisPassword = config['infraParams']['redisPass']

        logger.info(
            f"Connecting to Redis{purpose_str}: {redisHost}:{redisPort} DB:{db_index}")
        redisConn = Redis(host=redisHost, port=redisPort,
                          password=redisPassword, db=db_index)

        # Test Redis connection
        redisConn.ping()
        logger.info("✓ Redis connection established successfully")

        return redisConn
    except Exception as e:
        logger.error(
            f"Redis connection failed{purpose_str}: {str(e)}", exc_info=True)
        raise
    
def get_data_redis_conn(config: ConfigParser, logger, db_index: int, purpose: str = None):
    """
    Get Redis connection for any database index.

    Args:
        config: ConfigParser object with Redis configuration
        logger: Logger instance
        db_index: Redis database index
        purpose: Optional description of service for logging

    Returns:
        Redis connection object
    """
    purpose_str = f" for {purpose}" if purpose else ""
    try:
        redisHost = config['dbParams']['redisHost']
        redisPort = config['dbParams']['redisPort']
        redisPassword = config['dbParams']['redisPass']

        logger.info(
            f"Connecting to Redis{purpose_str}: {redisHost}:{redisPort} DB:{db_index}")
        redisConn = Redis(host=redisHost, port=redisPort,
                          password=redisPassword, db=db_index)

        # Test Redis connection
        redisConn.ping()
        logger.info("✓ Redis connection established successfully")

        return redisConn
    except Exception as e:
        logger.error(
            f"Redis connection failed{purpose_str}: {str(e)}", exc_info=True)
        raise


async def get_redis_conn_async(config: ConfigParser, logger, db_index: int, purpose: str = None):
    """
    Get async Redis connection for any database index.

    Args:
        config: ConfigParser object with Redis configuration
        logger: Logger instance
        db_index: Redis database index
        purpose: Optional description of service for logging

    Returns:
        Async Redis connection object (aioredis)
    """
    if not ASYNC_LIBRARIES_AVAILABLE:
        logger.error(
            "aioredis library not available. Please install required dependencies.")
        raise ImportError(
            "aioredis library not available. Run 'pip install aioredis'")

    purpose_str = f" for {purpose}" if purpose else ""
    try:
        redisHost = config['infraParams']['redisHost']
        redisPort = config['infraParams']['redisPort']
        redisPassword = config['infraParams']['redisPass']

        logger.info(
            f"Connecting to Redis (async){purpose_str}: {redisHost}:{redisPort} DB:{db_index}")

        # Construct Redis URI with proper DB
        redisPassword = quote_plus(redisPassword)
        if redisPassword:
            redis_uri = f"redis://:{redisPassword}@{redisHost}:{redisPort}/{db_index}"
        else:
            redis_uri = f"redis://{redisHost}:{redisPort}/{db_index}"

        # Create connection using modern aioredis v2.x API
        redis_conn = aioredis.from_url(
            redis_uri,
            max_connections=20,
            socket_timeout=10.0,
            socket_connect_timeout=10.0
        )

        # Test connection by executing a simple command
        await redis_conn.ping()
        logger.info(
            f"✓ Async Redis connection established successfully{purpose_str}")

        return redis_conn
    except Exception as e:
        logger.error(
            f"Async Redis connection failed{purpose_str}: {str(e)}", exc_info=True)
        raise


def get_mongo_conn(config: ConfigParser, logger):
    """
    Get MongoDB connection.

    Args:
        config: ConfigParser object with MongoDB configuration
        logger: Logger instance

    Returns:
        MongoDB connection object
    """
    try:
        mongoHost = config['infraParams']['mongoHost']
        mongoPort = int(config['infraParams']['mongoPort'])
        mongoUser = config['infraParams']['mongoUser']
        mongoPassword = config['infraParams']['mongoPass']

        logger.info(f"Connecting to MongoDB: {mongoHost}:{mongoPort}")
        mongoConn = MongoClient(host=mongoHost, port=mongoPort,
                                username=mongoUser, password=mongoPassword)
        mongoConn.admin.command('ping')
        logger.info("✓ MongoDB connection established successfully")

        return mongoConn
    except Exception as e:
        logger.error(f"MongoDB connection failed: {str(e)}", exc_info=True)
        raise


async def get_mongo_conn_async(config: ConfigParser, logger):
    """
    Get async MongoDB connection.

    Args:
        config: ConfigParser object with MongoDB configuration
        logger: Logger instance

    Returns:
        Async MongoDB connection object (motor.motor_asyncio.AsyncIOMotorClient)
    """
    if not ASYNC_LIBRARIES_AVAILABLE:
        logger.error(
            "motor library not available. Please install required dependencies.")
        raise ImportError(
            "motor library not available. Run 'pip install motor'")

    try:
        mongoHost = config['infraParams']['mongoHost']
        mongoPort = int(config['infraParams']['mongoPort'])
        mongoUser = config['infraParams']['mongoUser']
        mongoPassword = config['infraParams']['mongoPass']

        logger.info(f"Connecting to MongoDB (async): {mongoHost}:{mongoPort}")

        # Create the connection URI with credentials
        mongoPassword = quote_plus(mongoPassword)
        if mongoUser and mongoPassword:
            mongo_uri = f"mongodb://{mongoUser}:{mongoPassword}@{mongoHost}:{mongoPort}/"
        else:
            mongo_uri = f"mongodb://{mongoHost}:{mongoPort}/"

        # Create client with proper settings
        mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
            mongo_uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            maxPoolSize=50,
            retryWrites=True
        )

        # Test connection by executing a simple command
        await mongo_client.admin.command('ping')
        logger.info("✓ Async MongoDB connection established successfully")

        return mongo_client
    except Exception as e:
        logger.error(
            f"Async MongoDB connection failed: {str(e)}", exc_info=True)
        raise


# Reference of database indices for documentation purposes:
# DB 7: Mock Infrastructure
# DB 10: Token Infrastructure
# DB 12: Executor Infrastructure
# DB 13: Executor Client Operations
# DB 14: Order Modification
