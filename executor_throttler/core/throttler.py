"""
Order throttling and rate limiting module.

Implements client-level order throttling with sliding-window rate limiting to enforce
maximum orders per second constraints. Provides asynchronous HTTP request handling for
order placement, modification, and cancellation with response mapping to order tracking.

Includes limit price calculation based on LTP (Last Traded Price) and instrument-specific
rules (futures vs options), with support for priority queue consumption.

Classes:
    SlidingWindowRateLimiter: Token-bucket rate limiter with sliding time windows
    ClientOrderThrottler: Main process for throttling and executing orders per client
    
Functions:
    get_client_exec_list: Discover all active clients with pending orders
"""

import json
import asyncio
import random
import aiohttp
import redis.asyncio as aioredis

import motor.motor_asyncio
from redis import Redis
from typing import Any, Dict, Optional
from datetime import datetime, date
from multiprocessing import Process
from configparser import ConfigParser
from time import time, sleep, monotonic
from collections import deque
import logging


from core.logger import setup_logging
from core.utils import parse_redis_dict_values
from core.auth import (
    get_redis_conn,
    get_mongo_conn,
    get_redis_conn_async,
    get_data_redis_conn,
    get_mongo_conn_async
)


class SlidingWindowRateLimiter:
    """
    Sliding window rate limiter that ensures no more than max_requests 
    in any window_seconds time period from any point of reference.
    
    Unlike discrete token buckets, this implementation tracks actual request
    timestamps and enforces limits over any sliding time window, providing
    strict rate limiting guarantees for trading systems.
    """

    def __init__(self, capacity: int, refill_interval: float = 1.0, randomize: bool = False, randomize_interval:float = 1.0,min_capacity: int = 1, logger=logging.getLogger()):
        """
        Initialize the sliding window rate limiter.

        Creates a token bucket that enforces strict rate limits over any sliding
        time window. Optionally randomizes capacity within a range at intervals.

        Args:
            capacity: Maximum number of tokens (requests) allowed in the window
            refill_interval: Time window in seconds for rate limit enforcement (default: 1.0)
            randomize: Whether to randomly vary capacity within bounds (default: False)
            randomize_interval: Time between randomization events in seconds (default: 1.0)
            min_capacity: Minimum capacity when randomizing (default: 1)
            logger: Logger instance for debug output (default: root logger)
        
        Raises:
            ValueError: If capacity or refill_interval are not positive
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        if refill_interval <= 0:
            raise ValueError("Refill interval must be positive")
        
        self.logger = logger
        self.capacity = capacity
        self.window_seconds = refill_interval
        self.timestamps = deque()
        
        self.randomize = randomize
        self.last_randomized = monotonic()
        self.randomize_interval = randomize_interval
        self.min_capacity = min_capacity
        self.max_capacity = self.capacity

    def _randomize_capacity(self) -> None:
        """
        Randomize capacity if interval has passed since last randomization.

        Updates capacity to a random value between min_capacity and max_capacity,
        enabling dynamic rate limit variation for anti-pattern detection.
        """
        current_time = monotonic()
        if current_time - self.last_randomized >= self.randomize_interval:
            self.capacity = random.randint(self.min_capacity, self.max_capacity)
            self.last_randomized = monotonic()
            self.logger.debug(f"Randomized capacity to {self.capacity}")
    
    def _cleanup_old_timestamps(self) -> None:
        """
        Remove timestamps that fall outside the current sliding window.

        Executes capacity randomization if enabled, then removes stale timestamps
        to keep the deque efficient.
        """
        if self.randomize:
            self._randomize_capacity()
        current_time = monotonic()
        
        # Remove timestamps outside the sliding window
        while self.timestamps and current_time - self.timestamps[0] >= self.window_seconds:
            self.timestamps.popleft()
    
    def try_consume(self, tokens: int = 1) -> bool:
        """
        Attempt to consume tokens from the sliding window rate limiter.

        Checks if consuming the requested tokens would exceed the capacity within
        the current time window. If allowed, records the request timestamp.

        Args:
            tokens: Number of tokens to consume (default: 1)

        Returns:
            bool: True if tokens consumed successfully, False if rate limit exceeded
        """
        self._cleanup_old_timestamps()
        
        # Check if adding this request would exceed the capacity
        if len(self.timestamps) + tokens <= self.capacity:
            current_time = monotonic()
            # Add timestamps for each token consumed
            for _ in range(tokens):
                self.timestamps.append(current_time)
            return True
        return False
    
    def available_tokens(self) -> int:
        """
        Get current number of available tokens in the sliding window.

        Returns the number of additional requests that can be made before hitting
        the rate limit, accounting for timestamps that have fallen outside the window.

        Returns:
            int: Number of requests available (0 if at capacity)
        """
        self._cleanup_old_timestamps()
        return max(0, self.capacity - len(self.timestamps))
    
    def time_until_next_refill(self) -> float:
        """
        Calculate seconds until the next token becomes available.

        Computes how long to wait before the oldest request timestamp expires
        from the sliding window, making a new token available.

        Returns:
            float: Seconds to wait (0.0 if tokens are available now)
        """
        self._cleanup_old_timestamps()
        
        if len(self.timestamps) < self.capacity:
            return 0.0
        
        # Time until the oldest timestamp falls out of the window
        current_time = monotonic()
        oldest_timestamp = self.timestamps[0]
        return max(0.0, self.window_seconds - (current_time - oldest_timestamp))


def get_client_exec_list(redis_conn_client_ops_queue: Redis, config: ConfigParser):
    """
    Discover all active clients with pending orders in high or low priority queues.

    Performs a single efficient Redis SCAN operation to find all client queue keys,
    then extracts unique client IDs. Returns a deterministic sorted list for
    consistent processing order.

    Args:
        redis_conn_client_ops_queue: Redis connection to client operations database
        config: Configuration parser (for potential future use)

    Returns:
        List[Dict]: Sorted list of dicts with 'exec_id' and 'client_id' for each active client
    """

    valid_priorities = {"high", "low"}
    client_ids = set()
    cursor = 0
    while True:
        cursor, keys = redis_conn_client_ops_queue.scan(
            cursor=cursor, match='client:*:*', type='list')
        for raw_key in keys:
            key = raw_key.decode('utf-8') if isinstance(raw_key,
                                                        (bytes, bytearray)) else raw_key
            parts = key.split(':')
            if len(parts) == 3 and parts[0] == 'client' and parts[2] in valid_priorities:
                client_ids.add(parts[1])
        if cursor == 0:
            break

    client_order_throttler_processes_list = [
        {
            'exec_id': f"ClientOrderThrottler-{client_id}",
            'client_id': client_id
        }
        for client_id in sorted(client_ids)
    ]

    return client_order_throttler_processes_list


class ClientOrderThrottler(Process):

    def __init__(self, config: ConfigParser, exec_id: str, client_id: str, limit_ops: bool = True):
        """
        Initialize a client order throttler process.

        Sets up rate limiting, initializes connections to Redis and MongoDB, and
        prepares for asynchronous order processing. The actual async clients are
        initialized in async_init() when the process starts.

        Args:
            config: Configuration parser with all infrastructure and behavior settings
            exec_id: Unique executor identifier for process tracking
            client_id: Client identifier for queue and logging purposes
            limit_ops: Whether to enforce rate limiting (default: True for most clients)
        """
        super().__init__()
        self.LOG_FILE_NAME = f'client_{client_id}'
        self.config = config
        
        self.exec_id = exec_id
        self.client_id = client_id
        self.limit_ops = limit_ops

        self.logger_name = f'[ExecID:{exec_id}]'
        self.logger = setup_logging(
            logger_name=self.logger_name, log_dir="logs/client_order_throttler", file_name=self.LOG_FILE_NAME)
        
        self.logger.info(f"ClientOrderThrottler initialized for client_id: {client_id} with limit_ops={limit_ops}")
        
        self.error_stream = self.config.get('throttler', 'error_stream')
        self.route = self.config.get('throttler','order_route')
        # Will be initialized in async_init
        self.redis_client_ops_async = None
        self.redis_order_mod_async = None
        self.mongo_client_async = None
        self.mongo_db_async = None
        self.http_session = None

        # Rate limiting configuration
        self.max_orders_per_second = int(config.get(
            'throttler', 'max_orders_per_second'))

        # Event loop and task management
        self.loop = None
        self._running = False
        self._pending_tasks = set()
        
        # Initialize sliding window rate limiter
        self.token_bucket = SlidingWindowRateLimiter(
            capacity=self.max_orders_per_second,
            refill_interval=1.0,
            randomize=self.config.get('throttler', 'randomize_per_second').lower() == 'true',
            randomize_interval=float(self.config.get('throttler', 'randomize_interval')),
            min_capacity=int(self.config.get('throttler', 'min_orders_per_second')),
            logger=self.logger
        )

        self.logger.info(
            f"{self.client_id} Client Order Throttler initialized with sliding window "
            f"rate limit: {self.max_orders_per_second} orders/second")

    async def async_init(self):
        """
        Asynchronously initialize all connections and clients.

        Establishes HTTP session and async connections to Redis (multiple DBs) and
        MongoDB. These connections are used throughout the order execution lifecycle.
        Must be called before any async operations in run().

        Raises:
            Exception: If any connection fails; automatically cleans up partial connections
        """
        try:
            # Create aiohttp session for HTTP requests
            self.http_session = aiohttp.ClientSession()

            # Initialize async Redis connections using the auth module functions
            self.redis_mock_async = await get_redis_conn_async(
                self.config, self.logger, db_index=7, purpose="Mock Infrastructure"
            )
            
            self.redis_ping_conn_async = await get_redis_conn_async(
                self.config, self.logger, db_index=9, purpose="Ping Connection"
            )

            self.redis_token_async = await get_redis_conn_async(
                self.config, self.logger, db_index=10, purpose="Token Infrastructure"
            )
            
            self.redis_executor_infra_async = await get_redis_conn_async(
                self.config, self.logger, db_index=12, purpose="Executor Infrastructure"
            )
            
            self.redis_client_ops_async = await get_redis_conn_async(
                self.config, self.logger, db_index=13, purpose="Client Operations"
            )

            self.redis_order_mod_async = await get_redis_conn_async(
                self.config, self.logger, db_index=14, purpose="Order Modification"
            )
            
            # Initialize Data Redis Connection
            self.redis_data_conn = get_data_redis_conn(
                self.config, self.logger, db_index=0, purpose="Symbol Data Feed"
            )
            
            self.redis_master_data_conn = get_data_redis_conn(
                self.config, self.logger, db_index=11, purpose="Master Data Feed"
            )

            # Initialize async MongoDB connection using the auth module function
            self.mongo_client_async = await get_mongo_conn_async(
                self.config, self.logger
            )

            # Get the appropriate collection
            self.mongo_db_async = self.mongo_client_async['Info']["algo_signals"]

            self.logger.info("All async clients initialized successfully")

        except Exception as e:
            self.logger.error(
                f"Error in async initialization: {str(e)}", exc_info=True)
            # Clean up any connections that were created before the error
            await self.async_shutdown()
            raise

    def getLTP(self, algo_name, instrument_id):
        """
        Fetch the Last Traded Price (LTP) for an instrument from Redis.

        Args:
            algo_name: Name of algorithm for debug logging
            instrument_id: Exchange instrument ID to fetch price for

        Returns:
            float: LTP value, or None if not found in Redis
        """
        ltp_bytes = self.redis_data_conn.get(instrument_id)
        self.logger.debug(f"LTP for {algo_name} on {instrument_id}: {ltp_bytes}")
        if ltp_bytes is None:
            return None

        ltp = float(ltp_bytes.decode('utf-8'))
        return ltp
    
    def calculateLimitPrice(self, algo_signal, algo_name, ltp, instrument_id):
        """
        Calculate the limit price based on LTP and instrument-specific rules.

        For non-options (futures/equities): Uses percentage-based thresholds above/below LTP.
        For options: Uses premium thresholds and fixed amounts.
        Price is rounded to the nearest tick size.

        Args:
            algo_signal: Order signal dict containing orderSide and limitPrice
            algo_name: Algorithm name for logging
            ltp: Last Traded Price (float)
            instrument_id: Exchange instrument ID for master data

        Returns:
            float: Calculated limit price rounded to tick size

        Raises:
            Exception: If instrument master data not found in Redis
        """
        order_side = algo_signal.get('orderSide')
        
        masterF = self.redis_master_data_conn.get(instrument_id)
        if masterF is None:
            raise Exception(
                f"Master file not found for {instrument_id}")

        masterF = json.loads(masterF.decode('utf-8'))
        instrument_type = int(masterF['InstrumentType'])
        tick_size = float(masterF['TickSize'])
        
        # Instrument Type IDs:
        # 1 - futures
        # 8 - equity
        # 2 - options
        if (instrument_type != 2):
            not_option_limit_price_threshold = self.config.getfloat('LimitOrder', 'notOptionLimitPriceThreshold')
            not_option_threshold_above_percent = self.config.getfloat('LimitOrder', 'notOptionThresholdAbovePercent')
            not_option_threshold_below_percent = self.config.getfloat('LimitOrder', 'notOptionThresholdBelowPercent')

            # Calculate limit price based on LTP value and order side
            if ltp > not_option_limit_price_threshold:
                if order_side == 'BUY':
                    limit_price = round(ltp * (1 + not_option_threshold_above_percent), 1)
                else:  # SELL order
                    limit_price = round(ltp * (1 - not_option_threshold_above_percent), 1)
            else:
                if order_side == 'BUY':
                    limit_price = round(ltp * (1 + not_option_threshold_below_percent), 1)
                else:  # SELL order
                    limit_price = round(ltp * (1 - not_option_threshold_below_percent), 1)

        else:
            small_premium_threshold = self.config.getfloat(
                'LimitOrder', 'optionSmallPremiumThreshold')
            extra_percent = self.config.getfloat('LimitOrder', 'extraPercent')
            extra_amount = self.config.getfloat('LimitOrder', 'extraAmount')
            min_limit_price = self.config.getfloat(
                'LimitOrder', 'minimumLimitPrice')

            # Calculate limit price based on LTP value and order side
            if ltp > small_premium_threshold:
                if order_side == 'BUY':
                    limit_price = round(ltp * (1 + extra_percent), 1)
                else:  # SELL order
                    limit_price = round(ltp * (1 - extra_percent), 1)
            else:
                if order_side == 'BUY':
                    limit_price = round(ltp + extra_amount, 1)
                else:  # SELL order
                    limit_price = round(
                        max(min_limit_price, ltp - extra_amount), 1)

        limit_price = round(limit_price / tick_size) * tick_size
        self.logger.debug(f"Calculated limit price for {algo_name} on {instrument_id}: {algo_signal['limitPrice']} -> {limit_price} based on LTP: {ltp}")
        return limit_price

    def parse_order(self, order: Dict[str, Any], update_price: str) -> Dict[str, Any]:
        """
        Parse and prepare order for API submission.

        If update_price is enabled, fetches current LTP and recalculates limit price.
        Returns a normalized order dict with all required fields for the trading API.

        Args:
            order: Order dict from Redis with all trading details
            update_price: String 'true'/'false' to enable LTP-based price updates

        Returns:
            Dict: Order dict formatted for API request
        """
        if update_price.lower() == 'true':
            ltp = self.getLTP(order['algoName'], order['exchangeInstrumentID'])
            limitPrice = self.calculateLimitPrice(order, order['algoName'], ltp, order['exchangeInstrumentID']) if ltp else order['limitPrice']
        else:
            limitPrice = order['limitPrice']
            
        return {
            'clientID': order['clientID'],
            'exchangeSegment': order['exchangeSegment'],
            'exchangeInstrumentID': order['exchangeInstrumentID'],
            'productType': order['productType'],
            'orderType': 'LIMIT' if order['orderType'].lower() == 'buythensell' else order['orderType'],
            'orderSide': order['orderSide'],
            'timeInForce': order['timeInForce'],
            'disclosedQuantity': order['disclosedQuantity'],
            'orderQuantity': order['orderQuantity'],
            'limitPrice': limitPrice,
            'stopPrice': order['stopPrice'],
            'orderUniqueIdentifier': order['orderUniqueIdentifier'],
        }

    def get_signal_feed(self, order: Dict[str, Any], response_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construct a signal feed from the executed order and API response.

        Creates a complete record of the order execution including timing, pricing,
        and confirmation details for downstream processing and analytics.

        Args:
            order: Original order dict submitted to the API
            response_data: API response containing confirmed AppOrderID

        Returns:
            Dict: Complete signal feed with execution metadata
        """
        return {
            "exchangeSegment": order['exchangeSegment'],
            "exchangeInstrumentID": order['exchangeInstrumentID'],
            "productType": order['productType'],
            "orderType": 'LIMIT' if order['orderType'].lower() == 'buythensell' else order['orderType'],
            "orderSide": order['orderSide'],
            "timeInForce": order['timeInForce'],
            "disclosedQuantity": order['disclosedQuantity'],
            "orderQuantity": order['orderQuantity'],
            "limitPrice": order['limitPrice'],
            "stopPrice": order['stopPrice'],
            "orderUniqueIdentifier": order['orderUniqueIdentifier'],
            "clientID": order['clientID'],
            "algoName": order['algoName'],
            "orderSentTime": order['order_sent_time'],
            "responseFlag": False,
            "leaves_quantity": order['orderQuantity'],
            "timestamp": str(datetime.now().strftime("%H:%M:%S")),
            "symbol": order['symbol'],
            "initial_timestamp": order['order_sent_time'],
            "order_confirmed_timestamp": time(),
            "initial_price": order['limitPrice'],
            "actual_price": order['actualPrice'],
            "mod_status": "Placed",
            "appOrderId": response_data['result']['AppOrderID'],
            "algoPrice": order.get('algoPrice', order['actualPrice']),
            "algoTime": order.get('algoTime', order['order_sent_time']),
            "source": "executor"
        }

    async def map_order_id_to_app_id(self, order_unique_identifier, signal_feed: Dict[str, Any]):
        """
        Map order unique identifier to app order ID in Redis and MongoDB.

        Stores the bidirectional mapping and inserts order record into MongoDB
        for downstream tracking and analytics.

        Args:
            order_unique_identifier: Unique order identifier from upstream system
            signal_feed: Complete order signal data for MongoDB insertion
        """
        app_order_id = signal_feed.get('appOrderId')

        # Execute Redis and MongoDB operations asynchronously and concurrently
        await asyncio.gather(
            self.redis_order_mod_async.hset(
                'order_status:idmap', order_unique_identifier, app_order_id),
            self.mongo_db_async.insert_one(signal_feed)
        )

        self.logger.debug(
            f"Mapped order {order_unique_identifier} to app ID {app_order_id}")

    def get_endpoint_with_suffix(self, tokenMap):
        endpoint = tokenMap.get(b'endpoint', b'').decode()
        suffix = tokenMap.get(b'suffix', b'').decode()
        url = f"{endpoint}/{suffix}/{self.route}"
        return url

    def get_proxy(self, tokenMap: dict, client_id: str) -> Optional[str]:
        """
        Build a proxy URL from tokenMap if a gateway is configured.

        Checks the 'gateway' field in the client's Redis tokenMap (stored as ip:port).
        If a gateway is set, returns a proxy URL with client_id as credentials.
        Returns None for clients that are not mapped to any proxy.

        Args:
            tokenMap: Redis hash containing token/credentials data
            client_id: Client identifier used as proxy username and password

        Returns:
            Proxy URL string (http://client_id:client_id@ip:port) if gateway is set, else None
        """
        gateway = tokenMap.get(b'gateway', b'').decode().strip()

        return gateway
        




    async def _push_error(
        self,
        error_type: str,
        order_data: dict,
        reason: str,
        stage: str,
        client_id=None,
    ) -> None:
        payload = {
            "error_type": error_type,
            "stage":      stage,
            "reason":     reason,
            "signal":     json.dumps(order_data),
            "client_id":  str(client_id) if client_id else str(self.client_id),
            "timestamp":  int(time() * 1000),
        }
        await self.redis_executor_infra_async.xadd(self.error_stream, payload)
        self.logger.warning(f"Error -> {self.error_stream}  stage={stage}  reason={reason}")

    async def send_order_as_http_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sends an HTTP request for the given order using aiohttp.

        Args:
            order: Dictionary containing order details.

        Returns:
            The processed signal feed or None if there was an error.
        """

        try:
            method = request.get('method')
            url = request.get('url')

            # Handle headers that might be string or already parsed dictionary
            headers = request.get('headers')
            if isinstance(headers, str):
                headers = json.loads(headers)

            # Handle body that might be string or already parsed dictionary
            order = request.get('body')
            if isinstance(order, str):
                order = json.loads(order)
                
            update_price = request.get('update_price', 'True')

            try:
               
                broker = order.get('broker')
                client_id = order.get('clientID')
                broker = str.lower(broker)

                tokenMap = await self.redis_token_async.hgetall(f'token:{broker}:{client_id}')

                
            except Exception as e:
                self.logger.warning("Failed to load tokenMap from Redis")
                print('Failed to load tokenMap from Redis' , e)
                tokenMap = {}

            print(tokenMap)
            if tokenMap:
                url = self.get_endpoint_with_suffix(tokenMap)

            proxy = self.get_proxy(tokenMap, client_id)



            # self.logger.debug(order)
            final_order = self.parse_order(order, update_price)


            if tokenMap.get(b'type', b'').lower() == b'pro':
                final_order['clientID'] = '*****'

            self.logger.debug(
                f'Sending {method} HTTP request on {url} with body: {final_order} and headers: {headers}'
                + (f' via proxy: {proxy}' if proxy else ''))

            # Reuse the shared HTTP session
            async with self.http_session.request(
                method=method,
                url=url,
                json=final_order,
                headers=headers,
                proxy=proxy,
            ) as response:
                order_status = response.status

                # Only load response text if needed (non-success)
                if response.status != 200:
                    order_text = await response.text()
                    self.logger.error(
                        f"Order failed: {order_status} - {order_text} | {order}")

                    await self.redis_order_mod_async.hset('order_status:idmap', order['orderUniqueIdentifier'], '0')
                    await self.redis_order_mod_async.hset('order_status:filled', order['orderUniqueIdentifier'], '1')

                    await self._push_error(
                        "order_data", order,
                        order_text, "broker_send", order.get('clientID'),
                    )
                    return False

                # Parse JSON response
                response_data = await response.json()

                # Log success at debug level to reduce log volume
                self.logger.info(
                    f"Order sent successfully: orderUniqueIdentifier: {order['orderUniqueIdentifier']}")
                self.logger.debug(f"Response: {response_data}")

                # Create signal feed
                signal_feed = self.get_signal_feed(order, response_data)

                # Map order ID to app ID asynchronously
                await self.map_order_id_to_app_id(order['orderUniqueIdentifier'], signal_feed)

                return True

        except asyncio.TimeoutError:
            self.logger.error(
                f"Timeout sending order {order.get('orderUniqueIdentifier', 'unknown')}")
            return False
        except Exception as e:
            self.logger.exception(
                f"Error sending request for order {order.get('orderUniqueIdentifier', 'unknown')}: {str(e)}",
                exc_info=True)
            return False

    async def send_modify_order_as_http_request(self, request: Dict[str, Any]) -> bool:
        try:
            method = request.get('method')
            url = request.get('url')

            # Handle headers that might be string or already parsed dictionary
            headers = request.get('headers')
            if isinstance(headers, str):
                headers = json.loads(headers)

            # Handle body that might be string or already parsed dictionary
            order = request.get('body')
            if isinstance(order, str):
                order = json.loads(order)

            update_price = request.get('update_price', 'True')
            
            orderUniqueIdentifier = request.get('orderUniqueIdentifier')
            # if isinstance(orderUniqueIdentifier, str):
            #     orderUniqueIdentifier = json.loads(orderUniqueIdentifier)

            retryCount = int(request.get('retryCount',0))
            # if isinstance(retryCount, str):
            #     retryCount = json.loads(retryCount)

            try:
               
                broker = order.get('broker')
                client_id = order.get('clientID')
                tokenMap = await self.redis_token_async.hgetall(f'token:{broker}:{client_id}')
            except:
                self.logger.warning("Failed to load tokenMap from Redis")
                print('Failed to load tokenMap from Redis')
                tokenMap = {}

            if tokenMap:
                url = self.get_endpoint_with_suffix(tokenMap)

            proxy = self.get_proxy(tokenMap, client_id)

            # self.logger.debug(order)
            # final_order = self.parse_order(order)

            if update_price.lower() == 'true':
                order_doc = await self.mongo_db_async.find_one({'appOrderId':int(order['appOrderID'])})
                ltp = self.getLTP(order_doc['algoName'], order_doc['exchangeInstrumentID'])
                if ltp:
                    order['modifiedLimitPrice'] = self.calculateLimitPrice(order_doc, order_doc['algoName'], ltp, order_doc['exchangeInstrumentID'])

            if tokenMap.get(b'type', b'').lower() == b'pro':
                order['clientID'] = '*****'

            self.logger.debug(request)
            self.logger.debug(
                f'Sending {method} HTTP request on {url} with body: {order} and headers: {headers} with orderUniqueIdentifier: {orderUniqueIdentifier} and retryCount: {retryCount}'
                + (f' via proxy: {proxy}' if proxy else ''))

            # Reuse the shared HTTP session
            async with self.http_session.request(
                method=method,
                url=url,
                json=order,
                headers=headers,
                proxy=proxy,
            ) as response:
                # Parse JSON response
                response_data = await response.json()
                order_status = response.status

                # Only load response text if needed (non-success)
                if response.status != 200:
                    order_text = await response.text()

                    if ('is not found in OpenOrder List' in order_text) and (order_text is not None):
                        self.logger.info(f"Order is already filled")
                        await self.redis_order_mod_async.hset('order_status:filled', orderUniqueIdentifier, '1')
                        
                        return True

                    self.logger.error(
                        f"Order failed : {order_status} - {order_text} | {order}")

                    await self._push_error(
                        "order_data", order,
                        order_text, "broker_modify", order.get('clientID'),
                    )
                    return False

                # Log success at debug level to reduce log volume
                self.logger.info(
                    f"Order sent successfully: AppOrderId: {order['appOrderID']}")
                self.logger.debug(f"Response: {response_data}")

                self.mongo_db_async.update_one(
                    {'appOrderId': int(order['appOrderID'])},
                    {'$set': {
                        'limitPrice': order['modifiedLimitPrice'],
                        'orderSentTime': time(),
                        'mod_status': f"modified_{retryCount}"
                    }}
                )

                return True

        except asyncio.TimeoutError:
            self.logger.error(
                f"Timeout sending order {order.get('appOrderID', 'unknown')}")
            return False
        except Exception as e:
            self.logger.exception(
                f"Error sending request for order {order.get('appOrderID', 'unknown')}: {str(e)}",
                exc_info=True)
            return False

    async def send_cancel_order_as_http_request(self, request: Dict[str, Any]) -> bool:
        try:
            method = request.get('method')
            url = request.get('url')

            # Handle headers that might be string or already parsed dictionary
            headers = request.get('headers')
            if isinstance(headers, str):
                headers = json.loads(headers)

            # Handle body that might be string or already parsed dictionary
            order = request.get('body')
            if isinstance(order, str):
                order = json.loads(order)

            appOrderID = request.get('appOrderID')
            # if isinstance(appOrderID, str):
            #     appOrderID = json.loads(appOrderID)

            try:
               
                broker = order.get('broker')
                client_id = order.get('clientID')
                tokenMap = await self.redis_token_async.hgetall(f'token:{broker}:{client_id}')
            except:
                self.logger.warning("Failed to load tokenMap from Redis")
                print('Failed to load tokenMap from Redis')
                tokenMap = {}

            if tokenMap:
                url = self.get_endpoint_with_suffix(tokenMap)
            # self.logger.debug(order)
            # final_order = self.parse_order(order)

            proxy = self.get_proxy(tokenMap, client_id)

            if tokenMap.get(b'type', b'').lower() == b'pro':
                order['clientID'] = '*****'

            self.logger.debug(
                f'Sending {method} HTTP request on {url} with body: {order} and headers: {headers} with appOrderID: {appOrderID}'
                + (f' via proxy: {proxy}' if proxy else ''))

            # Reuse the shared HTTP session
            async with self.http_session.request(
                method=method,
                url=url,
                params=order,
                headers=headers,
                proxy=proxy,
            ) as response:
                # Parse JSON response
                response_data = await response.json()
                order_status = response.status

                # Only load response text if needed (non-success)
                if response.status != 200:
                    order_text = await response.text()

                    if ('is not found in OpenOrder List' in response_data['description']) and (response_data is not None):
                        self.logger.info(f"Order is already filled")
                        await self.redis_order_mod_async.hset('order_status:filled', appOrderID, '1')
                        return True

                    self.logger.error(
                        f"Order failed : {order_status} - {order_text} | {order}")
                    
                    # self.logger.error(f"Cancel order failed: {response['description']}")
                    orderToModify = await self.mongo_db_async.find_one({'appOrderId':int(appOrderID)})
                    upDatedData = {'responseFlag':False, 'CancelRejectReason':response_data['description']}
                    await self.mongo_db_async.update_one({'_id':orderToModify['_id']},{"$set": upDatedData})
                    sent = time()
                    sent = json.dumps(str(sent))
                    await self.redis_ping_conn_async.set('lastOrderUpdated', sent)

                    await self._push_error(
                        "order_data", order,
                        order_text, "broker_cancel", order.get('clientID'),
                    )
                    return False

                if response_data['type'] == 'success':
                    orderToModify = await self.mongo_db_async.find_one({'appOrderId':int(appOrderID)})
                    upDatedData = {'responseFlag':'Cancelled'}
                    await self.mongo_db_async.update_one({'_id':orderToModify['_id']},{"$set": upDatedData})
                else:
                    self.logger.error(f"Cancel order failed: {response_data['description']}")
                    orderToModify = await self.mongo_db_async.find_one({'appOrderId':int(appOrderID)})
                    upDatedData = {'responseFlag':False, 'CancelRejectReason':response_data['description']}
                    await self.mongo_db_async.update_one({'_id':orderToModify['_id']},{"$set": upDatedData})
                    sent = time.time()
                    sent = json.dumps(str(sent))
                    await self.redis_ping_conn_async.set('lastOrderUpdated', sent)

                # Log success at debug level to reduce log volume
                self.logger.info(
                    f"Cancel Order sent successfully: AppOrderId: {order['appOrderID']}")
                self.logger.debug(f"Response: {response_data}")
        except asyncio.TimeoutError:
            self.logger.error(
                f"Timeout sending order {order.get('appOrderID', 'unknown')}")
            return False
        except Exception as e:
            self.logger.exception(
                f"Error sending request for order {order.get('appOrderID', 'unknown')}: {str(e)}",
                exc_info=True)
            return False

    async def process_request(self, request: bytes) -> bool:
        """
        Process an order from Redis queue.

        Args:
            order: Order bytes from Redis

        Returns:
            Boolean indicating success
        """
        try:
            # First decode bytes to string if needed
            if isinstance(request, bytes):
                request_str = request.decode('utf-8')
            else:
                request_str = request

            # Parse as JSON
            request_dict = json.loads(request_str)

            # Process any bytes values in the dictionary
            self.logger.debug(f"Processing request: {request_dict}")
            request_dict = parse_redis_dict_values(request_dict)
            
            request_dict['throttler_time'] = datetime.now().timestamp()
            self.mongo_client_async['throttler_logs'][f'logs_{date.today().__str__()}'].insert_one(request_dict)

            if request_dict.get('type', '').lower() == 'order':
                result = await self.send_order_as_http_request(request_dict)
            elif request_dict.get('type', '').lower() == 'modify':
                result = await self.send_modify_order_as_http_request(request_dict)
            elif request_dict.get('type', '').lower() == 'cancel':
                result = await self.send_cancel_order_as_http_request(request_dict)
            else:
                self.logger.error(
                    f"Unknown request type: {request_dict.get('type')}")
                result = False

            return result

        except json.JSONDecodeError as e:
            self.logger.error(
                f"JSON decode error processing order: {e}", exc_info=True)
            return False
        except Exception as e:
            self.logger.error(f"Error processing order: {e}", exc_info=True)
            return False

    async def async_run(self):
        """
        Main async processing loop.
        Processes orders from Redis queues with priority and sliding window rate limiting.
        """
        
        # Define queue names once
        redis_queue_high_priority = f"client:{self.client_id}:high"
        redis_queue_low_priority = f"client:{self.client_id}:low"

        self.logger.info(
            f"Starting async order processing with sliding window "
            f"rate limit of {self.max_orders_per_second} orders/second")

        self._running = True

        while self._running:
            try:
                # Try high priority queue first
                request = await self.redis_client_ops_async.lpop(redis_queue_high_priority)
                is_high_priority = True
                
                if not request:
                    # No high priority orders, try low priority
                    request = await self.redis_client_ops_async.lpop(redis_queue_low_priority)
                    is_high_priority = False

                if request:
                    # We have an order, now check if we can consume a token
                    if (not self.token_bucket.try_consume()) and self.limit_ops:
                        # No tokens available, put the order back to maintain order
                        queue_to_use = redis_queue_high_priority if is_high_priority else redis_queue_low_priority
                        await self.redis_client_ops_async.lpush(queue_to_use, request)
                        
                        # Sleep until next token is available
                        sleep_time = self.token_bucket.time_until_next_refill()
                        if sleep_time > 0:
                            self.logger.debug(
                                f"Rate limit reached, sleeping for {sleep_time:.3f}s until next slot available")
                            await asyncio.sleep(sleep_time + 0.1)
                        continue

                    # We have both an order and a token, process it asynchronously
                    priority_str = "high" if is_high_priority else "low"
                    self.logger.debug(
                        f"Processing {priority_str} priority order "
                        f"(slots remaining: {self.token_bucket.available_tokens()}) | Capacity: {self.token_bucket.capacity}")
                    
                    task = asyncio.create_task(self.process_request(request))
                    self._pending_tasks.add(task)
                    task.add_done_callback(self._pending_tasks.discard)
                else:
                    # No orders at all, sleep briefly to avoid busy waiting
                    await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                self.logger.info("Async run loop cancelled, shutting down")
                break
            except Exception as e:
                self.logger.error(
                    f"Error in async_run: {str(e)}", exc_info=True)
                # Brief sleep to avoid tight error loop
                await asyncio.sleep(1)

    async def async_shutdown(self):
        """
        Gracefully shut down all async resources.
        """
        self.logger.info("Shutting down async resources")
        self._running = False

        # Wait for pending tasks to complete with timeout
        if self._pending_tasks:
            self.logger.info(
                f"Waiting for {len(self._pending_tasks)} pending tasks to complete")
            try:
                await asyncio.wait_for(asyncio.gather(*self._pending_tasks), timeout=5)
            except asyncio.TimeoutError:
                self.logger.warning(
                    "Some tasks did not complete within timeout")

        # Close HTTP session
        if self.http_session:
            await self.http_session.close()

        # Close Redis connections
        if self.redis_client_ops_async:
            self.redis_client_ops_async.close()
            await self.redis_client_ops_async.wait_closed()

        if self.redis_order_mod_async:
            self.redis_order_mod_async.close()
            await self.redis_order_mod_async.wait_closed()

        self.logger.info("All async resources closed successfully")

    def run(self):
        """
        Main process entry point.
        Sets up async event loop and resources, then runs the async processing loop.
        """
        try:
            # Update logger with PID
            self.logger_name = f'{self.logger_name}[PID:{self.pid}]'
            self.logger = setup_logging(
                logger_name=self.logger_name,
                log_dir="logs/client_order_throttler",
                file_name=self.LOG_FILE_NAME
            )

            self.logger.info(
                f"Starting ClientOrderThrottler process for client {self.client_id}")

            # Create and set event loop
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            # Run async initialization and main loop
            self.loop.run_until_complete(self.async_init())
            self.loop.run_until_complete(self.async_run())

        except KeyboardInterrupt:
            self.logger.info("Process interrupted, shutting down")
        except Exception as e:
            self.logger.exception(f"Error in main process: {str(e)}")
        finally:
            # Ensure proper cleanup of async resources
            if self.loop and self.loop.is_running():
                self.loop.run_until_complete(self.async_shutdown())

            # Close the event loop
            if self.loop:
                self.loop.close()

            self.logger.info(
                "ClientOrderThrottler process shut down successfully")
