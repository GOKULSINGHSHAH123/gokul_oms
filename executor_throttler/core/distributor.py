"""
Order distribution module for order routing and priority queuing.

Implements a Redis Streams consumer that reads incoming orders, determines priority
based on order value, and distributes them to client-specific queues. Supports
graceful stream initialization and error recovery.

Classes:
    OrderDistributor: Process that consumes and distributes orders based on priority
"""

import json
import asyncio
from time import sleep
from multiprocessing import Process
from typing import Dict, Any, Optional


from core.logger import setup_logging
from core.utils import parse_redis_dict_values
from core.auth import get_redis_conn


class OrderDistributor(Process):
    # Class constants
    LOG_FILE_NAME = 'order_distributor'
    CONSUMER_GROUP = 'order_distributor_group'
    BLOCK_TIMEOUT_MS = 1000
    READ_COUNT = 5  # Process multiple messages per batch for better throughput
    RETRY_DELAY_SECONDS = 1

    def __init__(self, config: Dict[str, Any], exec_id: str = "Unknown"):
        """
        Initialize the OrderDistributor process.

        Sets up Redis connections for consuming from the incoming orders stream and
        publishing to client priority queues. Configures the consumer group for
        reliable message processing.

        Args:
            config: Configuration parser with Redis, streams, and threshold settings
            exec_id: Unique executor identifier for logging and process tracking
        """
        super().__init__()

        self.exec_id = exec_id
        self.config = config

        self.logger_name = f'[ExecID:{exec_id}]'
        self.logger = setup_logging(
            logger_name=self.logger_name, log_dir="logs/order_distributor", file_name=self.LOG_FILE_NAME)

        self.redis_conn_incoming_orders = get_redis_conn(
            config, self.logger, db_index=14, purpose="Executor Infrastructure")
        self.redis_conn_client_ops_queue = get_redis_conn(
            config, self.logger, db_index=13, purpose="Client Operations")

        self.stream_name = config['params']['redisStreamIncomingOrders']
        self.priority_threshold = float(
            config['params']['order_priority_threshold'])

        self.initialize_redis_streams()
        self.logger.info("Order Distributor initialized")

    def initialize_redis_streams(self) -> None:
        """
        Initialize Redis consumer group for order stream.

        Creates a new consumer group if it doesn't exist, starting from offset 0.
        If the group already exists, handles the BUSYGROUP error gracefully.

        Raises:
            Exception: If stream creation fails for reasons other than group already existing
        """
        self.logger.info("Initializing Redis streams for incoming orders")

        try:
            self.redis_conn_incoming_orders.xgroup_create(
                name=self.stream_name,
                groupname=self.CONSUMER_GROUP,
                id='0',
                mkstream=True
            )
        except Exception as e:
            # If the group already exists, we can ignore the error
            if "BUSYGROUP" not in str(e):
                self.logger.error(
                    f"Error creating consumer group: {e}", exc_info=True)
                raise
            else:
                self.logger.debug(
                    f"Consumer group {self.CONSUMER_GROUP} already exists")

    def distribute_requests_from_stream(self) -> None:
        """
        Main message consumption and distribution loop.

        Continuously reads orders from the Redis stream as part of the consumer group,
        processes them in batches for efficiency, and automatically acknowledges
        successfully processed messages. Handles stream recreation on error.
        """
        while True:
            try:
                messages = self.redis_conn_incoming_orders.xreadgroup(
                    self.CONSUMER_GROUP,
                    self.exec_id,
                    {self.stream_name: '>'},
                    count=self.READ_COUNT,
                    block=self.BLOCK_TIMEOUT_MS
                )

                if not messages:
                    continue

                # Process messages in batch for better performance
                for stream, message_list in messages:
                    for message_id, message in message_list:
                        self._process_single_message(message_id, message)

            except Exception as e:
                if "NOGROUP" in str(e) or "stream does not exist" in str(e).lower():
                    self.logger.warning(f"Stream appears to be deleted, attempting to recreate")
                    self.initialize_redis_streams()
                    self.logger.info("Successfully recreated stream and consumer group")
                else:
                    self.logger.error(f"Fatal error in distribute_requests_from_stream: {e}", exc_info=True)
                sleep(self.RETRY_DELAY_SECONDS)

    def _process_single_message(self, message_id: bytes, message: Dict[bytes, bytes]) -> None:
        """
        Process a single order message and route to appropriate priority queue.

        Parses the message, extracts the client ID and order details, determines
        priority based on order value, and enqueues to the appropriate client queue.
        Acknowledges the message after successful processing.

        Args:
            message_id: Redis stream message ID (bytes)
            message: Message payload dictionary with byte keys/values
        """
        try:
            # Decode message_id once
            decoded_message_id = message_id.decode('utf-8')

            # Parse Redis message values
            parsed_message = parse_redis_dict_values(message)
            order_unique_identifier = parsed_message.get(
                'orderUniqueIdentifier', '')
            app_order_id = parsed_message.get(
                'appOrderID', '')

            # Extract and parse body data once
            body_json = parsed_message.get('body', '{}')
            try:
                body_data = json.loads(body_json)
                client_id = body_data.get('clientID', 'unknown')
            except json.JSONDecodeError as e:
                self.logger.warning(
                    f"Failed to parse body JSON for message {decoded_message_id}\t"
                    f"OUI: {order_unique_identifier}, Error: {e}"
                )
                client_id = 'unknown'
                body_data = {}

            parsed_message['message_id'] = decoded_message_id

            # Determine priority and create queue name
            priority = self._determine_priority(body_data)
            priority_queue = f"client:{client_id}:{priority}"

            # Push to queue and acknowledge in Redis
            self.redis_conn_client_ops_queue.lpush(
                priority_queue, json.dumps(parsed_message))

            # Log queue size after enqueueing
            queue_size = self.redis_conn_client_ops_queue.llen(priority_queue)
        
            self.redis_conn_incoming_orders.xack(
                self.stream_name, self.CONSUMER_GROUP, message_id)

            self.logger.info(
                f"Enqueued order MSG_ID:{decoded_message_id}\tOrderUniqueID:{order_unique_identifier}\tAppOrderID:{app_order_id} "
                f"to {priority} priority queue for client {client_id} (Queue size: {queue_size})"
            )

        except Exception as e:
            # More specific error handling
            message_id_str = message_id.decode(
                'utf-8') if isinstance(message_id, bytes) else str(message_id)
            self.logger.error(
                f"Error processing message {message_id_str}: {e}", exc_info=True)

    def _determine_priority(self, order: Dict[str, Any]) -> str:
        """
        Determine order priority based on notional value (price × quantity).

        Calculates the order's notional value and compares against a configured
        threshold to assign high or low priority. Defaults to low priority on errors.

        Args:
            order: Parsed order data dictionary containing limitPrice/modifiedLimitPrice
                   and orderQuantity/modifiedOrderQuantity

        Returns:
            str: 'high' if notional value exceeds threshold, 'low' otherwise
        """
        try:
            if 'limitPrice' in order.keys():
            # Use get with default values to avoid multiple lookups
                price = float(order.get('limitPrice'))
                qty = int(order.get('orderQuantity'))
            elif 'modifiedLimitPrice' in order.keys():
                price = float(order.get('modifiedLimitPrice'))
                qty = int(order.get('modifiedOrderQuantity'))
            else:
                self.logger.debug(
                    "No price information found, defaulting to 'low' priority")
                return 'low'
            
            # Calculate priority value (price * quantity)
            priority_value = price * qty
            self.logger.debug(
                f"Calculated priority value: {priority_value} (Price: {price}, Quantity: {qty})")

            return 'high' if priority_value > self.priority_threshold else 'low'

        except (ValueError, TypeError) as e:
            self.logger.warning(
                f"Error calculating priority, defaulting to 'low': {e}")
            return 'low'

    def run(self) -> None:
        """
        Main process entry point for order distribution.

        Initializes logger with process ID and starts the order distribution loop.
        Handles interruption and fatal errors gracefully.
        """
        # Re-initialize logger with PID in the child process
        self.logger_name = f'{self.logger_name}[PID:{self.pid}]'
        self.logger = setup_logging(
            logger_name=self.logger_name, log_dir="logs/order_distributor", file_name=self.LOG_FILE_NAME)

        self.logger.info(
            f"Order Distributor process started with PID {self.pid}")

        try:
            self.distribute_requests_from_stream()
        except KeyboardInterrupt:
            self.logger.info("Order Distributor process interrupted by user")
        except Exception as e:
            self.logger.error(
                f"Fatal error in Order Distributor process: {e}", exc_info=True)
            raise
