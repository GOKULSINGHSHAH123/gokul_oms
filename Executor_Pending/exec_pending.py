import asyncio
import json
import logging
import redis.asyncio as aioredis
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any, List, Optional
from configparser import ConfigParser
import os
import sys
from stream_logger import RedisStreamLogger
import datetime
import time
from secrets import token_hex
import random
import uvloop
import aiohttp
from urllib.parse import quote_plus


# Use uvloop for better performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

class QueueOrderProcessor:
    """
    Processes pending orders using a Redis queue approach.
    1. Reads orders from input stream and pushes to Redis queue
    2. Pops orders from queue, processes them, and either completes or re-queues
    3. Simple, no stream complexity, easier to monitor and maintain
    """

    def __init__(
        self, 
        config: ConfigParser,
        input_stream,
        pending_queue,
        logging_stream,
        error_stream,
        order_stream,
        modify_queue,
        processing_concurrency,
        modify_min_limit,
        modify_max_limit,
    ):
        
        self.config = config
        self.input_stream = input_stream
        self.pending_queue = pending_queue
        self.order_stream = order_stream
        self.modify_queue = modify_queue
        self.throttler_stream_name = self.config.get('params', 'throttler_stream',fallback='throttle_all_orders')


        self.consumer_group = sys.argv[1]
        self.consumer_name = sys.argv[2]
        self.modify_min_limit = modify_min_limit
        self.modify_max_limit = modify_max_limit

        self.time_period = float(self.config['params'].get('modify_interval', 1))
        self.modify_workers = self.config['params'].getint('modify_workers', 1)
        self.modify_url = self.config.get('params', 'modify_url')
        self.small_option_limit = self.config.getfloat('params', 'small_option_limit')

        self.token_map = {}
        
        # Set up logging
        logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        force=True,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        )

        logging.info(f"Starting Queue-Based Order Processor for {self.consumer_group} - {self.consumer_name}")

        self.logging_stream = logging_stream
        self.error_stream = error_stream
                
        # Concurrency control
        self.processing_concurrency = processing_concurrency
        
        # For graceful shutdown
        self.is_running = False

    async def get_redis_conn(self, db:int = 0, key = 'dbParams') -> aioredis.Redis:
        try:
            host = self.config[key]['redisHost']
            port = int(self.config[key]['redisPort'])
            password = self.config[key]['redisPass']
            redis = await aioredis.Redis(
                host=host,
                port=port,
                password=password,
                db=db,
                decode_responses=True
            )
            logging.info(f"Connected to Redis at {host}:{port}, db {db}")
            return redis
        except Exception as e:
            logging.exception(f"Failed to connect to Redis: {str(e)}")
            raise

    async def get_mongo_conn(self):
        try:
            mongo_host = self.config['infraParams']['mongoHost']
            mongo_port = int(self.config['infraParams']['mongoPort'])
            mongo_user = self.config['infraParams']['mongoUser']
            mongo_pass = self.config['infraParams']['mongoPass']

            # Escape username and password for MongoDB URI
            mongo_user_encoded = quote_plus(mongo_user)
            mongo_pass_encoded = quote_plus(mongo_pass)

            # Construct MongoDB URI with properly escaped credentials
            mongo_uri = f"mongodb://{mongo_user_encoded}:{mongo_pass_encoded}@{mongo_host}:{mongo_port}/"
            
            # Create MongoDB client and database reference
            mongo_client = AsyncIOMotorClient(mongo_uri)
            logging.info(f"Connected to MongoDB at {mongo_host}:{mongo_port}")
            return mongo_client
        except Exception as e:
            logging.exception(f"Failed to connect to MongoDB: {str(e)}")
            raise

    async def get_token_map(self):
        try: 
            token_map = await self.token_redis.get('tokenMap')
            if token_map: 
                self.token_map = json.loads(token_map)
                logging.info("Token map loaded successfully")
        except Exception as e:
            await self.logger.exception(f"Error getting token map: {str(e)}")
    
    async def initialize(self):
        """Initialize connections and set up consumer group."""
        logging.info("Initializing Queue-Based Order Processor")
                
        # Connect to Redis for various needs
        self.sym_redis = await self.get_redis_conn(db=0)
        self.master_redis = await self.get_redis_conn(db=11)
        self.stream_redis = await self.get_redis_conn(db=14, key='infraParams')
        self.ping_redis = await self.get_redis_conn(db=9, key='infraParams')
        self.modify_redis = await self.get_redis_conn(db=14, key='infraParams')
        self.token_redis = await self.get_redis_conn(db=10, key='infraParams')

        self.logger = RedisStreamLogger(self.stream_redis, self.logging_stream)
        self.logger.added_packets.update({'consumer_group': self.consumer_group, 'consumer_name': self.consumer_name})
        await self.ping_redis.rpush('alarms-v2', 'exec_pending_start')

        await self.get_token_map()
        self.session = aiohttp.ClientSession()
        self.db_mongo_client = await self.get_mongo_conn()
        self.orders_db = self.db_mongo_client['symphonyorder_raw'][f'orders_{str(datetime.date.today())}']
        # Create consumer group for input stream
        await self.create_consumer_group(self.input_stream)

    async def create_consumer_group(self, stream_name: str):
        try:
            await self.stream_redis.xgroup_create(
                stream_name, 
                self.consumer_group,
                id='0',  # Start from beginning of stream
                mkstream=True  # Create stream if it doesn't exist
            )
            logging.info(f"Created consumer group {self.consumer_group} for stream {stream_name}")
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logging.info(f"Consumer group {self.consumer_group} already exists for stream {stream_name}")
            else:
                raise

    async def close(self):
        """Close all connections."""
        self.is_running = False
        if self.session: 
            self.session.close()
        # Close Redis connections
        if self.sym_redis:
            await self.sym_redis.close()
        if self.master_redis:
            await self.master_redis.close()
        if self.stream_redis:   
            await self.stream_redis.close()
        if self.ping_redis:
            await self.ping_redis.close()
        if self.modify_redis:
            await self.modify_redis.close()
        if self.db_mongo_client:
            self.db_mongo_client.close()
        logging.info("Closed all connections")

    async def get_current_ltp(self, sym_id: int) -> float:
        """Get the current Last Traded Price for a symbol."""
        try:
            ltp = float(await self.sym_redis.get(str(sym_id)))
            return ltp
        except Exception as e:
            logging.exception(f"Error getting current LTP for {sym_id}: {str(e)}")
            return None

    async def check_order_status(self, ouid: str) -> bool:
        try:
            order_flag = await self.modify_redis.hget('order_status:filled', ouid)
            
            if not order_flag:
                order_status = await self.orders_db.distinct('responseFlag',{'orderUniqueIdentifier': ouid})
                if not order_status:
                    await self.logger.warning(f"Order {ouid} not found in database")
                    return False
                return order_status[0]
            
            return True if order_flag == '1' else False

        except Exception as e:
            await self.logger.exception(f"Error checking order status for {ouid}: {str(e)}")
            return False

    async def modify_order_price(self, pending_order: Dict[str, Any]) -> float:
        """
        Modify order price based on the current LTP and strategy parameters.
        
        Returns:
            Dict with modified order details and success status
        """
        try:
            sym_id = pending_order['exchangeInstrumentID']
            
            mfile = await self.master_redis.get(sym_id)
            if not mfile:
                raise Exception(f"Master file not found for {sym_id}") 
            
            mfile = json.loads(mfile)
            tick_size = float(mfile['TickSize'])
            # upper_band = float(mfile['PriceBand.High'])
            # lower_band = float(mfile['PriceBand.Low'])
            upper_price_limit = float(pending_order['upperPriceLimit'])
            lower_price_limit = float(pending_order['lowerPriceLimit'])

            # Get current market price
            current_ltp = await self.get_current_ltp(sym_id)
            if not current_ltp:
                raise Exception(f"Failed to get current LTP for {sym_id}")
            
            extra_move = float(pending_order.get('extraPercent', 0.1))*current_ltp
            

            await self.logger.info(f'Extra move is {extra_move} for current LTP {current_ltp}')
            if tick_size <= current_ltp <= 20: extra_move = 2
            side_modifier = 1 if pending_order['orderSide'].upper() == 'BUY' else -1
            new_price = current_ltp + extra_move * side_modifier
            await self.logger.info(f'Current LTP is {current_ltp} and new price is {new_price}')
            new_price = max(new_price, tick_size)

            if side_modifier == 1:
                new_price = min(new_price, upper_price_limit)
            if side_modifier == -1:
                new_price = max(new_price, lower_price_limit)

            new_price = round(new_price / tick_size) * tick_size
            # if not (lower_band < current_ltp < upper_band):
            #     new_price = min(upper_band, max(lower_band, current_ltp))
            
            # else: 
            #     # Calculate new price based on order type and price limits
            #     upper_price_limit = float(pending_order['upperPriceLimit'])
            #     lower_price_limit = float(pending_order['lowerPriceLimit'])
            #     extra_percent = float(pending_order['extraPercent'])
            #     order_type:str = pending_order['orderType']
                                
            #     if order_type.upper() == 'BUY':
            #         # For buy orders, we might increase the price to get filled
            #         new_price = min(current_ltp * (1 + extra_percent/100), upper_price_limit)
            #     else:
            #         # For sell orders, we might decrease the price to get filled
            #         new_price = max(current_ltp * (1 - extra_percent/100), lower_price_limit)
            await self.logger.info(f'Current LTP is {current_ltp} and new price is {new_price}')
            return new_price,current_ltp

        except Exception as e:
            await self.logger.exception(f"Error modifying order price: {str(e)}")
            raise Exception('Error modifying order price: {str(e)}')
    
    async def send_modify_request_via_throttler(self, modify_packet: Dict[str, Any], pending_order: Dict[str, Any],app_order_id: str):

        try:
            if modify_packet['clientID'] not in self.token_map:
                await self.logger.warning(f"Client ID {modify_packet['clientID']} not found in token map, fetching token map again")
                await self.get_token_map()
                if modify_packet['clientID'] not in self.token_map:
                    await self.logger.error(f"Client ID {modify_packet['clientID']} still not found in token map after refresh")

            token = self.token_map[modify_packet['clientID']]

            headers = {'Authorization': token['token']}

            # Publish HTTP request details to Redis Stream
            # Ensure all values are plain strings (JSON-encode complex structures)
            stream_data = {
                "type": "modify",
                "method": "PUT",
                "url": str(self.modify_url),
                "headers": json.dumps(headers),
                "body": json.dumps(modify_packet),
                "retryCount": str(int(pending_order.get('retryCount'))),
                "orderUniqueIdentifier": pending_order.get('orderUniqueIdentifier', ''),
                "appOrderID": app_order_id,
            }

            # xadd expects a mapping of field->value, not a JSON string
            message_id = await self.stream_redis.xadd(self.throttler_stream_name, stream_data, maxlen=10000, approximate=True)

            logging.info(
                f"Order sent successfully: {message_id}\t Stream Data: {stream_data}")
            logging.info(
                f"Published to Redis stream with message ID: {message_id}")

        except Exception as e:
            logging.exception(
                f"Error requesting order {modify_packet}: {str(e)}")
        
    async def send_modify_request(self, modify_packet: Dict[str, Any], pending_order: Dict[str, Any]) -> Optional[bool]:
        try:
            # if modify_packet['clientID'] not in self.token_map:
            #     await self.logger.warning(f"Client ID {modify_packet['clientID']} not found in token map, fetching token map again")
            #     await self.get_token_map()
            #     if modify_packet['clientID'] not in self.token_map:
            client_details = await self.token_redis.hgetall(f'token:{modify_packet["clientID"]}')
            if not client_details:
                await self.logger.error(f"Client ID {modify_packet['clientID']} not found in token map")
                return None
            modify_packet['clientID'] = modify_packet['clientID'] if client_details['type'].lower() != 'pro' else '*****'
            headers = {'Authorization': client_details['token']}
            modify_url = self.modify_url
            if 'endpoint' in client_details:
                endpoint = client_details['endpoint']
                suffix = self.modify_url.split('interactive')[-1]
                modify_url = f'{endpoint}{suffix}'
            if not self.session: self.session = aiohttp.ClientSession()
            async with self.session.put(
                modify_url, 
                data=modify_packet, 
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                result = await response.json()
                await self.logger.info(f"{modify_packet['appOrderID']}: {result}")
                if response.status != 200:
                    await self.logger.info(f"Response for {modify_packet['appOrderID']}: {result['description']}")
                    if 'is not found in OpenOrder List' in result['description']:
                        await self.logger.info(f"Order is already filled")
                        return True
                    else: 
                        # await self.logger.error(f"Failed to modify order {modify_packet['appOrderID']}: {result['description']}")
                        return False
                else: 
                    try:
                        await self.orders_db.update_one(
                            {'appOrderId': int(modify_packet['appOrderID'])},
                            {'$set': {
                                'limitPrice': modify_packet['modifiedLimitPrice'],
                                'orderSentTime':time.time(),
                                'mod_status':f"modified_{pending_order.get('retryCount',0)}"
                                }}
                        )
                    except Exception as e:
                        await self.logger.error(f'Order {modify_packet["appOrderID"]} modification failed to update in MongoDB: {str(e)}')
                    return False
            return False     
        except Exception as e:
            await self.logger.exception(f"Error sending modify request: {str(e)}")
            return False

    async def should_modify_order(self, pending_order: Dict[str, Any]) -> bool:
        """
        Determine if an order should be modified based on time elapsed.
        """
        current_time = time.time()
        start_time = float(pending_order.get('startTime', 0))
        # await self.logger.info(f'pending order is {pending_order}')
          # Default 30 seconds
        
        # If time_period has elapsed since the order was placed/last modified
        if current_time - start_time > self.time_period:
            return True
        
        return False
    
    async def send_secondary_order(self, pending_order: Dict[str, Any]):
        """
        Send a secondary order based on the pending order details.
        
        Args:
            pending_order: The pending order data to process
        """
        try:
            # Extract necessary details from the pending order
            app_order_id = pending_order['appOrderID']
            sym_id = pending_order['exchangeInstrumentIDSell']
            if sym_id == 'None':
                await self.logger.warning(f"Secondary order not sent, sym_id is None for order {app_order_id}")
                return
            current_ltp = await self.get_current_ltp(sym_id)
            if not current_ltp:
                await self.logger.warning(f"Failed to get LTP for secondary order {app_order_id}")
                return
            limit_price = current_ltp*0.8
            limit_price = max(round(limit_price,1),0.1)

            final_order = {
                'clientID': pending_order['clientID'], 
                'algoName': pending_order['algoName'], 
                'exchangeSegment': pending_order['exchangeSegment'], 
                'exchangeInstrumentID': sym_id, 
                'exchangeInstrumentIDSell': 'None',
                'productType': 'NRML', 
                'orderType': 'LIMIT', 
                'orderSide': 'SELL', 
                'timeInForce': 'DAY', 
                'disclosedQuantity': 0, 
                'orderQuantity': int(pending_order['quantity']), 
                'limitPrice': limit_price, 
                'stopPrice': 0, 
                'upperPriceLimit': limit_price*10, 
                'lowerPriceLimit': 0.1, 
                'timePeriod': 1, 
                'extraPercent': 0.1, 
                'algoPrice': current_ltp, 
                'algoTime': time.time(), 
                'ltpExceedMaxValueCheck': '0', 
                'orderUniqueIdentifier': self.get_unique_id(),
                'algoSignalUniqueIdentifier': self.get_unique_id(),
            }

            await self.logger.info(f"Sending secondary order: {final_order}")
            await self.stream_redis.xadd(self.order_stream, final_order)

        except Exception as e:
            await self.logger.exception(f"Error sending secondary order: {str(e)}")
            return
    
    def get_unique_id(self) -> str:
        timestamp = int(time.time())  # Current Unix timestamp in milliseconds
        random_token = token_hex(4)  # Generate a random 16-character hex token
        return f"{timestamp}{random_token}"
            
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
            "client_id":  str(client_id) if client_id else "",
            "timestamp":  int(time.time() * 1000),
        }
        await self.stream_redis.xadd(self.error_stream, payload)
        await self.logger.warning(f"Error -> {self.error_stream}  stage={stage}  reason={reason}")

    async def process_pending_order(self, pending_order: Dict[str, Any]) -> bool:
        """
        Process a single pending order: check if filled, modify if needed.
        
        Args:
            pending_order: The pending order data to process
            
        Returns:
            bool: True if order was completed (filled, rejected, etc), False if requeued
        """
        try:

            self.logger.added_packets.update({'pending_id': pending_order['pending_id']})
            app_order_id = pending_order.get('appOrderID')
            if not app_order_id:
                await self.logger.error(f"Missing appOrderID in pending order data: {pending_order}")
                return True  # Don't requeue invalid orders
            
            retry_count = int(pending_order.get('retryCount', 0))
            # await self.logger.info(f"Processing pending order: {app_order_id} (retry count: {retry_count})")
            
            # Check order status with broker
            res_flag = await self.check_order_status(pending_order['orderUniqueIdentifier'])

            # if order_status == None:
            #     await self.logger.warning(f"Failed to get order status for {app_order_id}, will retry later")
            #     return False  # Requeue for retry

            
            # Check if the order is filled
            if res_flag == True:
                await self.logger.info(f"Order {app_order_id} is filled, rejected or cancelled.")
                if pending_order.get('orderType').lower() == 'buythensell':
                    await self.logger.info(f"Order {app_order_id} is a buy then sell order.")
                    await self.send_secondary_order(pending_order)
                return True  # Order completed, don't requeue
            
            # If order is still pending, check if it should be modified
            if await self.should_modify_order(pending_order):
                # await self.logger.info(f"Time to modify order {app_order_id}")
                # Modify the order
                # await self.logger.info(f"Modifying order {app_order_id} due to time elapsed")

                new_price,ltp = await self.modify_order_price(pending_order)
                skip_order_below_price = float(self.config.get('params', 'skip_order_below', fallback=1))

                if ltp < skip_order_below_price:
                    # await self.logger.info(f"Skipping order below {skip_order_below_price} with appOrderID: {app_order_id} LTP: {ltp} ModPrice: {new_price}")
                    return False

                modify_packet = {
                    'appOrderID': app_order_id, 
                    'clientID': pending_order['clientID'],
                    'modifiedProductType': 'NRML', 
                    'modifiedOrderType': 'LIMIT',
                    'modifiedOrderQuantity': pending_order['quantity'],
                    'modifiedDisclosedQuantity': 0,
                    'modifiedTimeInForce': 'DAY',
                    'modifiedLimitPrice': new_price,
                    'modifiedStopPrice': 0,
                }

                await self.logger.info(f"Modifying order {app_order_id} with new price: {new_price}")

                if self.config.get('params', 'use_throttler', fallback='false').lower() == 'true':
                    await self.logger.info(f"Sending modify request via throttler for order {app_order_id}")
                    await self.send_modify_request_via_throttler(modify_packet, pending_order, app_order_id)
                    order_flag = await self.modify_redis.hget('order_status:filled', pending_order['orderUniqueIdentifier'])
                    if order_flag == '1':
                        await self.logger.info(f"Order {app_order_id} is already filled (detected after throttler send), marking as completed")
                        if pending_order.get('orderType', '').lower() == 'buythensell':
                            await self.logger.info(f"Order {app_order_id} is a buy then sell order.")
                            await self.send_secondary_order(pending_order)
                        return True  # Order completed, don't requeue
                else:
                    self.logger.info(f"Sending direct modify request for order {app_order_id}")
                    status = await self.send_modify_request(modify_packet, pending_order)

                    if status: 
                        await self.logger.info(f"Order {app_order_id} not found in OpenOrder List, marking as completed")
                        if pending_order.get('orderType').lower() == 'buythensell':
                            await self.logger.info(f"Order {app_order_id} is a buy then sell order.")
                            await self.send_secondary_order(pending_order)
                        return True  # Order completed, don't requeue

                    # if not status: 
                    #     # await self.logger.warning(f"Failed to modify order {app_order_id}, will retry later")
                    #     return False
                
                # Update the pending order with new details
                pending_order['retryCount'] = retry_count + 1
                pending_order['startTime'] = str(time.time())  # Reset timer
                
                # await self.logger.info(f"Modified order {app_order_id}, price: {new_price}, retry: {retry_count + 1}")
                return False  # Requeue with updated details

            else:
                # await self.logger.debug(f"Order {app_order_id} checked but no modification needed yet")
                return False  # Requeue to check again later
                
        except Exception as e:
            await self.logger.exception(f"Error processing pending order: {str(e)}")
            await self._push_error(
                "pending_order", pending_order,
                str(e), "process_pending", pending_order.get('clientID'),
            )
            return False  # Requeue on error

    async def stream_to_queue(self):
        """
        Read orders from input stream and push them to the Redis queue.
        """
        while self.is_running:
            try:
                # Read new messages from the stream
                messages = await self.stream_redis.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    {self.input_stream: '>'},  # > means new messages only
                    count=20,  # Process 20 messages at a time
                    block=2000  # Block for 2 seconds if no messages
                )
                
                if not messages:
                    continue
                    
                # Process each message
                for stream, stream_messages in messages:
                    await self.logger.info(f"Received {len(stream_messages)} messages from input stream")
                    
                    for message_id, fields in stream_messages:
                        # Parse order data from fields
                        order_data:dict = fields
                        # Set initial retry count and start time if not present
                        if 'retryCount' not in order_data:
                            order_data['retryCount'] = '0'
                        if 'startTime' not in order_data:
                            order_data['startTime'] = str(time.time())
                            
                        # Convert to JSON string for queue storage
                        order_data['pending_id'] = message_id
                        order_json = json.dumps(order_data)
                        
                        # Push to Redis queue
                        await self.stream_redis.rpush(self.pending_queue, order_json)
                        
                        # Acknowledge the message after pushing to queue
                        await self.stream_redis.xack(self.input_stream, self.consumer_group, message_id)
                        
                        ouid = order_data.get('orderUniqueIdentifier', 'unknown')
                        await self.logger.info(f"Pushed order {ouid} to pending queue")
                
            except aioredis.ResponseError as e:
                error_msg = str(e).upper()
                if any(keyword in error_msg for keyword in ['NOGROUP', 'NOSTREAM']):
                    await self.logger.error(f"Stream or consumer group missing: {str(e)}")
                    await self.logger.info("Attempting to recreate consumer group...")
                    try:
                        await self.create_consumer_group(self.input_stream)
                        await self.logger.info("Consumer group recreated successfully")
                    except Exception as recreate_error:
                        await self.logger.exception(f"Failed to recreate consumer group: {recreate_error}")
                        # Implement exponential backoff to prevent tight loops
                        await asyncio.sleep(10)
                else:
                    await self.logger.exception(f"Redis response error: {str(e)}")
                    await asyncio.sleep(5)
                    
            except asyncio.CancelledError:
                await self.logger.info("Stream to queue task cancelled")
                break
                
            except Exception as e:
                await self.logger.exception(f"Error in stream to queue task: {str(e)}")
                # Sleep briefly to prevent tight loop on persistent errors
                await asyncio.sleep(1)

    async def process_queue(self):
        """
        Process orders from the Redis queue.
        
        Args:
            worker_id: Worker identifier for logging
        """
        await self.logger.info(f"Starting queue processor")
        
        while self.is_running:
            try:
                # Try to get an order from the queue with a timeout
                result = await self.stream_redis.blpop(self.pending_queue, timeout=1)
                if not result:
                    # No orders in queue, just continue
                    continue
                
                # Parse the order data from JSON
                # await self.logger.info(f"Worker got order from queue {result}")
                _, order_json = result
                pending_order:dict = json.loads(order_json)
                
                app_order_id = await self.modify_redis.hget('order_status:idmap', pending_order['orderUniqueIdentifier'])

                completed = False
                if app_order_id:
                    if app_order_id == '0':
                        await self.logger.info(f"Order {pending_order['orderUniqueIdentifier']} has errored, removing from queue")
                        continue
                    pending_order['appOrderID'] = app_order_id
                    completed = await self.process_pending_order(pending_order)
                else: 
                    # await self.logger.info(f"Order {pending_order['orderUniqueIdentifier']} not found in idmap, requeueing")
                    await asyncio.sleep(0.1)

                if not completed:
                    # Order needs more processing, push back to queue
                    # try:pending_order['startTime'] = str(time.time())
                    # except:pass
                    await self.stream_redis.rpush(self.pending_queue, json.dumps(pending_order))

                else:
                    await self.logger.info(f"Worker completed order {app_order_id}")
                
            except asyncio.CancelledError:
                await self.logger.info(f"Queue processor worker cancelled")
                break
                
            except Exception as e:
                await self.logger.exception(f"Error in queue processor worker: {str(e)}")
                await asyncio.sleep(1)

    async def claim_pending_messages(self):
        """
        Look for any pending messages in the input stream that weren't processed and push them to queue.
        """
        while self.is_running:
            try:
                # Read pending messages for our consumer
                pending = await self.stream_redis.xpending_range(
                    self.input_stream,
                    self.consumer_group,
                    min='-',
                    max='+',
                    count=10
                )
                
                if not pending:
                    # No pending messages, check again later
                    await asyncio.sleep(60)
                    continue
                
                for p in pending:

                    message_id = p['message_id']
                    
                    # Claim the message
                    claimed = await self.stream_redis.xclaim(
                        self.input_stream,
                        self.consumer_group,
                        self.consumer_name,
                        min_idle_time=60 * 1000,  # 1 minute in milliseconds
                        message_ids=[message_id]
                    )
                    
                    if not claimed:
                        continue
                    
                    for c_message_id, fields in claimed:
                        if not c_message_id: 
                            continue
                        
                        order_data:dict = fields
                        
                        # Set initial values if missing
                        if 'retryCount' not in order_data:
                            order_data['retryCount'] = '0'
                        if 'startTime' not in order_data:
                            order_data['startTime'] = str(time.time())
                            
                        # Convert to JSON and push to queue
                        order_json:dict = json.dumps(order_data)
                        await self.stream_redis.rpush(self.pending_queue, order_json)
                        
                        # Acknowledge the claimed message
                        await self.stream_redis.xack(self.input_stream, self.consumer_group, c_message_id)
                        
                        ouid = order_data.get('orderUniqueIdentifier', 'unknown')
                        await self.logger.info(f"Claimed and requeued pending message for order {ouid}")
                
            except Exception as e:
                await self.logger.exception(f"Error claiming pending messages: {str(e)}")
                await asyncio.sleep(10)

    async def monitor_queue_size(self):
        """
        Periodically monitor and log the queue size for visibility.
        """
        while self.is_running:
            try:
                # Get current queue length
                queue_len = await self.stream_redis.llen(self.pending_queue)
                
                # Log the current queue status
                await self.logger.info(f"Current pending queue size: {queue_len}")
                
                # Wait before checking again
                await asyncio.sleep(30)
                
            except Exception as e:
                await self.logger.exception(f"Error monitoring queue size: {str(e)}")
                await asyncio.sleep(10)

    async def trim_streams(self):
        """
        Periodically trim Redis streams to prevent them from bloating.
        """
        while self.is_running:
            try:
                # Trim input and output streams to reasonable lengths
                await self.stream_redis.xtrim(self.input_stream, maxlen=10000, approximate=True)
                
                # await self.logger.info("Trimmed streams")
            except Exception as e:
                await self.logger.exception(f"Error in trim_streams: {str(e)}")
            
            # Wait before trimming again
            await asyncio.sleep(60)

    async def run(self):
        """
        Run the queue-based order processor.
        """
        await self.initialize()
        self.is_running = True
        
        # Start the stream to queue task
        stream_to_queue_task = asyncio.create_task(self.stream_to_queue())
        
        # Start  queue processor worker
        queue_processor = asyncio.create_task(self.process_queue())
        
        # Start the pending message claimer
        claim_task = asyncio.create_task(self.claim_pending_messages())
        
        # Start queue monitor
        monitor_task = asyncio.create_task(self.monitor_queue_size())
        
        # Start stream trimmer
        trimmer_task = asyncio.create_task(self.trim_streams())
        
        # Combine all tasks
        all_tasks = [stream_to_queue_task, claim_task, monitor_task, trimmer_task, queue_processor]
        
        try:
            # Run until interrupted
            await asyncio.gather(*all_tasks)
        finally:
            await self.close()


async def main():
    """Main entry point for the application."""
    # Load configuration
    config = ConfigParser()
    config.read('config.ini')

    payload = {
        'config': config,
        'input_stream': config['params']['input_stream'],
        'pending_queue': config['params']['pending_queue'],
        'logging_stream': config['params']['logging_stream'],
        'error_stream': config['params']['error_stream'],
        'order_stream': config['params']['order_stream'],
        'modify_queue': config['params']['modify_queue'],
        'processing_concurrency': int(config['params']['processing_concurrency']),
        'modify_min_limit': float(config['params']['modify_min_limit']),
        'modify_max_limit': float(config['params']['modify_max_limit']),
    }
    
    # Create and run the queue-based order processor
    processor = QueueOrderProcessor(**payload)
    
    try:
        await processor.run()
    except KeyboardInterrupt:
        logging.info("Shutting down")
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main())