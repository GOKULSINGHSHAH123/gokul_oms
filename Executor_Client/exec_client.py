import asyncio
import json
import logging
import redis.asyncio as aioredis
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any
from configparser import ConfigParser
from slicer import OrderSlicer
from api_client import BrokerAPIClient
import os
import sys
from stream_logger import RedisStreamLogger
import datetime
import time
import uvloop
import random
from urllib.parse import quote_plus

# Use uvloop for better performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# Configure logging

class TradeExecutionEngine:
    """
    Main execution engine that processes trades from Redis and executes them.
    """

    def __init__(
        self, 
        config: ConfigParser,
        input_stream,
        output_stream,
        logging_stream,
        error_stream,
        broker_url,
        route,
        processing_concurrency
    ):
        
        self.config = config


        self.input_stream = input_stream
        self.output_stream = output_stream
        self.consumer_group = sys.argv[1]
        self.consumer_name = sys.argv[2]

        logdir = f'{self.consumer_group}_Logs/{str(datetime.date.today())}/'
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        logfile = f'{logdir}{self.consumer_name}.log'

        logging.basicConfig(
            level=logging.INFO,
            filename=logfile,
            filemode='a',
            force=True,
            format='%(asctime)s - %(levelname)s - %(message)s',
        )

        logging.info(f"Starting Trade Execution Engine for {self.consumer_group} - {self.consumer_name}")

        self.logging_stream = logging_stream
        self.error_stream = error_stream
        
        self.broker_client = BrokerAPIClient(broker_url, route, self.config)
        self.order_slicer = OrderSlicer()
        
        self.is_mock = True if config['params']['is_mock'] == 'True' else False

        # Controls how many orders we process concurrently
        self.processing_concurrency = processing_concurrency
        self.processing_semaphore = asyncio.Semaphore(processing_concurrency)
             
        # For graceful shutdown
        self.is_running = False
        self.tasks = set()

    async def get_redis_conn(self, db:int = 0, key = 'dbParams') -> aioredis.Redis:
        try:
            host = self.config[key]['redisHost']
            port = int(self.config[key]['redisPort'])
            password = self.config[key]['redisPass']
            redis = await aioredis.Redis(
                host=host,
                port=port,
                password=password,
                db = db,
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

            mongo_user = quote_plus(mongo_user)
            mongo_pass = quote_plus(mongo_pass)
            
            # Construct MongoDB URI
            mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/"
            
            # Create MongoDB client and database reference
            mongo_client = AsyncIOMotorClient(mongo_uri, directConnection=True)
            logging.info(f"Connected to MongoDB at {mongo_host}:{mongo_port}")
            return mongo_client
        except Exception as e:
            logging.exception(f"Failed to connect to MongoDB: {str(e)}")
            raise
    
    async def initialize(self):
        """Initialize connections and set up consumer group."""
        logging.info("Initializing Trade Execution Engine")
                
        # Initialize broker API client
        await self.broker_client.initialize()

        # Connect to Redis for data storage
        self.sym_redis = await self.get_redis_conn(db=0)

        self.master_redis = await self.get_redis_conn(db=11)

        self.stream_redis = await self.get_redis_conn(db=14, key='infraParams')

        self.ping_redis = await self.get_redis_conn(db=9, key='infraParams')

        self.token_redis = await self.get_redis_conn(db=10, key='infraParams')

        self.mock_redis = await self.get_redis_conn(db=7, key='infraParams')

        self.modify_redis = await self.get_redis_conn(db=14, key='infraParams')

        self.logger = RedisStreamLogger(self.stream_redis, self.logging_stream)
        self.logger.added_packets.update({'consumer_group': self.consumer_group, 'consumer_name': self.consumer_name})
        
        self.db_mongo_client = await self.get_mongo_conn()
        self.raw_db = self.db_mongo_client['symphonyorder_raw'][f'orders_{str(datetime.date.today())}']
        await self.ping_redis.rpush('alarms-v2' , 'exec_client_start')
        # Create consumer group if it doesn't exist
        await self.create_consumer_group()
    
    async def create_consumer_group(self):
        try:
            await self.stream_redis.xgroup_create(
                self.input_stream, 
                self.consumer_group,
                id='0',  # Start from beginning of stream
                mkstream=True  # Create stream if it doesn't exist
            )
            logging.info(f"Created consumer group {self.consumer_group}")
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logging.info(f"Consumer group {self.consumer_group} already exists")
            else:
                raise

    async def close(self):
        """Close all connections."""
        self.is_running = False
        
        # Wait for all processing tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close broker client
        await self.broker_client.close()
        
        # Close Redis connection
        if self.sym_redis:
            await self.sym_redis.close()
        if self.master_redis:
            await self.master_redis.close()
        if self.stream_redis:   
            await self.stream_redis.close()
        if self.ping_redis:
            await self.ping_redis.close()
        if self.db_mongo_client:
            self.db_mongo_client.close()
        logging.info("Closed all connections")

    def parse_pending_post(self, order:Dict[str,Any]) -> Dict[str, Any]:

        return {
            'exchangeInstrumentID': order['exchangeInstrumentID'],
            'exchangeSegment': order['exchangeSegment'],
            'orderUniqueIdentifier': order['orderUniqueIdentifier'],
            # 'appOrderID': signal_feed['appOrderId'],
            'algoName': order['algoName'],
            'clientID': order['clientID'],
            'broker': order.get('broker'),
            'upperPriceLimit': order['upperPriceLimit'],
            'lowerPriceLimit': order['lowerPriceLimit'],
            'quantity': order['orderQuantity'],
            'timePeriod': order['timePeriod'],
            'orderType': order['orderType'],
            'orderSide': order['orderSide'],
            'extraPercent': order['extraPercent'],
            'startTime': time.time(),
            'exchangeInstrumentIDSell': order['exchangeInstrumentIDSell'] if order['exchangeInstrumentIDSell'] else 'None',
        }
    
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
            "signal": json.dumps(order_data),
            "client_id":  str(client_id) if client_id else "",
            "timestamp":  int(time.time() * 1000),
        }
        await self.stream_redis.xadd(self.error_stream, payload)
        await self.logger.warning(f"Error -> {self.error_stream}  stage={stage}  reason={reason}")

    async def process_order(self, order_id: str, order_data: Dict[str, Any]):
        """
        Process a single order: slice it and send to broker.

        Args:
            order_id: The Redis message ID
            order_data: The order data to process
        """
        async with self.processing_semaphore:
            t_start = time.monotonic()
            client_id = order_data.get('clientID')
            try:
                await self.logger.info(f"Processing order: {order_data.get('orderUniqueIdentifier', 'unknown')}")

                # ── Stage 1: masterfile lookup ────────────────────────────────
                t1 = time.monotonic()
                sym_id = order_data.get('exchangeInstrumentID')
                if not sym_id:
                    await self._push_error(
                        "order_data", order_data,
                        "Missing exchangeInstrumentID in order data", "masterfile_lookup", client_id,
                    )
                    return

                mfile = await self.master_redis.get(sym_id)
                if not mfile:
                    await self._push_error(
                        "order_data", order_data,
                        f"Masterfile entry missing for instrument '{sym_id}'", "masterfile_lookup", client_id,
                    )
                    return

                mfile: dict = json.loads(mfile)
                freeze_qty = int(mfile.get('FreezeQty', order_data.get('orderQuantity', 0)))
                lot_size   = int(mfile.get('LotSize', 1))
                slice_qty  = (freeze_qty // lot_size) * lot_size
                t1_ms = (time.monotonic() - t1) * 1000

                # ── Stage 2: symbol lookup ────────────────────────────────────
                t2 = time.monotonic()
                symbol = await self.get_symbol(order_data)
                if not symbol:
                    await self._push_error(
                        "order_data", order_data,
                        f"Symbol not found for instrument '{sym_id}'", "symbol_lookup", client_id,
                    )
                    return
                order_data['symbol'] = symbol
                order_data['actualPrice'] = await self.get_current_ltp(order_data)
                t2_ms = (time.monotonic() - t2) * 1000

                # ── Stage 3: order slicing ────────────────────────────────────
                t3 = time.monotonic()
                await self.logger.info(f'lot_size={lot_size}, freeze_qty={freeze_qty}, slice_qty={slice_qty}')
                order_slices = self.order_slicer.slice_order(order_data, slice_qty)
                if not order_slices:
                    await self._push_error(
                        "order_data", order_data,
                        "Order slicer returned empty result", "order_slice", client_id,
                    )
                    return
                await self.logger.info(f"Sliced order into {len(order_slices)} parts")
                t3_ms = (time.monotonic() - t3) * 1000

                # ── Stage 4: broker send ──────────────────────────────────────
                t4 = time.monotonic()
                broker = order_data.get('broker')
                token  = await self.token_redis.hgetall(f'token:{broker}:{client_id}')

                signal_feed_inserts = []
                for order_slice in order_slices:
                    if not self.is_mock:
                        await self.logger.info('Sending order to pending stream')
                        await self.modify_redis.hset('order_status:filled', order_slice['orderUniqueIdentifier'], '0')
                        pending_stream_insert = self.parse_pending_post(order_slice)
                        print("pending_stream_insert",pending_stream_insert)
                        await self.stream_redis.xadd(self.output_stream, pending_stream_insert)
                        try:
                            if self.config.get('params', 'use_throttler', fallback='false').lower() == 'true':
                                await self.broker_client.send_order_via_throttler(order_slice, token, self.stream_redis)
                            else:
                                signal_feed = await self.broker_client.send_order(order_slice, token)
                                app_order_id = signal_feed.get('appOrderId')
                                await self.modify_redis.hset('order_status:idmap', order_slice['orderUniqueIdentifier'], app_order_id)
                                signal_feed_inserts.append(signal_feed)
                        except Exception as e:
                            await self._push_error(
                                "order_data", order_slice,
                                str(e), "broker_send", client_id,
                            )
                            await self.logger.info(f'Setting order status to "Failed" for {order_slice["orderUniqueIdentifier"]}')
                            await self.modify_redis.hset('order_status:idmap', order_slice['orderUniqueIdentifier'], '0')
                            await self.modify_redis.hset('order_status:filled', order_slice['orderUniqueIdentifier'], '1')
                    else:
                        await self.logger.info(f"Order sent to dummyMock redis queue: {order_slice}")
                        app_order_id = str(random.randint(10**9, 10**10 - 1))
                        order_slice['AppOrderID'] = app_order_id
                        await self.mock_redis.rpush('dummyMock', json.dumps(order_slice))
                        await self.modify_redis.hset('order_status:idmap', order_slice['orderUniqueIdentifier'], app_order_id)
                        await self.modify_redis.hset('order_status:filled', order_slice['orderUniqueIdentifier'], '1')
                        pending_stream_insert = self.parse_pending_post(order_slice)
                        await self.stream_redis.xadd(self.output_stream, pending_stream_insert)
                        continue

                t4_ms = (time.monotonic() - t4) * 1000

                if signal_feed_inserts:
                    await self.raw_db.insert_many(signal_feed_inserts)
                    await self.logger.info(f"Inserted {len(signal_feed_inserts)} signal feeds into MongoDB")

                total_ms = (time.monotonic() - t_start) * 1000
                await self.logger.info(
                    f"TIMING order={order_data.get('orderUniqueIdentifier')} client={client_id} | "
                    f"s1_masterfile={t1_ms:.2f}ms s2_symbol={t2_ms:.2f}ms "
                    f"s3_slice={t3_ms:.2f}ms s4_broker={t4_ms:.2f}ms | total={total_ms:.2f}ms"
                )

            except Exception as e:
                await self.logger.exception(f"Unhandled error processing order: {str(e)}")
                await self._push_error(
                    "order_data", order_data,
                    str(e), "process_order", client_id,
                )

            finally:
                await self.stream_redis.xack(self.input_stream, self.consumer_group, order_id)
                await self.logger.debug(f"Order {order_data.get('orderUniqueIdentifier', 'unknown')} acknowledged")

    async def get_symbol(self, order_data: Dict[str, Any]) -> str:
        try: 
            sym_id = order_data.get('exchangeInstrumentID')
            segment = order_data.get('exchangeSegment')
            key = f'{sym_id}_{segment}'
            symbol = await self.sym_redis.get(key)
            return symbol
        except Exception as e:
            await self.logger.exception(f"Error getting symbol: {str(e)}")
            return None

    async def get_current_ltp(self, order_data: Dict[str, Any]) -> float:
        try:
            sym_id = order_data.get('exchangeInstrumentID')
            ltp = float(await self.sym_redis.get(sym_id))
            return ltp
        except Exception as e:
            raise Exception(f"Error getting current LTP: {str(e)}")

    async def is_exec_open(self, order_data: Dict[str, Any]) -> bool:
        if order_data['algoUniqueIdentifier'] == '0': return True
        client = order_data.get('clientID')
        algo = order_data.get('algoName')
        redis_key = f'client_action_map: {algo}'
        open_flag = await self.stream_redis.hget(redis_key, client)
        # await self.logger.info(f'Open flag for {client} and {algo}: {open_flag}')
        if open_flag == '1': return True
        else: return False
    
    async def listen_for_orders(self):
        """
        Main loop to listen for new orders on the Redis stream.
        """
        self.is_running = True
        
        await self.logger.info(f"Starting to listen on stream {self.input_stream}")
        group_info = await self.stream_redis.xinfo_groups(self.input_stream)
        await self.logger.info(f"Consumer group info: {group_info}")

        stream_info = await self.stream_redis.xinfo_stream(self.input_stream)
        await self.logger.info(f"Stream info: {stream_info}")

        while self.is_running:
            try:
                # Read new messages from the stream
                messages = await self.stream_redis.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    {self.input_stream: '>'},  # > means new messages only
                    count=10,  # Process 10 messages at a time
                    block=2000  # Block for 2 seconds if no messages
                )
                if not messages:
                    continue
                # Process each message
                for stream, stream_messages in messages:
                    await self.logger.info(f"Received {len(stream_messages)} messages from stream {stream}")
                    for message_id, fields in stream_messages:

                        
                        self.logger.added_packets.update({'order_id': message_id})

                        await self.logger.info(f"Message fields: {fields}")

                        # Step 1: extract actual order data
                        if 'payload' in fields:
                            try:
                                order_data = json.loads(fields['payload'])
                            except json.JSONDecodeError as e:
                                await self._push_error(
                                    "order_data", {},
                                    f"malformed JSON payload: {e}", "message_parse",
                                )
                                await self.stream_redis.xack(self.input_stream, self.consumer_group, message_id)
                                continue
                        elif 'data' in fields and isinstance(fields['data'], str):
                            try:
                                order_data = json.loads(fields['data'])
                            except json.JSONDecodeError as e:
                                await self._push_error(
                                    "order_data", {},
                                    f"malformed JSON in 'data' field: {e}", "message_parse",
                                )
                                await self.stream_redis.xack(self.input_stream, self.consumer_group, message_id)
                                continue
                        else:
                            order_data = fields

                        # Step 2: parse and validate fields
                        try:
                            order_data = self.parse_order_data(order_data)
                        except Exception as e:
                            await self._push_error(
                                "order_data", order_data if isinstance(order_data, dict) else {},
                                f"parse_order_data failed: {e}", "message_parse",
                            )
                            await self.stream_redis.xack(self.input_stream, self.consumer_group, message_id)
                            continue
                        
                        if not await self.is_exec_open(order_data):
                            logging.info(f"Execution is closed for order {order_data}")
                            await self.stream_redis.xack(self.input_stream, self.consumer_group, message_id)
                            continue

                        # Start processing task
                        task = asyncio.create_task(self.process_order(message_id, order_data))
                        self.tasks.add(task)
                        task.add_done_callback(self.tasks.discard)
                
            except aioredis.ResponseError as e:
                error_msg = str(e).upper()
                if any(keyword in error_msg for keyword in ['NOGROUP', 'NOSTREAM']):
                    await self.logger.error(f"Stream or consumer group missing: {str(e)}")
                    await self.logger.info("Attempting to recreate consumer group...")
                    try:
                        await self.create_consumer_group()
                        await self.logger.info("Consumer group recreated successfully")
                    except Exception as recreate_error:
                        await self.logger.exception(f"Failed to recreate consumer group: {recreate_error}")
                        # Consider breaking the loop or implementing exponential backoff
                        await asyncio.sleep(10)
                else:
                    await self.logger.exception(f"Redis response error: {str(e)}")
                    await asyncio.sleep(5)

            except asyncio.CancelledError:
                await self.logger.info("Order listener cancelled")
                break
                
            except Exception as e:
                await self.logger.exception(f"Error in order listener: {str(e)}")
                # Sleep briefly to prevent tight loop on persistent errors
                await asyncio.sleep(1)

    def parse_order_data(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        
        return {
            'clientID': order_data['clientID'], 
            'algoName': order_data['algoName'], 
            'exchangeSegment': order_data['exchangeSegment'], 
            'exchangeInstrumentID': int(order_data['exchangeInstrumentID']), 
            'exchangeInstrumentIDSell': int(order_data['exchangeInstrumentIDSell']) if ('exchangeInstrumentIDSell' in order_data) and (order_data['exchangeInstrumentIDSell'] != 'None') else None,
            'productType': order_data['productType'], 
            'orderType': order_data['orderType'], 
            'orderSide': order_data['orderSide'], 
            'timeInForce': order_data['timeInForce'], 
            'disclosedQuantity': int(order_data['disclosedQuantity']), 
            'orderQuantity': int(order_data['orderQuantity']), 
            'limitPrice': float(order_data['limitPrice']), 
            'stopPrice': float(order_data['stopPrice']), 
            'upperPriceLimit': float(order_data['upperPriceLimit']), 
            'lowerPriceLimit': float(order_data['lowerPriceLimit']), 
            'timePeriod': float(order_data['timePeriod']), 
            'extraPercent': float(order_data['extraPercent']), 
            'algoPrice': float(order_data['algoPrice']), 
            'algoTime': float(order_data['algoTime']), 
            'ltpExceedMaxValueCheck': int(order_data['ltpExceedMaxValueCheck']), 
            'orderUniqueIdentifier': order_data['orderUniqueIdentifier'],
            'algoUniqueIdentifier': order_data['algoSignalUniqueIdentifier'],
            'source': order_data.get('source', 'executor'),
            'broker':order_data.get('broker')
            }
    
    async def process_pending_orders(self):
        """
        Process any pending messages that weren't acknowledged.
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
                    
                    # Claim the message and process it
                    claimed = await self.stream_redis.xclaim(
                        self.input_stream,
                        self.consumer_group,
                        self.consumer_name,
                        min_idle_time=60 * 1000,  # 1 minute in milliseconds
                        message_ids=[message_id],
                        retrycount=2
                    )

                    if not claimed :
                        await asyncio.sleep(60)
                        continue

                    for c_message_id, fields in claimed:
                        if not c_message_id: continue
                        # Convert byte keys/values to strings
                        order_data = {k:v for k,v in fields.items()}
                        await self.logger.info(f"Claimed pending message: {c_message_id} with fields: {fields}")
                        # Parse JSON if stored as JSON string
                        if 'data' in order_data and isinstance(order_data['data'], str):
                            try:
                                order_data = json.loads(order_data['data'])
                            except json.JSONDecodeError:
                                await self.logger.error(f"Invalid JSON in pending message: {order_data['data']}")
                                await self.stream_redis.xack(self.input_stream, self.consumer_group, c_message_id)
                                continue
                            
                        if not await self.is_exec_open(order_data):
                            logging.info(f"Execution is closed for order {order_data}")
                            await self.stream_redis.xack(self.input_stream, self.consumer_group, c_message_id)
                            continue
                        # Process the order
                        order_data = self.parse_order_data(order_data)
                        task = asyncio.create_task(self.process_order(c_message_id, order_data))
                        self.tasks.add(task)
                        task.add_done_callback(self.tasks.discard)
                        logging.info(f'Acknowledging pending message {c_message_id} to prevent cascading failure')
                        await self.stream_redis.xack(self.input_stream, self.consumer_group, c_message_id)
                
            
            except Exception as e:
                await self.logger.exception(f"Error processing pending orders: {str(e)}")
                await asyncio.sleep(10)
    
    async def trim_stream(self, trim_interval: int = 60):
        """
        Periodically trim the Redis stream to prevent it from bloating.
        Only trims acknowledged messages, preserving unprocessed ones.

        Args:
            trim_interval: Time interval (in seconds) between trims.
        """
        while self.is_running:
            try:
                # await self.trim_acknowledged_messages()
                await self.stream_redis.xtrim(self.input_stream, maxlen=20000, approximate=True)
            except Exception as e:
                await self.logger.exception(f"Error in trim_stream: {str(e)}")
            
            # Wait for the specified interval before trimming again
            await asyncio.sleep(trim_interval)

    async def trim_acknowledged_messages(self):
        """
        Trim only acknowledged messages in the Redis stream.
        This keeps the stream size under control while ensuring 
        unacknowledged messages are preserved.
        """
        try:
            logging.info(f"Trimming acknowledged messages from {self.input_stream}")
            pending_info = await self.stream_redis.xpending(self.input_stream, self.consumer_group)
            last_pending_id = pending_info['min']
            if last_pending_id:
                # Fetch all IDs from 0 to the last_pending_id
                stream_messages = await self.stream_redis.xrange(self.input_stream, min='0', max=last_pending_id)
                stream_ids = [message[0] for message in stream_messages]
                logging.info(f"Stream IDs to trim: {stream_ids}")
                ids_to_trim = stream_ids[1:]
                for stream_id in ids_to_trim:
                    await self.stream_redis.xdel(self.input_stream, stream_id)
                logging.info(f"Trimmed {len(ids_to_trim)} acknowledged messages from {self.input_stream}")
            else:
                await self.stream_redis.xtrim(self.input_stream, maxlen=500, approximate=True)
                logging.info(f"Trimmed acknowledged messages from {self.input_stream} to keep size under control")
                
        except Exception as e:
            await self.logger.exception(f"Error trimming acknowledged messages from {self.input_stream}: {str(e)}")

    async def run(self):
        """
        Run the main execution engine.
        """
        await self.initialize()
        
        # Start the main listener and pending processor
        listener = asyncio.create_task(self.listen_for_orders())
        pending_processor = asyncio.create_task(self.process_pending_orders())
        stream_trimmer = asyncio.create_task(self.trim_stream())
        
        try:
            # Run until interrupted
            # await asyncio.gather(listener)
            await asyncio.gather(listener, pending_processor, stream_trimmer)
        finally:
            await self.close()


async def main():
    """Main entry point for the application."""
    # Configuration (should be loaded from environment or config file)
    config = ConfigParser()
    config.read('config.ini')

    payload = {
        'config': config,
        'input_stream': config['params']['input_stream'],
        'output_stream': config['params']['output_stream'],
        'logging_stream': config['params']['logging_stream'],
        'error_stream': config['params']['error_stream'],
        'broker_url': config['url']['broker_url'],
        'route': config['url']['order_route'],
        'processing_concurrency': int(config['params']['processing_concurrency']),
    }
    
    # Create and run the execution engine
    engine = TradeExecutionEngine(**payload)
    
    try:
        await engine.run()
    except KeyboardInterrupt:
        logging.info("Shutting down")
        await engine.close()


if __name__ == "__main__":
    asyncio.run(main())