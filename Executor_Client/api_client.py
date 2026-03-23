import json
import aiohttp
from typing import Dict, Any
from configparser import ConfigParser
import logging
import os
import datetime
import time


class BrokerAPIClient:
    """Client for interacting with the broker's API."""
    
    def __init__(self, api_url: str, route: str, config: ConfigParser):
        self.api_url = api_url
        self.route = route
        self.session = None
        self.config = config
        self.stream_name = self.config.get('params', 'throttler_stream',fallback='throttle_all_orders')

    
    async def initialize(self):
        """Initialize the HTTP session."""
        self.session = aiohttp.ClientSession()

    async def close(self):
        """Close the HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None

    def parse_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        
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
            'limitPrice': order['limitPrice'],
            'stopPrice': order['stopPrice'],
            'orderUniqueIdentifier': order['orderUniqueIdentifier'],
        }
    
    def get_signal_feed(self, order: Dict[str, Any], response_data: Dict[str, Any]) -> Dict[str, Any]:
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
            "timestamp": str(datetime.datetime.now().strftime("%H:%M:%S")),
            "symbol": order['symbol'],
            "initial_timestamp": order['order_sent_time'],
            "order_confirmed_timestamp": time.time(),
            "initial_price": order['limitPrice'],
            "actual_price": order['actualPrice'],
            "mod_status": "Placed",
            "appOrderId": response_data['result']['AppOrderID'],   
            "algoPrice": order.get('algoPrice',order['actualPrice']),
            "algoTime": order.get('algoTime',order['order_sent_time']),
            "source": order.get('source', 'executor')
        }  
    

    async def send_order(self, order: Dict[str, Any], token: dict) -> Dict[str, Any]:
        
        if not self.session:
            await self.initialize()

        try:
            if not token:
                raise Exception(f"Token not found for client {order['clientID']}.")
            final_order = self.parse_order(order)
            if token['type'].lower() == 'pro': final_order['clientID'] = '*****'
            headers = {
                'Content-Type': 'application/json',
                'Authorization': token['token']
            }
            order['order_sent_time'] = time.time()
            endpoint = token.get('endpoint', self.api_url)
            suffix = token.get('suffix', '')
            final_url = f"{endpoint}/interactive{suffix}/{self.route}"
            async with self.session.post(
                final_url, 
                json=final_order,
                headers=headers
            ) as response:
                order_status = response.status
                order_text = await response.text()

                if response.status != 200:
                    logging.exception(f"Order failed: {order_status} - {order_text}")
                    raise Exception(f"Order failed: {order_status} - {order_text}")
                    
                response_data = await response.json()
                logging.info(f"Order sent successfully: {order['orderUniqueIdentifier']}")
                logging.info(f"Response: {response_data}")
        
                signal_feed = self.get_signal_feed(order, response_data)
            
            return signal_feed
                
        except Exception as e:
            logging.exception(f"Error sending order {order}: {str(e)}")
            raise e


    async def send_order_via_throttler(self, order: Dict[str, Any], token: dict, redis) -> Dict[str, Any]:
        if not self.session:
            await self.initialize()

        try:
            if not token:
                raise Exception(f"Token not found for client {order['clientID']}.")

            headers = {
                'Content-Type': 'application/json',
                'Authorization': token['token']
            }
            order['order_sent_time'] = time.time()
            endpoint = token.get('endpoint', self.api_url)
            suffix = token.get('suffix', '')
            final_url = f"{endpoint}/{suffix}/{self.route}"

            # Publish HTTP request details to Redis Stream
            stream_data = {
                "type": "order",
                "method": "POST",
                "url": final_url,
                "headers": json.dumps(headers),
                "body": json.dumps(order),
                "orderUniqueIdentifier": order['orderUniqueIdentifier'],
            }

            message_id = await redis.xadd(self.stream_name, stream_data, maxlen=10000, approximate=True)

            logging.info(
                f"Order sent successfully: {order['orderUniqueIdentifier']}")
            logging.info(
                f"Published to Redis stream with message ID: {message_id}")

        except Exception as e:
            logging.exception(f"Error sending order {order}: {str(e)}")
            raise e
