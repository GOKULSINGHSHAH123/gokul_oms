import aiohttp
from typing import Dict, Any
import logging
import os
import datetime
import time


class BrokerAPIClient:
    """Client for interacting with the broker's API."""
    
    def __init__(self, api_url: str, order_prefix: str):
        self.api_url = api_url
        self.order_prefix = order_prefix
        self.session = None
    
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
            "orderSentTime": time.time(),
            "responseFlag": False,
            "leaves_quantity": order['orderQuantity'],
            "timestamp": str(datetime.datetime.now().strftime("%H:%M:%S")),
            "symbol": order['symbol'],
            "initial_timestamp": time.time(),
            "initial_price": order['limitPrice'],
            "actual_price": order['actualPrice'],
            "mod_status": "Placed",
            "appOrderId": response_data['result']['AppOrderID'],   
            "algoPrice": order.get('algoPrice',order['actualPrice']),
            "algoTime": order.get('algoTime',time.time()),
        }  
    

    async def send_order(self, order: Dict[str, Any], tokenMap:dict) -> Dict[str, Any]:
    
        if not self.session:
            await self.initialize()
            
        try:
            if order['clientID'] not in tokenMap:
                raise Exception(f"Client ID {order['clientID']} not found in token map.")
            token = tokenMap[order['clientID']]
            final_order = self.parse_order(order)
            if token['type'].lower() == 'pro': final_order['clientID'] = '*****'
            headers = {
                'Content-Type': 'application/json',
                'Authorization': token['token']
            }

            async with self.session.post(
                f"{self.api_url}{self.order_prefix}", 
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

