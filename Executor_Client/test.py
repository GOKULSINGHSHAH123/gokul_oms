from redis import Redis
from configparser import ConfigParser
import time
from secrets import token_hex

def test(message):
    # Load configuration
    config = ConfigParser()
    config.read('config.ini')

    # Get Redis connection details
    rhost = config['infraParams']['redisHost']
    rport = int(config['infraParams']['redisPort'])
    rpass = config['infraParams']['redisPass']
    stream_name = config['params']['input_stream']
    # Connect to Redis
    redis_client = Redis(host=rhost, port=rport, password=rpass, db=12, decode_responses=True)

    # Store the message in Redis
    try: redis_client.xgroup_create(stream_name, 'Exec_Client', id='0', mkstream=True)
    except: pass

    return redis_client.xadd(stream_name, message)

if __name__ == '__main__':

    message = {
        'exchangeSegment': 'BSEFO', 
        'exchangeInstrumentID': '1100515', 
        'productType': 'NRML', 
        'orderType': 'BuyThenSell', 
        'orderSide': 'BUY', 
        'timeInForce': 'DAY', 
        'disclosedQuantity': '0', 
        'orderQuantity': '20', 
        'limitPrice': '40.2', 
        'stopPrice': '0', 
        'upperPriceLimit': '60.14', 
        'lowerPriceLimit': '0', 
        'algoOrderNum': '143813', 
        'timePeriod': '4', 
        'extraPercent': '0.1', 
        'exchangeInstrumentIDSell': '889143', 
        'algoName': '2BHLCC_SS_ExT', 
        'algoPrice': '40.3', 
        'algoTime': f'{time.time()}', 
        'algoSignalUniqueIdentifier': f'{int(time.time())}{token_hex(4)}', 
        'ltpExceedMaxValueCheck': '0', 
        'clientID': 'D7730024', 
        'orderUniqueIdentifier': f'{int(time.time())}{token_hex(4)}'
        }

    test(message)
    # for _ in range(10000): test(message)

