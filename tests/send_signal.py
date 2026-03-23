import redis
import time
import argparse
import pymongo
from datetime import datetime

# Redis connection
redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=14,
    password="RMS_TESTING_AWS_DABBA",
    decode_responses=True
)

redis_data = redis.Redis(
    host="101.53.133.122",
    port=6379,
    db=0,
    password="Algo_infra_stock_options",
    decode_responses=True
)

# MongoDB connection
mongo_uri = f"mongodb://mudraksh:NewStrongPassword@localhost:27017/"
mongo_client = pymongo.MongoClient(mongo_uri)

algo_status_db = mongo_client["algo_status"]
info_db = mongo_client["Info"]


def get_symbol(exchangeInstrumentID, exchangeSegment):
    symbol_key = f"{exchangeInstrumentID}_{exchangeSegment}"
    return redis_data.get(symbol_key)


def generate_signal(exchangeSegment,
                    exchangeInstrumentID,
                    orderType,
                    orderSide,
                    orderQuantity,
                    limitPrice,
                    algoName):

    signal_data = {
        "exchangeSegment": exchangeSegment,
        "exchangeInstrumentID": exchangeInstrumentID,
        "productType": "NRML",
        "orderType": orderType,
        "orderSide": orderSide,
        "timeInForce": "DAY",
        "disclosedQuantity": "0",
        "orderQuantity": str(orderQuantity),
        "limitPrice": str(limitPrice),
        "stopPrice": "0",
        "upperPriceLimit": "0",
        "lowerPriceLimit": "0",
        "algoOrderNum": str(int(time.time()*1000)),
        "timePeriod": "60",
        "extraPercent": "0.5",
        "algoName": algoName,
        "algoPrice": str(limitPrice),
        "algoTime": str(time.time())
    }

    return signal_data


def send_signal_to_stream(signal):
    redis_client.xadd(
        name="Algo_Signal",
        fields=signal
    )


def insert_position(symbol, algoName, quantity, limitPrice, mpName="process_1"):
    now = str(datetime.now())
    price = float(limitPrice)
    algo_status_db["algo_position"].insert_one({
        "Symbol": symbol,
        "EntryPrice": price,
        "CurrentPrice": price,
        "Quantity": int(quantity),
        "PositionStatus": 1,
        "Pnl": 0.0,
        "EntryTime": now,
        "mpName": mpName,
        "AlgoName": algoName
    })


def insert_message(algoName, message="Signal sent"):
    algo_status_db["algo_message"].insert_one({
        "algoName": algoName,
        "type": "info",
        "message": message,
        "time_stamp": str(datetime.now())
    })


def insert_algo_signal(exchangeSegment, exchangeInstrumentID, orderType, orderSide,
                        orderQuantity, limitPrice, algoName, symbol):
    now = datetime.now()
    algo_time = int(time.time())
    order_unique_id = f"ORDER_{algo_time}"

    signal_doc = {
        "orderUniqueIdentifier": order_unique_id,
        "exchangeInstrumentID": int(exchangeInstrumentID),
        "exchangeSegment": exchangeSegment,
        "orderQuantity": int(orderQuantity),
        "orderSide": orderSide,
        "orderType": orderType,
        "productType": "NRML",
        "limitPrice": float(limitPrice),
        "initial_price": float(limitPrice),
        "algoName": algoName,
        "algoPrice": float(limitPrice),
        "actual_price": float(limitPrice),
        "algoTime": algo_time,
        "initial_timestamp": time.time(),
        "orderSentTime": time.time(),
        "symbol": symbol,
        "source": "send_order",
        "timestamp": now.strftime("%H:%M:%S"),
        "timeInForce": "DAY",
        "disclosedQuantity": 0,
        "stopPrice": 0,
        "mod_status": "Placed",
        "responseFlag": False,
        "leaves_quantity": int(orderQuantity),
        "sentRequest": False,
        "doc_date": now.strftime("%Y-%m-%d"),
        "orderStatus": "Unresolved",
    }

    info_db["algo_signals"].insert_one(signal_doc)
    return signal_doc


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--exchangeSegment", required=True)
    parser.add_argument("--exchangeInstrumentID", required=True)
    parser.add_argument("--orderType", required=True)
    parser.add_argument("--orderSide", required=True)
    parser.add_argument("--orderQuantity", required=True)
    parser.add_argument("--limitPrice", required=True)
    parser.add_argument("--algoName", required=True)
    parser.add_argument("--mpName", default="process_1")

    args = parser.parse_args()

    signal = generate_signal(
        args.exchangeSegment,
        args.exchangeInstrumentID,
        args.orderType,
        args.orderSide,
        args.orderQuantity,
        args.limitPrice,
        args.algoName
    )

    send_signal_to_stream(signal)
    print("Signal pushed to Redis stream:", signal)

    symbol = get_symbol(args.exchangeInstrumentID, args.exchangeSegment)
    if symbol is None:
        print(f"Warning: symbol not found in redis_data for key {args.exchangeInstrumentID}_{args.exchangeSegment}")
        symbol = args.exchangeInstrumentID

    insert_position(symbol, args.algoName, args.orderQuantity, args.limitPrice, args.mpName)
    print(f"Position inserted to algo_status.algo_position: {symbol}")

    insert_message(args.algoName, f"{args.orderSide} signal sent for {symbol} @ {args.limitPrice}")
    print(f"Message inserted to algo_status.algo_message")

    # signal_doc = insert_algo_signal(
    #     args.exchangeSegment,
    #     args.exchangeInstrumentID,
    #     args.orderType,
    #     args.orderSide,
    #     args.orderQuantity,
    #     args.limitPrice,
    #     args.algoName,
    #     symbol
    # )
    # print(f"Signal inserted to Info.algo_signals: {signal_doc['orderUniqueIdentifier']}")