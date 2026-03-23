import redis
import json

# Redis config
REDIS_HOST = "101.53.133.122"
REDIS_PORT = 6379
REDIS_PASS = "Algo_infra_stock_options"

# Your data
segment = "NSEFO"
instrument_id = 100145

# Connect to Redis DB 9 and DB 12
redis_db9 = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASS,
    db=9,
    decode_responses=True
)

redis_db12 = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASS,
    db=12,
    decode_responses=True
)

def fetch_ltp():
    json_data = {
        "exchangeSegment": segment,
        "exchangeInstrumentID": instrument_id
    }

    # Push to XTS subscription
    redis_db9.lpush("subscribe:xts", json.dumps(json_data))

    # Push to Kite subscription
    redis_db12.lpush("subscribe:kite", instrument_id)

    print("Data pushed successfully!")

# Run
if __name__ == "__main__":
    fetch_ltp()