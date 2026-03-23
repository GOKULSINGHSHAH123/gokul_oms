# gokul_oms — Order Management System Documentation

---

## Table of Contents
1. [System Overview](#1-system-overview)
2. [Module Reference — Inputs, Outputs & Schemas](#2-module-reference--inputs-outputs--schemas)
   - [pipeline/firewall.py](#21-pipelinefirewallpy)
   - [pipeline/sync.py](#22-pipelinesyncpy)
   - [services/algo_service.py](#23-servicesalgo_servicepy)
   - [services/client_service.py](#24-servicesclient_servicepy)
   - [services/segment_service.py](#25-servicessegment_servicepy)
   - [Executor_Client/exec_client.py](#26-executor_clientexec_clientpy)
   - [Executor_Client/api_client.py](#27-executor_clientapi_clientpy)
   - [Executor_Client/slicer.py](#28-executor_clientslicerpy)
   - [Executor_Error/exec_error.py](#29-executor_errorexec_errorpy)
   - [executor_throttler/core/distributor.py](#210-executor_throttlercoredistributorpy)
   - [executor_throttler/core/throttler.py](#211-executor_throttlercorethrottlerpy)
3. [Redis & MongoDB Reference](#3-redis--mongodb-reference)
4. [Full System Flow](#4-full-system-flow)

---

## 1. System Overview

**gokul_oms** is a distributed, high-throughput Order Management System (OMS) for algorithmic trading.

It does three things:
1. **Validate** — Incoming algo signals pass through a 4-stage firewall (field check → algo RMS → segment check → client fan-out)
2. **Route** — Valid orders are distributed to per-client priority queues via a throttler
3. **Execute** — Orders are sent to the broker REST API with rate limiting, order slicing, and error handling

**Key technologies**: Redis Streams, MongoDB, async Python (uvloop + aioredis + motor), multiprocessing, PM2

---

## 2. Module Reference — Inputs, Outputs & Schemas

---

### 2.1 `pipeline/firewall.py`

**Role**: Central signal processor. The entry point for all algo trade signals. Runs as N independent worker processes (default 2), each consuming from a Redis consumer group.

---

#### INPUT — Redis Stream `Algo_Signal` (DB 13)

Each message on the stream contains either a `payload` field (JSON string) or flat fields.

**Signal Schema**:

| Field | Type | Required | Description |
|---|---|---|---|
| `exchangeSegment` | string | YES | Market segment: `NSEFO`, `NSECM`, `MCXFO`, `BSEFO`, etc. |
| `exchangeInstrumentID` | string | YES | Unique instrument token (e.g. `"66647"`) |
| `productType` | string | YES | `NRML`, `MIS`, `CNC` |
| `orderType` | string | YES | `LIMIT`, `MARKET`, `SL`, `SL-M` |
| `orderSide` | string | YES | Must be exactly `BUY` or `SELL` |
| `orderQuantity` | string (int) | YES | Must be > 0. For MCXFO: divided by instrument Multiplier before processing |
| `limitPrice` | string (float) | YES | Must be ≥ 0 |
| `stopPrice` | string (float) | YES | Stop price (0 for non-SL orders) |
| `algoName` | string | YES | Strategy name — must exist in `algo_configs` cache |
| `extraPercent` | string (float) | YES | Slippage buffer percentage |
| `algoTime` | string (epoch) | YES | Unix timestamp of signal origination |

**Example**:
```json
{
  "exchangeSegment":      "NSEFO",
  "exchangeInstrumentID": "66647",
  "productType":          "NRML",
  "orderType":            "LIMIT",
  "orderSide":            "BUY",
  "orderQuantity":        "300",
  "limitPrice":           "150.0",
  "stopPrice":            "0",
  "algoName":             "TestAlgo",
  "extraPercent":         "0.5",
  "algoTime":             "1718000000"
}
```

---

#### PROCESSING — 4 Stages

Each stage either passes the signal forward or pushes an error record and stops processing.

**Stage 1 — `signal_field_check` (pure in-memory)**
- Checks all 11 required fields exist and are not `None`/`""`/`"None"`
- Validates `orderQuantity` is a positive number
- Validates `limitPrice` is ≥ 0
- Validates `orderSide` is exactly `"BUY"` or `"SELL"`
- Validates `algoName` has at least 2 characters
- Special: For `exchangeSegment == "MCXFO"`, divides `orderQuantity` by `Multiplier` from the MASTERFILE before proceeding

**Stage 2 — `algo_check` (reads Redis DB 11 on instrument miss, writes counters to Redis DB 12)**
- Looks up instrument in 2-level cache (memory → Redis DB 11 MASTERFILE)
- Validates `exchangeSegment` matches the masterfile's `ExchangeSegment`
- Loads `algo_cfg` from in-memory cache (synced from Redis DB 12 every 3s)
- Checks algo is active (`status == True`)
- Checks segment is in `allowed_segments`
- Validates `orderQuantity` is a multiple of `LotSize` (from masterfile or algo config)
- Checks per-trade lot limit (`per_trade_lot_limit`)
- Checks max order value (`max_value_of_symbol_check` = `quantity × limitPrice`)
- **Atomic rate check**: Lua script on Redis DB 12 key `trade_count_second:{algoName}:{epoch}` (TTL 2s)
- Increments `trade_count_day:{algoName}` and checks `trade_limit_per_day`
- Increments `lot_count_day:{algoName}` and checks `daily_lot_limit`

**Stage 3 — `segment_check` (reads Redis DB 12 config, DB 9 market data)**
- Loads `seg_cfg` from in-memory cache
- For **derivatives** (`OPTIDX`, `OPTSTK`, `FUTIDX`, `FUTSTK`):
  - Checks OI from Redis DB 9 key `OI_LATEST` ≥ `oi_threshold`
  - Checks bid-ask spread from Redis DB 9 keys `bid:{id}` ≤ `max_spread_pct`
- For **equity** (`EQ`): checks instrument is in Redis DB 2 `quality_stocks` set
- *(Note: derivative/equity sub-checks are currently commented out in code — stage passes unconditionally)*

**Stage 4 — `client_check_and_fanout` (reads memory cache, writes to Redis DB 14)**
- Looks up `client_ids` from `algo_clients[algoName]` (in-memory)
- For each client, in parallel via `ThreadPoolExecutor` (max 20 threads):
  - Loads `client_rms` from `client_rms[{clientID}:{algoName}]` (in-memory)
  - Applies `quantity_multiplier`: `new_qty = int(original_qty × multiplier)`
  - Generates `orderUniqueIdentifier = "{algoName}_{uuid4().hex}"`
  - Assembles broker payload (see output schema below)
  - Pushes to Redis Stream `Client_API_Place_Order` (DB 14)

---

#### OUTPUT A — Redis Stream `Client_API_Place_Order` (DB 14) — on success

**Broker Payload Schema** (written as `{"payload": "<JSON string>"}`):

| Field | Type | Source |
|---|---|---|
| `orderUniqueIdentifier` | string | Generated: `"{algoName}_{uuid4().hex}"` |
| `algoName` | string | From signal |
| `clientID` | string | From `algo_clients` cache |
| `exchangeInstrumentID` | string | From signal |
| `exchangeSegment` | string | From signal |
| `orderSide` | string | From signal |
| `orderQuantity` | int | `original_qty × quantity_multiplier` |
| `limitPrice` | float | From signal |
| `stopPrice` | float | From signal (default 0) |
| `orderType` | string | From signal |
| `productType` | string | From signal (default `"NRML"`) |
| `timeInForce` | string | From signal (default `"DAY"`) |
| `disclosedQuantity` | int | From signal (default 0) |
| `upperPriceLimit` | float | From signal (default 0) |
| `lowerPriceLimit` | float | From signal (default 0) |
| `timePeriod` | int | From signal (default 0) |
| `extraPercent` | float | From signal |
| `algoPrice` | float | From signal `algoPrice` or `limitPrice` |
| `algoTime` | float | From signal |
| `ltpExceedMaxValueCheck` | int | From signal (default 0) |
| `algoSignalUniqueIdentifier` | string | From signal (default `"0"`) |
| `source` | string | Hardcoded `"signal_engine"` |
| `broker` | string | From `client_rms` cache |
| `timestamp` | int (ms epoch) | `int(time.time() × 1000)` |

**Example**:
```json
{
  "orderUniqueIdentifier": "TestAlgo_a3f9c2d1e5b6...",
  "algoName": "TestAlgo",
  "clientID": "client123",
  "exchangeInstrumentID": "66647",
  "exchangeSegment": "NSEFO",
  "orderSide": "BUY",
  "orderQuantity": 150,
  "limitPrice": 150.0,
  "stopPrice": 0,
  "orderType": "LIMIT",
  "productType": "NRML",
  "timeInForce": "DAY",
  "disclosedQuantity": 0,
  "upperPriceLimit": 0,
  "lowerPriceLimit": 0,
  "timePeriod": 0,
  "extraPercent": 0.5,
  "algoPrice": 150.0,
  "algoTime": 1718000000,
  "ltpExceedMaxValueCheck": 0,
  "algoSignalUniqueIdentifier": "0",
  "source": "signal_engine",
  "broker": "NIRMALBANG",
  "timestamp": 1718000000000
}
```

---

#### OUTPUT B — Redis Stream `Error_Stream` (DB 13) — on any stage failure

**Error Record Schema** (flat fields, not JSON-encoded):

| Field | Type | Description |
|---|---|---|
| `error_type` | string | `"AlgoSignalValidationError"` or `"ClientOrderValidationError"` |
| `stage` | string | `"signal_field_check"`, `"algo_check"`, `"segment_check"`, `"client_check"` |
| `reason` | string | Human-readable failure reason |
| `signal` | string (JSON) | Original signal serialized as JSON string |
| `client_id` | string | Client ID if failure is client-specific, else `""` |
| `timestamp` | int (ms epoch) | `int(time.time() × 1000)` |

**Example**:
```json
{
  "error_type": "AlgoSignalValidationError",
  "stage":      "algo_check",
  "reason":     "Algo 'GhostAlgo' is inactive (status=false)",
  "signal":     "{\"exchangeSegment\":\"NSEFO\",...}",
  "client_id":  "",
  "timestamp":  1718000000000
}
```

---

#### Internal Config Cache (read-only in firewall, written by sync.py)

The firewall reads four in-memory tables refreshed from Redis DB 12 every 3 seconds:

**`algo_configs` hash** — key: `algo_name`
```json
{
  "status":                    true,
  "trade_limit_per_second":    5,
  "trade_limit_per_day":       1000,
  "allowed_segments":          ["NSEFO", "NSECM"],
  "lot_size":                  1,
  "lot_size_multiple":         1,
  "per_trade_lot_limit":       50,
  "daily_lot_limit":           500,
  "max_value_of_symbol_check": 500000
}
```

**`algo_clients` hash** — key: `algo_name`
```json
["client001", "client002", "client003"]
```

**`client_rms` hash** — key: `"{clientID}:{algoName}"`
```json
{
  "quantity_multiplier": 0.5,
  "broker": "NIRMALBANG"
}
```

**`segment_rms_check` hash** — key: `segment_name`
```json
{
  "oi_threshold":   50000,
  "max_spread_pct": 2.5
}
```

---

### 2.2 `pipeline/sync.py`

**Role**: Background daemon — the **sole writer** to Redis DB 12. Syncs all RMS config from MongoDB to Redis every 2 seconds.

#### INPUT — MongoDB (`Info` database)
- `algo_rms_params` collection (via `algo_service.build_cache`)
- `client_rms_params` collection (via `client_service.build_cache`)
- `segment_rms_params` collection (via `segment_service.make_cache_in_redis`)

#### OUTPUT — Redis DB 12 (CONFIG_CACHE)

| Redis Key | Type | Built By |
|---|---|---|
| `algo_configs` | Hash | `algo_service.build_cache()` |
| `algo_clients` | Hash | `client_service.build_cache()` |
| `client_rms` | Hash | `client_service.build_cache()` |
| `segment_rms_check` | Hash | `segment_service.make_cache_in_redis()` |

**Behaviour**:
- Runs in infinite loop, sleeps 2 seconds between cycles
- Errors in one cycle are logged but don't stop the loop
- Redis DB 12 can be safely flushed at any time — repopulates within 2 seconds

---

### 2.3 `services/algo_service.py`

**Role**: CRUD for algo-level RMS configuration + Redis cache builder.

#### INPUT — MongoDB `Info.algo_rms_params` document schema:

| Field | Type | Description |
|---|---|---|
| `algo_name` | string | Primary key |
| `is_active` | bool | Whether the algo is live |
| `max_trade_limit_per_sec` | int | Max trades/second (0 = unlimited) |
| `max_trade_limit_per_day` | int | Max trades/day (0 = unlimited) |
| `allowed_segments` | list[string] | Permitted segments, e.g. `["NSEFO"]` |
| `lot_size` | int | Minimum lot size |
| `lot_size_multiple` | int | Quantity must be a multiple of this |
| `max_order_lot_qty` | int | Max lots per trade |
| `max_net_lot_qty` | int | Max lots per day |
| `max_order_value` | float | Max order value in rupees |

#### OUTPUT — Redis DB 12 Hash `algo_configs`

Key: `algo_name` → Value: JSON with renamed fields:

| MongoDB field | Redis field |
|---|---|
| `is_active` | `status` |
| `max_trade_limit_per_sec` | `trade_limit_per_second` |
| `max_trade_limit_per_day` | `trade_limit_per_day` |
| `allowed_segments` | `allowed_segments` |
| `lot_size` | `lot_size` |
| `lot_size_multiple` | `lot_size_multiple` |
| `max_order_lot_qty` | `per_trade_lot_limit` |
| `max_net_lot_qty` | `daily_lot_limit` |
| `max_order_value` | `max_value_of_symbol_check` |

---

### 2.4 `services/client_service.py`

**Role**: CRUD for per-client RMS parameters + Redis cache builder.

#### INPUT — MongoDB `Info.client_rms_params` document schema:

| Field | Type | Description |
|---|---|---|
| `client_id` | string | Client identifier |
| `algo_name` | string | Algo this config applies to |
| `quantity_multiplier` | float | Scale factor applied to signal quantity at fan-out |
| `broker` | string | Broker name (e.g. `"NIRMALBANG"`) |

#### OUTPUT — Redis DB 12

**Hash `client_rms`** — key: `"{client_id}:{algo_name}"`:
```json
{
  "quantity_multiplier": 0.5,
  "broker": "NIRMALBANG"
}
```

**Hash `algo_clients`** — key: `algo_name`:
```json
["client001", "client002", "client003"]
```

---

### 2.5 `services/segment_service.py`

**Role**: CRUD for segment-level RMS rules + Redis cache builder.

#### INPUT — MongoDB `Info.segment_rms_params` document schema:

| Field | Type | Description |
|---|---|---|
| `segment` | string | Segment name: `NSEFO`, `NSECM`, `MCXFO`, etc. |
| `oi` | float | Minimum Open Interest required for derivatives |
| `spread` | float | Maximum bid-ask spread percentage for derivatives |

#### OUTPUT — Redis DB 12 Hash `segment_rms_check`

Key: `segment_name` → Value:
```json
{
  "oi_threshold":   50000.0,
  "max_spread_pct": 2.5
}
```

---

### 2.6 `Executor_Client/exec_client.py`

**Role**: Async consumer that takes broker payloads from the stream and executes them against the broker API.

#### INPUT — Redis Stream `Client_API_Place_Order` (DB 14)

Each message is the broker payload produced by `firewall.py` Stage 4 (see schema in [2.1 Output A](#output-a--redis-stream-client_api_place_order-db-14--on-success)).

---

#### PROCESSING — 4 Stages

**Stage 1 — Masterfile lookup** (Redis DB 11)
- Looks up instrument by `exchangeInstrumentID` key in Redis DB 11
- Extracts `FreezeQty` and `LotSize`
- Computes: `slice_qty = (FreezeQty // LotSize) × LotSize`

**Stage 2 — Symbol lookup** (Redis DB 0)
- Reads symbol string from Redis DB 0 key `{exchangeInstrumentID}_{exchangeSegment}`
- Also fetches current LTP from Redis DB 0 key `{exchangeInstrumentID}`

**Stage 3 — Order slicing** (`slicer.py`)
- If `orderQuantity ≤ slice_qty`: single order, no slicing
- If `orderQuantity > slice_qty`: split into N slices, each with `orderUniqueIdentifier` suffixed `_1`, `_2`, ... `_N` (truncated to last 20 chars)

**Stage 4 — Broker send** (HTTP or throttler stream)
- Reads auth token from Redis DB 10 key `token:{broker}:{clientID}` (hash: `token`, `type`, `endpoint`, `suffix`)
- If `use_throttler=true`: publishes to `throttle_all_orders` stream (Redis DB 14)
- If `use_throttler=false`: HTTP POST directly to `{endpoint}/interactive{suffix}/orders`
- On success: inserts `signal_feed` record into MongoDB `symphonyorder_raw.orders_{date}`
- On error: pushes to `Error_Stream` and marks order status as failed

---

#### OUTPUT A — MongoDB `symphonyorder_raw.orders_{YYYY-MM-DD}`

**Signal Feed Schema** (inserted on successful broker response):

| Field | Type | Description |
|---|---|---|
| `exchangeSegment` | string | Market segment |
| `exchangeInstrumentID` | string | Instrument token |
| `productType` | string | `NRML`/`MIS`/`CNC` |
| `orderType` | string | `LIMIT`/`MARKET`/etc. |
| `orderSide` | string | `BUY`/`SELL` |
| `timeInForce` | string | `DAY`/`IOC` |
| `disclosedQuantity` | int | Disclosed qty |
| `orderQuantity` | int | Slice quantity sent |
| `limitPrice` | float | Limit price |
| `stopPrice` | float | Stop price |
| `orderUniqueIdentifier` | string | Slice-level unique ID |
| `clientID` | string | Client ID |
| `algoName` | string | Algo name |
| `orderSentTime` | float | Unix epoch when order was dispatched |
| `responseFlag` | bool | Always `False` (set to `True` by Gyan on fill/cancel) |
| `leaves_quantity` | int | Equals `orderQuantity` at insertion (Gyan updates on partial fill) |
| `timestamp` | string | `HH:MM:SS` |
| `symbol` | string | Trading symbol string |
| `initial_timestamp` | float | Same as `orderSentTime` |
| `order_confirmed_timestamp` | float | Unix epoch of broker response |
| `initial_price` | float | `limitPrice` |
| `actual_price` | float | LTP fetched from Redis at execution time |
| `mod_status` | string | `"Placed"` |
| `appOrderId` | string | Broker-assigned order ID from response |
| `algoPrice` | float | Original algo price |
| `algoTime` | float | Original algo timestamp |
| `source` | string | `"signal_engine"` or `"executor"` |

#### OUTPUT B — Redis Stream `Error_Stream` (DB 13) — on failure

Same error record schema as firewall (see [2.1 Output B](#output-b--redis-stream-error_stream-db-13--on-any-stage-failure)).

#### OUTPUT C — Redis Stream `Pending_Order_Stream` (DB 14)

On each slice sent, a pending tracking record is pushed:

| Field | Description |
|---|---|
| `exchangeInstrumentID` | Instrument token |
| `exchangeSegment` | Segment |
| `orderUniqueIdentifier` | Slice unique ID |
| `algoName` | Algo name |
| `clientID` | Client ID |
| `upperPriceLimit` | Upper price limit |
| `lowerPriceLimit` | Lower price limit |
| `quantity` | Slice quantity |
| `timePeriod` | Time period |
| `orderType` | Order type |
| `orderSide` | BUY/SELL |
| `extraPercent` | Slippage % |
| `startTime` | `time.time()` when dispatch started |
| `exchangeInstrumentIDSell` | Sell-side instrument if applicable |

#### OUTPUT D — Redis DB 14 Hashes (order tracking)

| Key | Field | Value |
|---|---|---|
| `order_status:filled` | `orderUniqueIdentifier` | `"0"` = pending, `"1"` = done |
| `order_status:idmap` | `orderUniqueIdentifier` | `appOrderID` from broker |

---

### 2.7 `Executor_Client/api_client.py`

**Role**: HTTP client that serialises orders and communicates with the broker REST API.

#### INPUT — Order dict (from `exec_client.py` per slice)

Fields used for the HTTP request body (`parse_order`):

| Field | Transformation |
|---|---|
| `clientID` | Masked as `*****` if `token['type'] == 'pro'` |
| `exchangeSegment` | As-is |
| `exchangeInstrumentID` | As-is |
| `productType` | As-is |
| `orderType` | `"buythensell"` → `"LIMIT"`, else as-is |
| `orderSide` | As-is |
| `timeInForce` | As-is |
| `disclosedQuantity` | As-is |
| `orderQuantity` | As-is |
| `limitPrice` | As-is |
| `stopPrice` | As-is |
| `orderUniqueIdentifier` | As-is |

#### INPUT — Auth token (from Redis DB 10 hash `token:{broker}:{clientID}`)

| Field | Description |
|---|---|
| `token` | Bearer token for `Authorization` header |
| `type` | `"pro"` or `"retail"` |
| `endpoint` | Broker API base URL |
| `suffix` | URL suffix for routing |

#### HTTP Request

```
POST {endpoint}/interactive{suffix}/orders
Headers:
  Content-Type: application/json
  Authorization: {token}
Body: parsed order JSON
```

#### OUTPUT — Broker JSON Response

```json
{
  "result": { "AppOrderID": "123456789" },
  "status": 200,
  "message": "Order placed successfully"
}
```

Non-200 responses raise an exception which is caught by `exec_client.py` and pushed to `Error_Stream`.

---

### 2.8 `Executor_Client/slicer.py`

**Role**: Splits large orders into smaller chunks that fit within the exchange's FreezeQty limit.

#### INPUT

| Field | Description |
|---|---|
| `order` | Full order dict |
| `max_slice_size` | `(FreezeQty // LotSize) × LotSize` — computed by exec_client |

#### ALGORITHM

```
if orderQuantity <= max_slice_size:
    return [order]   # no slicing needed

num_slices = ceil(orderQuantity / max_slice_size)
for i in 0..num_slices-1:
    slice_qty = min(max_slice_size, remaining_qty)
    slice_id  = f"{orderUniqueIdentifier}_{i+1}"[-20:]  # truncated to 20 chars
```

#### OUTPUT — List of order dicts

Each slice is a copy of the original order with:
- `orderQuantity` = slice quantity
- `orderUniqueIdentifier` = `"{parent_id}_{slice_number}"` (last 20 characters)

---

### 2.9 `Executor_Error/exec_error.py`

**Role**: Consumes error records from `Error_Stream` and takes corrective actions.

#### INPUT — Redis Stream `Error_Stream` (DB 13)

Same error record schema (see [2.1 Output B](#output-b--redis-stream-error_stream-db-13--on-any-stage-failure)).

#### PROCESSING — by error type

| `error_type` | Action |
|---|---|
| `order_data` | Close execution gate, log to MongoDB, send alarm to `alarms-v2` list |
| `pending_order` | Log error and acknowledge message |
| `AlgoSignalValidationError` | Per-client action mapping from `client_action_map:{algoName}` hash |
| `ClientOrderValidationError` | Close that client's gate in `Client_Strategy_Status` collection |

#### OUTPUTS

| Destination | Description |
|---|---|
| MongoDB `symphonyorder_raw.orders_{date}` | Error order records |
| MongoDB `algo_status.algo_message` | Monitoring/status messages |
| MongoDB `Client_Strategy_Status.Client_Strategy_Status` | Gate open/close flags per client-algo |
| Redis DB 9 list `alarms-v2` | Alarm messages for monitoring systems |

---

### 2.10 `executor_throttler/core/distributor.py`

**Role**: Reads orders from the shared stream and routes them to per-client priority queues.

#### INPUT — Redis Stream `throttle_all_orders` (DB 14)

Consumer group: `order_distributor_group`

Each message contains the full broker payload (same schema as [2.1 Output A](#output-a--redis-stream-client_api_place_order-db-14--on-success)).

#### PROCESSING — Priority Determination

```
order_value = limitPrice × orderQuantity
order_value > priority_threshold (default 2500)  →  HIGH priority
order_value ≤ priority_threshold                  →  LOW  priority
```

#### OUTPUT — Redis DB 13 Lists (per client)

| Key | Priority | Consumed by |
|---|---|---|
| `client:{clientID}:high` | High value orders | `ClientOrderThrottler` |
| `client:{clientID}:low` | Low value orders | `ClientOrderThrottler` |

---

### 2.11 `executor_throttler/core/throttler.py`

**Role**: Per-client sliding-window rate limiter. Reads from priority queues and sends orders to broker.

#### INPUT — Redis DB 13 Lists `client:{clientID}:high` and `client:{clientID}:low`

Full broker payload (same schema as [2.1 Output A](#output-a--redis-stream-client_api_place_order-db-14--on-success)).

#### PROCESSING — Rate Limiting

**Algorithm**: Sliding window (deque of timestamps)

```
max_orders_per_second = 15  (configurable)
randomize = True            (randomizes actual rate between min and max)

For each order:
  - Remove timestamps older than 1 second from deque
  - If len(deque) < max_rate: allow, add current timestamp
  - Else: wait until oldest timestamp expires
  - Process high-priority queue before low-priority
```

**Client blacklist**: Clients in the blacklist bypass rate limiting entirely.

#### OUTPUT

| Destination | Description |
|---|---|
| Broker REST API (HTTP POST) | Same endpoint as `api_client.py` |
| MongoDB `algo_signals_{date}` | Order result records (handoff to Response Handler) |
| Redis Stream `Error_Stream` | On broker failure |

---

## 3. Redis & MongoDB Reference

### Redis Database Map

| DB | Name | Type | Contents |
|---|---|---|---|
| 0 | SYMBOL_MAPPING | String | `{instrumentID}_{segment}` → symbol string; `{instrumentID}` → LTP |
| 2 | QUALITY_STOCKS | Hash | `quality_stocks` hash — instrument IDs of top 150 liquid equity stocks |
| 7 | MOCK | Mixed | `dummyMock` list for mock order testing |
| 9 | MARKET_DATA | Hash | `OI_LATEST` hash; `bid:{id}` hash; `alarms-v2` list |
| 10 | AUTH_TOKENS | Hash | `token:{broker}:{clientID}` → `{token, type, endpoint, suffix}` |
| 11 | MASTERFILE | String/Hash | `{instrumentID}` → JSON string; `{instrumentID}_hash` → Hash with PascalCase fields |
| 12 | CONFIG_CACHE | Hash | `algo_configs`, `algo_clients`, `client_rms`, `segment_rms_check` |
| 13 | LIVE_STREAMS | Stream + List | `Algo_Signal`, `Error_Stream`; `client:{id}:high`, `client:{id}:low` |
| 14 | THROTTLER | Stream + Hash | `Client_API_Place_Order`, `throttle_all_orders`, `Pending_Order_Stream`; `order_status:*` |

### Masterfile Field Schema (Redis DB 11)

| Field (PascalCase) | Description |
|---|---|
| `ExchangeSegment` | Segment name must match signal |
| `ExchangeInstrumentID` | Instrument token |
| `LotSize` | Minimum lot size for quantity validation |
| `FreezeQty` | Maximum quantity per exchange order slice |
| `Multiplier` | MCXFO quantity divisor |
| `Series` | `EQ`, `OPTIDX`, `OPTSTK`, `FUTIDX`, `FUTSTK` |

---

### MongoDB Collection Map

| Database | Collection | Writer | Reader | Purpose |
|---|---|---|---|---|
| `Info` | `algo_rms_params` | Admin | `algo_service.py` → Redis | Algo RMS config |
| `Info` | `segment_rms_params` | Admin | `segment_service.py` → Redis | Segment OI/spread config |
| `Info` | `client_rms_params` | Admin | `client_service.py` → Redis | Per-client multipliers |
| `Info` | `final_response` | Response Handler | — | Order outcome summaries |
| `algo_signals` | `algo_signals_{date}` | `exec_client.py` / throttler | Response Handler (Gyan) | **PRIMARY HANDOFF** |
| `symphonyorder_raw` | `orders_{date}` | `exec_client.py` | — | Raw order records |
| `error_db` | (various) | `exec_error.py` | — | Validation failure logs |
| `algo_status` | `algo_message` | `exec_error.py` | — | Monitoring messages |
| `Client_Strategy_Status` | `Client_Strategy_Status` | `exec_error.py` | `exec_client.py` | Execution gate flags |

---

## 4. Full System Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          CONFIGURATION SYNC (always running)                         │
│                                                                                      │
│   MongoDB Info database                                                              │
│   ├── algo_rms_params       ─┐                                                       │
│   ├── client_rms_params     ─┼──► pipeline/sync.py (every 2 seconds)                │
│   └── segment_rms_params    ─┘         │                                             │
│                                        ▼                                             │
│                              Redis DB 12 (CONFIG_CACHE)                              │
│                              ├── algo_configs      {algoName → JSON RMS params}      │
│                              ├── algo_clients      {algoName → [clientID, ...]}      │
│                              ├── client_rms        {clientID:algo → JSON}            │
│                              └── segment_rms_check {segment → JSON thresholds}       │
└───────────────────────────────────────────┬─────────────────────────────────────────┘
                                            │ (read every 3s by CacheUpdater thread)
                                            ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          SIGNAL INGESTION & VALIDATION                               │
│                                                                                      │
│  External Algo System                                                                │
│       │ JSON signal pushed                                                           │
│       ▼                                                                              │
│  Redis DB 13 Stream: Algo_Signal                                                     │
│       │                                                                              │
│       ▼                                                                              │
│  pipeline/firewall.py  (2 independent worker processes, Redis consumer group)        │
│                                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  Stage 1 — signal_field_check (pure memory)                                 │    │
│  │  • Validate 11 required fields                                              │    │
│  │  • Validate qty > 0, price ≥ 0, side ∈ {BUY,SELL}                         │    │
│  │  • MCXFO: divide orderQuantity by instrument Multiplier (Redis DB 11)       │    │
│  │  ──────────────────────────────────────────────────────────────────────     │    │
│  │  Stage 2 — algo_check (reads DB 11 on miss, writes counters to DB 12)       │    │
│  │  • Fetch instrument metadata (2-level: memory → Redis DB 11 MASTERFILE)     │    │
│  │  • Validate segment matches masterfile ExchangeSegment                      │    │
│  │  • Validate algo is active, segment is allowed                              │    │
│  │  • Validate qty is multiple of LotSize                                      │    │
│  │  • Check per_trade_lot_limit, max_order_value                               │    │
│  │  • Lua atomic rate check: trade_count_second (2s TTL)                       │    │
│  │  • Increment trade_count_day, lot_count_day; check daily limits             │    │
│  │  ──────────────────────────────────────────────────────────────────────     │    │
│  │  Stage 3 — segment_check (reads DB 12, DB 9)                                │    │
│  │  • Derivatives: check OI ≥ threshold, spread ≤ max_spread_pct              │    │
│  │  • Equity: check instrument in quality_stocks set                           │    │
│  │  ──────────────────────────────────────────────────────────────────────     │    │
│  │  Stage 4 — client_check + fan-out (reads memory cache, writes DB 14)        │    │
│  │  • Look up client list from algo_clients cache                              │    │
│  │  • Per-client (ThreadPoolExecutor, 20 threads):                             │    │
│  │    - Apply quantity_multiplier from client_rms cache                        │    │
│  │    - Generate orderUniqueIdentifier = "{algoName}_{uuid4}"                  │    │
│  │    - Build broker payload                                                   │    │
│  │    - XADD to Client_API_Place_Order stream (DB 14)                         │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                      │
│       │ FAIL (stage 1/2/3/4)              │ PASS (stage 4 produces N payloads)      │
│       ▼                                   ▼                                          │
│  Redis DB 13                         Redis DB 14                                     │
│  Stream: Error_Stream                Stream: Client_API_Place_Order                  │
└───────────────┬───────────────────────────┬──────────────────────────────────────────┘
                │                           │
                ▼                           ▼
┌───────────────────────┐   ┌──────────────────────────────────────────────────────────┐
│  ERROR HANDLING        │   │  ORDER EXECUTION                                         │
│                        │   │                                                          │
│  Executor_Error/       │   │  Executor_Client/exec_client.py                          │
│  exec_error.py         │   │  (async, uvloop, semaphore concurrency=10)               │
│                        │   │                                                          │
│  Reads Error_Stream    │   │  Stage 1: Masterfile lookup                              │
│                        │   │  • HGETALL {id}_hash from Redis DB 11                   │
│  Actions:              │   │  • Extract FreezeQty, LotSize → compute slice_qty        │
│  • Close client gate   │   │                                                          │
│    in MongoDB          │   │  Stage 2: Symbol + LTP lookup                           │
│  • Log to             │   │  • Redis DB 0: {id}_{segment} → symbol string            │
│    symphonyorder_raw   │   │  • Redis DB 0: {id} → current LTP                       │
│  • Write alarm to      │   │                                                          │
│    Redis DB 9          │   │  Stage 3: Order slicing (slicer.py)                     │
│    alarms-v2 list      │   │  • If qty > slice_qty: split into N slices              │
│  • Update              │   │  • Each slice gets suffix _1, _2, ... _N               │
│    Client_Strategy_    │   │                                                          │
│    Status in MongoDB   │   │  Stage 4: Broker send (api_client.py)                   │
│                        │   │  • Auth token from Redis DB 10: token:{broker}:{client}  │
│                        │   │  • HTTP POST → broker REST API                          │
│                        │   │  • Response: AppOrderID                                 │
│                        │   │  • Insert signal_feed into MongoDB symphonyorder_raw     │
│                        │   │  • Push pending record to Pending_Order_Stream (DB 14)  │
│                        │   │  • On error → push to Error_Stream (DB 13)             │
└───────────────────────┘   └─────────────────────┬────────────────────────────────────┘
                                                   │
                              ┌────────────────────┘
                              │ OR if use_throttler=true, exec_client publishes to:
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  THROTTLER  (executor_throttler/)                                                    │
│                                                                                      │
│  main.py — Orchestrator                                                              │
│  • Spawns N OrderDistributor processes                                               │
│  • Discovers active clients every 1s from Redis DB 13                               │
│  • Spawns one ClientOrderThrottler process per active client                         │
│                                                                                      │
│  ┌───────────────────────────────────────────┐                                       │
│  │  core/distributor.py (N processes)         │                                      │
│  │                                           │                                       │
│  │  INPUT:  Redis DB 14                      │                                       │
│  │          Stream throttle_all_orders       │                                       │
│  │          Consumer group: order_dist_group │                                       │
│  │                                           │                                       │
│  │  LOGIC:  order_value = price × qty        │                                       │
│  │          > 2500 → HIGH priority           │                                       │
│  │          ≤ 2500 → LOW  priority           │                                       │
│  │                                           │                                       │
│  │  OUTPUT: Redis DB 13 Lists                │                                       │
│  │          client:{clientID}:high           │                                       │
│  │          client:{clientID}:low            │                                       │
│  └───────────────────────────────────────────┘                                       │
│                          │                                                            │
│                          ▼                                                            │
│  ┌───────────────────────────────────────────┐                                       │
│  │  core/throttler.py (1 process per client)  │                                      │
│  │                                           │                                       │
│  │  INPUT:  Redis DB 13 Lists                │                                       │
│  │          client:{clientID}:high  (first)  │                                       │
│  │          client:{clientID}:low   (then)   │                                       │
│  │                                           │                                       │
│  │  RATE LIMIT: Sliding window               │                                       │
│  │   • max 15 orders/second/client           │                                       │
│  │   • randomization optional               │                                       │
│  │   • blacklisted clients: no limit         │                                       │
│  │                                           │                                       │
│  │  OUTPUT: HTTP POST → Broker REST API      │                                       │
│  │          MongoDB: algo_signals_{date}     │                                       │
│  └───────────────────────────────────────────┘                                       │
└─────────────────────────────────┬───────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  BROKER REST API                                                                     │
│  POST {endpoint}/interactive{suffix}/orders                                          │
│  Returns: { "result": { "AppOrderID": "..." }, "status": 200 }                      │
└─────────────────────────────────┬───────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  HANDOFF TO RESPONSE HANDLER (Gyan)                                                  │
│                                                                                      │
│  MongoDB: algo_signals.algo_signals_{YYYY-MM-DD}                                    │
│                                                                                      │
│  Fields written by OMS (gokul):      Fields updated by Response Handler (Gyan):     │
│  ├── orderUniqueIdentifier           ├── responseFlag  (False → True on complete)   │
│  ├── clientID                        └── leaves_quantity (updated on partial fill)   │
│  ├── exchangeInstrumentID                                                            │
│  ├── exchangeSegment                                                                 │
│  ├── orderQuantity                                                                   │
│  ├── responseFlag = False                                                            │
│  ├── appOrderId                                                                      │
│  └── ... (all signal_feed fields)                                                    │
│                                                                                      │
│  CRITICAL CONTRACT: never rename orderUniqueIdentifier, responseFlag,                │
│  or leaves_quantity without coordinating with Gyan.                                  │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

*Documentation last updated: 2026-03-21*
