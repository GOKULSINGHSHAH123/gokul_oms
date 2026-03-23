# Executor Throttler

A high-performance order execution system that distributes and throttles trading orders across multiple clients with sliding-window rate limiting and priority-based queue management.

## Project Purpose and Scope

**Executor Throttler** is a distributed order execution engine designed to:

- **Route Orders**: Consume orders from a Redis stream and distribute them to client-specific queues
- **Enforce Rate Limiting**: Implement per-client sliding-window rate limiting to enforce maximum orders per second constraints
- **Execute Orders**: Send orders to a trading API via HTTP with automatic retry and error handling
- **Modify/Cancel Orders**: Support order modifications and cancellations with price updates based on latest market data
- **Track Orders**: Map order identifiers across systems and persist execution records in MongoDB

The system is built for low-latency, high-throughput trading environments where strict rate limiting and priority-based execution are critical.

## High-Level Architecture

```
Redis Stream (Incoming Orders)
    ↓
[OrderDistributor Process(es)]
    ↓ (SCAN for clients)
[ClientOrderThrottler Process(es) - Per Client]
    ↓
    ├─ Priority Queue (High/Low)
    ├─ Sliding Window Rate Limiter
    ├─ HTTP API (Order Placement, Modification, Cancellation)
    ├─ Redis (Order Status, Token Management)
    └─ MongoDB (Order Records & Execution Logs)
```

### Data Flow

1. **Order Ingestion**: Orders arrive in a Redis stream (`redisStreamIncomingOrders`)
2. **Distribution**: OrderDistributor processes consume from the stream and parse orders
3. **Priority Routing**: Orders are routed to client-specific queues (high/low priority based on notional value)
4. **Rate Limiting**: ClientOrderThrottler applies per-client rate limits using a sliding-window algorithm
5. **Execution**: Orders are submitted to the trading API via HTTP (GET/POST)
6. **Response Handling**: API responses are processed and order status is tracked in Redis and MongoDB
7. **Modifications**: Orders can be modified (limit price updates) or cancelled based on downstream signals

### Process Model

- **Multi-Process**: Uses Python's `multiprocessing` for true parallelism
- **Async Operations**: ClientOrderThrottler uses `asyncio` for non-blocking HTTP requests and Redis operations
- **Daemon Processes**: All child processes are daemon processes for automatic cleanup on parent termination
- **Dynamic Client Discovery**: Continuously discovers active clients from Redis and spawns throttler processes on-demand

## Key Modules and Responsibilities

### [core/auth.py](core/auth.py)
**Connection Management**

Provides factory functions for establishing Redis and MongoDB connections. Supports both synchronous and asynchronous interfaces with configurable database indices and error handling.

**Key Functions:**
- `get_redis_conn()`: Synchronous Redis connection
- `get_redis_conn_async()`: Asynchronous Redis connection (aioredis)
- `get_mongo_conn()`: Synchronous MongoDB connection
- `get_mongo_conn_async()`: Asynchronous MongoDB connection (motor)

**Database Indices:**
```
DB 0:  Symbol Data Feed (Market Data)
DB 7:  Mock Infrastructure
DB 9:  Ping Connection (Heartbeat/Timing)
DB 10: Token Infrastructure (Auth Tokens)
DB 11: Master Data Feed (Instrument Metadata)
DB 12: Executor Infrastructure (Streams)
DB 13: Client Operations (Order Queues)
DB 14: Order Modification (Status Tracking)
```

### [core/pm.py](core/pm.py)
**Process Lifecycle Management**

Manages multiprocessing.Process instances with features including:
- Daemon process configuration
- Graceful and forceful termination
- Health checking and process status tracking
- Signal-based shutdown (SIGTERM, SIGINT)
- Context manager support

**Key Classes:**
- `ProcessManager`: Central process registry and controller

**Design Decision**: All processes are set as daemon processes by default. This ensures cleanup when the parent process terminates, even if killed with SIGKILL. Critical for reliable operation in production environments.

### [core/logger.py](core/logger.py)
**Centralized Logging**

Provides consistent logging configuration with rotating file handlers and multi-process support. Each process component has its own logger instance to avoid conflicts.

**Key Functions:**
- `setup_logging()`: Creates a logger with RotatingFileHandler (10MB per file, keeps 10 backups)

**Log Organization:**
```
logs/
├── main_YYYY-MM-DD.log
├── order_distributor/
│   └── order_distributor_YYYY-MM-DD.log
└── client_order_throttler/
    ├── client_<client_id>_YYYY-MM-DD.log
    └── ...
```

### [core/utils.py](core/utils.py)
**Data Parsing and Transformation**

Provides utility functions for converting Redis byte strings to native Python types.

**Key Functions:**
- `parse_redis_dict_values()`: Decodes byte keys/values and infers numeric types

### [core/distributor.py](core/distributor.py)
**Order Distribution and Routing**

Consumes orders from a Redis stream (consumer group) and routes them to client-specific priority queues based on order value (notional = price × quantity).

**Key Classes:**
- `OrderDistributor(Process)`: Main distributor process

**Algorithm:**
1. Reads messages from Redis stream in batches (configurable READ_COUNT)
2. Parses order JSON and extracts client ID and price/quantity
3. Calculates notional value: `priority_value = limitPrice × orderQuantity`
4. Routes to high priority queue if `priority_value > threshold`, otherwise low priority
5. Acknowledges message after successful enqueue

**Design Decision**: Using consumer groups for reliable, distributed message processing. Failed messages are automatically retried if the process crashes (they remain pending until acknowledged).

### [core/throttler.py](core/throttler.py)
**Order Throttling and Execution**

Main order execution engine with rate limiting, HTTP request handling, and order tracking.

**Key Classes:**

#### `SlidingWindowRateLimiter`
Implements strict rate limiting over any sliding time window.

**Design**: Unlike discrete token buckets, this maintains actual request timestamps and enforces limits over any time window. Critical for trading systems where consistent rate limits are essential.

**Features:**
- Configurable capacity and window (default: 10 orders/second)
- Optional randomization of capacity (anti-pattern detection)
- `try_consume(tokens)`: Returns True if request can be made
- `available_tokens()`: Current tokens available
- `time_until_next_refill()`: Wait time until next token available

#### `ClientOrderThrottler(Process)`
Per-client order throttler that applies rate limiting and executes orders.

**Initialization:**
- Sets up sliding window rate limiter with configured parameters
- Lazy initializes async clients in `async_init()` (called when process starts)

**Async Operations:**
- Consumes from high/low priority queues with priority preference
- Checks rate limiter before consuming orders
- Puts back orders if rate limit reached (maintains FIFO order)
- Sleeps until next token available

**Order Processing Paths:**
1. **Order Placement** (`send_order_as_http_request`):
   - Parses order, updates limit price if needed
   - Sends HTTP POST to trading API
   - Maps order ID to AppOrderID in Redis
   - Stores order record in MongoDB

2. **Order Modification** (`send_modify_order_as_http_request`):
   - Fetches order from MongoDB
   - Recalculates limit price based on current LTP
   - Sends HTTP POST to modify endpoint
   - Updates MongoDB with new price and timestamp

3. **Order Cancellation** (`send_cancel_order_as_http_request`):
   - Sends HTTP DELETE/POST to cancel endpoint
   - Updates order status as "Cancelled" in MongoDB
   - Handles "order not found" errors (already filled)

**Limit Price Calculation** (`calculateLimitPrice`):
- For **Futures/Equities** (Type 1, 8): Uses percentage-based thresholds
  - If LTP > threshold: ±percentage adjustment
  - Otherwise: Apply different percentage or amount
  - Snap to nearest tick size
  
- For **Options** (Type 2): Uses premium-based thresholds
  - If LTP > small_premium_threshold: ±percentage adjustment
  - Otherwise: Fixed amount adjustment with minimum floor
  - Ensures options remain tradeable on both sides

**Complex Logic - Sliding Window Implementation**:
The `_cleanup_old_timestamps()` and rate limiter methods implement a strict sliding window algorithm that differs from token buckets. Instead of discrete refills at intervals, timestamps are stored and the oldest ones are purged when they fall outside the window. This provides deterministic rate limiting for trading systems:

```python
# At timestamp T, can make 10 requests in [T, T+1.0]
# If 10 requests already made at T, must wait until oldest falls out at T+1.0
```

### [main.py](main.py)
**Application Orchestrator**

Main entry point that coordinates the order execution pipeline.

**Responsibilities:**
1. Loads configuration from `config.ini`
2. Creates ProcessManager for lifecycle control
3. Spawns N OrderDistributor processes (configurable, default: number of CPU cores)
4. Enters main loop that:
   - Discovers active clients from Redis (via SCAN)
   - Spawns ClientOrderThrottler for each new client
   - Applies client blacklist (skip rate limiting for configured clients)
   - Updates process list every second

**Client Blacklist**: Comma-separated list of client IDs that bypass rate limiting.

## Complex Logic and Design Decisions

### 1. Sliding Window Rate Limiting
**Why**: Trading systems need strict, deterministic rate limits. A client sending 10 requests at T=0.0 should have 0 capacity until T=1.0, not until the next discrete interval.

**Implementation**: Maintains a deque of request timestamps. When checking if a request can be made, removes all timestamps outside the current window and compares remaining count to capacity.

### 2. Consumer Group Message Handling
**Why**: If a ClientOrderThrottler process crashes, orders shouldn't be lost. Redis consumer groups provide automatic retry via pending entry list (PEL).

**Implementation**: OrderDistributor only acknowledges messages after successfully enqueuing. If the process crashes before acknowledgement, the message remains pending and is automatically retried.

### 3. Priority Queue with FIFO Preservation
**Why**: High-priority orders should be processed first, but within the same priority, FIFO order must be maintained for fairness.

**Implementation**: Uses two Redis lists (`client:{id}:high` and `client:{id}:low`). ClientOrderThrottler always checks high priority first, then low. If rate limit is reached, the popped order is pushed back to maintain position.

### 4. Dynamic Client Discovery
**Why**: Clients can connect/disconnect at runtime. Throttler processes should only exist for active clients.

**Implementation**: Main loop continuously scans Redis for active client queues and spawns/monitors processes. Uses efficient Redis SCAN with pattern matching to avoid blocking.

### 5. Limit Price Calculation Rules
**Why**: Different instrument types have different trading rules. Options have premium constraints, futures need percentage buffers.

**Implementation**: Queries instrument master data (InstrumentType, TickSize), then applies type-specific rules. Prices are always snapped to the nearest tick to ensure exchange acceptance.

## Configuration

Configuration is read from `config.ini`. Key sections:

```ini
[params]
# Order processing
max_orders_per_second = 10
order_distributor_processes = 4
order_priority_threshold = 100000  # Notional value threshold for high priority
randomize_per_second = false       # Enable capacity randomization
min_orders_per_second = 6          # Min capacity if randomizing

# Client blacklist (no rate limiting for these clients)
client_blacklist = client_001, client_002

[infraParams]
# Redis connection for infrastructure/streaming
redisHost = localhost
redisPort = 6379
redisPass = password

[dbParams]
# Redis connection for market data
redisHost = data-redis-host
redisPort = 6379
redisPass = password

[infraParams]
# MongoDB
mongoHost = localhost
mongoPort = 27017
mongoUser = user
mongoPass = password

[LimitOrder]
# Futures/Equity limit price rules
notOptionLimitPriceThreshold = 100.0
notOptionThresholdAbovePercent = 0.05    # 5% above LTP
notOptionThresholdBelowPercent = 0.05    # 5% below LTP

# Options limit price rules
optionSmallPremiumThreshold = 10.0
extraPercent = 0.10                      # 10% premium adjustment
extraAmount = 0.5                        # 50 paise
minimumLimitPrice = 5.0                  # Floor for sells
```

## Setup and Installation

### Prerequisites
- Python 3.7+
- Redis 6.0+ (for Streams and Consumer Groups)
- MongoDB 4.0+
- Trading API access (configured in config.ini)

### Installation

1. **Clone/Copy Repository**
```bash
cd executor_throttler
```

2. **Create Virtual Environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install Dependencies**
```bash
pip install -r requirements.txt
```

Dependencies:
- `redis`: Synchronous Redis client
- `aioredis`: Asynchronous Redis client
- `motor`: Asynchronous MongoDB client
- `aiohttp`: Asynchronous HTTP client
- `pymongo`: MongoDB driver
- `pandas`: Data processing (used by APIs)

4. **Configure Application**
```bash
cp config_placeholder.ini config.ini
# Edit config.ini with your infrastructure details
```

## Future Enhancements

Planned improvements (documented in code TODOs):
- Redis streams trimming for memory efficiency
- Unit tests for order creation
- Process health checks with automatic restart capability
- Enhanced cancel order handling with multiple retry strategies
- Error stream monitoring and automatic remediation
- Random distribution between 6-8 orders per second for smarter rate limiting
- Persistent order database with full audit trail
- Redis stream message acknowledgement batching for throughput
- Expiry/TTL on all Redis keys for automatic cleanup
