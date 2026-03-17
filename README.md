# OMS — Order Management System (Gokul's Part)

**Owner:** Gokul  
**System:** Algo Trading Signal Pipeline  
**Counterpart:** Response Handler (Gyan)

---

## Project Structure

```
gokul_oms/
├── pipeline/
│   ├── algo_signal_check.py    ← Entry point: all 4 pipeline stages + fan-out
│   ├── client_signal_check.py  ← Per-client qty multiplier + throttler push
│   ├── order_executer.py       ← HTTP POST to broker, MongoDB handoff write
│   ├── error_handler.py        ← Error_Stream consumer → MongoDB persistence
│   └── sync.py                 ← Background config sync: MongoDB → Redis DB 12
├── services/
│   ├── algo_service.py         ← Algo RMS config CRUD + Redis cache builder
│   └── segment_service.py      ← Segment RMS config CRUD + Redis cache builder
├── utils/
│   ├── db_helpers.py           ← Redis + MongoDB connection manager
│   ├── schema.py               ← MongoDB collection/database registry
│   └── setup_logging.py        ← Rotating file logger factory
└── requirements.txt
```

---

## Pipeline Stages (all in algo_signal_check.py)

| Stage | Function | DB Reads | DB Writes |
|-------|----------|----------|-----------|
| 1 | signal_field_check | None | Error_Stream (DB 13) on fail |
| 2 | algo_check | DB 11, DB 12 | DB 12 counters |
| 3 | segment_check | DB 11, DB 12, DB 9 or DB 2 | Error_Stream on fail |
| 4 | client_check + fan-out | DB 12, DB 10 | DB 13 (client streams), DB 14 (throttler) |

---

## Critical Handoff — algo_signals_{date} Collection

`order_executer.py` writes to `algo_signals_{YYYY-MM-DD}` in MongoDB **before** returning.

This is the **PRIMARY HANDOFF** point between OMS (Gokul) and Response Handler (Gyan).

**Fields Gyan depends on:**
- `orderUniqueIdentifier` — PRIMARY JOIN KEY (Gyan's find_one filter)
- `clientID`
- `exchangeInstrumentID`
- `exchangeSegment`
- `orderQuantity`
- `responseFlag` (OMS writes False; Gyan sets True on fill/cancel/reject)
- `leaves_quantity` (Gyan updates on partial fill)

**⚠️ Never rename or remove these fields without coordinating with Gyan first.**

---

## Running

```bash
# Install dependencies
pip install -r requirements.txt

# Start pipeline entry point (reads Algo_Signal stream)
python -m pipeline.algo_signal_check

# Start per-client processor (one per active client)
CLIENT_ID=client123 python -m pipeline.client_signal_check

# Start order executer (one per active client)
CLIENT_ID=client123 python -m pipeline.order_executer

# Start error handler (one instance system-wide)
python -m pipeline.error_handler

# Start config sync (one instance system-wide)
python -m pipeline.sync
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_DATA_HOST` | localhost | Redis data instance host |
| `REDIS_DATA_PORT` | 6379 | Redis data instance port |
| `REDIS_LIVE_HOST` | localhost | Redis live instance host |
| `REDIS_LIVE_PORT` | 6380 | Redis live instance port |
| `MONGO_URI` | mongodb://localhost:27017 | MongoDB connection string |
| `BROKER_BASE_URL` | https://api.broker.example.com | Broker HTTP base URL |
| `LOG_DIR` | logs/ | Log file directory |
| `LOG_LEVEL` | INFO | Logging level |
