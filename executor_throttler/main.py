"""
Executor Throttler - Main application orchestrator.

Initializes and manages the order execution pipeline with the following key components:

1. **OrderDistributor**: Consumes orders from Redis stream and routes to priority queues
2. **ClientOrderThrottler**: Rate-limits and executes orders per client via HTTP API

The system enforces per-client rate limiting (configured max_orders_per_second) and
priority-based queue consumption. Clients can be blacklisted to skip rate limiting.

Usage:
    python main.py
    
    Requires 'config.ini' in the current directory with Redis, MongoDB, and
    trading API configuration.
"""

import os
import json
from time import sleep
from configparser import ConfigParser


from core.pm import ProcessManager
from core.logger import setup_logging
from core.distributor import OrderDistributor
from core.auth import get_redis_conn
from core.throttler import get_client_exec_list, ClientOrderThrottler


def initialize_client_order_throttler(logger, config, process_manager, exec_id, client_id, limit_ops=True):
    """
    Initialize a client order throttler process if not already running.

    Creates and registers a new ClientOrderThrottler process for the given client
    if one doesn't already exist or has crashed. Skips if process is already alive.

    Args:
        logger: Logger instance for this operation
        config: Configuration parser with all settings
        process_manager: ProcessManager instance for lifecycle management
        exec_id: Unique executor ID for the throttler process
        client_id: Client identifier for the throttler
        limit_ops: Whether to enforce rate limiting for this client (default: True)
    """
    if not process_manager.is_process_alive(exec_id):
        logger.info(
            f"Process {exec_id} is not running. Starting...")

        client_order_throttler = ClientOrderThrottler(
            config, exec_id, client_id,limit_ops=limit_ops)
        process_manager.register_process(
            exec_id, client_order_throttler)
        process_manager.start_process(exec_id)


def main():
    """
    Main application entry point.

    Orchestrates the complete order execution pipeline:
    1. Loads configuration and initializes logging
    2. Creates ProcessManager for lifecycle management
    3. Spawns OrderDistributor processes to consume orders from Redis stream
    4. Continuously discovers active clients and spawns ClientOrderThrottler
       processes as needed
    5. Enforces per-client rate limiting and applies client blacklist

    Configuration is read from 'config.ini' in the current directory.
    Logs are written to 'logs/' directory.
    """
    logger = setup_logging(
        logger_name='main', log_dir="logs")
    logger.info("="*30 + "Executor Throttler started" + "="*40)

    configfile = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
    config = ConfigParser()
    config.read(configfile)
    logger.info(f"Loaded configuration from: {configfile}")

    process_manager = ProcessManager()

    order_distributor_processes = config.getint(
        'throttler', 'order_distributor_processes', fallback=os.cpu_count())

    for i in range(order_distributor_processes):
        od_exec_id = f"OrderDistributor-{i+1}"
        order_distribution_worker = OrderDistributor(config, od_exec_id)
        process_manager.register_process(od_exec_id, order_distribution_worker)

    process_manager.start_all_processes()

    redis_conn_client_ops_queue = get_redis_conn(
        config, logger, db_index=13, purpose="Client Operations")

    client_blacklist = [i.strip() for i in config.get('throttler', 'client_blacklist', fallback="").split(',')]
    logger.info(f"Client Blacklist: {client_blacklist}")

    while True:
        exec_id_list = get_client_exec_list(
            redis_conn_client_ops_queue, config)

        for client_exec_id in exec_id_list:
            limit_ops = False if client_exec_id['client_id'] in client_blacklist else True
            initialize_client_order_throttler(
                logger, config, process_manager, **client_exec_id, limit_ops=limit_ops)

        sleep(1)


if __name__ == "__main__":
    main()
