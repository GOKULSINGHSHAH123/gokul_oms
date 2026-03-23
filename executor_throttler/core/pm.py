"""
Process lifecycle management module.

Provides centralized management for multiprocessing.Process instances with features
including: daemon process configuration, graceful and forceful termination, health
checking, and signal-based shutdown handling.

All child processes are set as daemon processes to ensure cleanup when the parent
process terminates, providing robust lifecycle management for multi-process applications.

Classes:
    ProcessManager: Main process lifecycle manager with start/stop/restart capabilities
"""

import signal
import atexit
import os
import time
import multiprocessing
from typing import Dict, Optional, Union


from core.logger import setup_logging


class ProcessManager:
    """
    A manager class for handling multiple multiprocessing.Process instances.
    Provides lifecycle management including start, stop, terminate, restart, and health checking.

    All child processes are automatically set as daemon processes to ensure they are terminated
    when the parent process exits, including if terminated by SIGKILL.
    """

    def __init__(self, auto_daemon: bool = True, register_signal_handlers: bool = True):
        """
        Initialize the ProcessManager with an empty process registry.

        Args:
            auto_daemon: If True, all registered processes will be set as daemon processes
            register_signal_handlers: If True, register signal handlers for SIGTERM and SIGINT
        """
        self._processes: Dict[str, multiprocessing.Process] = {}
        # Track process termination behavior
        self._process_health: Dict[str, Dict] = {}
        self._auto_daemon = auto_daemon
        self._main_pid = os.getpid()  # Store the main process PID

        # Set up logging
        self._logger = setup_logging(logger_name="ProcessManager")

        # Register cleanup on normal Python exit
        atexit.register(self.cleanup)

        # Register signal handlers
        if register_signal_handlers:
            try:
                signal.signal(signal.SIGTERM, self._signal_handler)
                signal.signal(signal.SIGINT, self._signal_handler)
                self._logger.info(
                    "Signal handlers registered for graceful shutdown.")
            except ValueError:
                # This happens when not in the main thread
                self._logger.warning(
                    "Could not register signal handlers - not running in main thread. "
                    "Signal-based termination will not clean up processes.")

    def _signal_handler(self, sig, frame):
        """
        Handle process termination signals for graceful shutdown.

        When SIGTERM or SIGINT is received, initiates orderly shutdown of all child
        processes before allowing the main process to terminate.

        Args:
            sig: Signal number
            frame: Current stack frame
        """
        # Only handle signals in the main process that created this ProcessManager
        if os.getpid() != self._main_pid:
            self._logger.debug(
                f"Ignoring signal {sig} in child process (PID: {os.getpid()})")
            return

        self._logger.info(f"Received signal {sig}. Stopping all processes...")
        # Use shorter timeout for signal handling to improve responsiveness
        self.stop_all_processes(timeout=3.0)
        # Re-raise the signal to allow the default handler to run
        # This ensures the main process still terminates
        signal.default_int_handler(sig, frame)

    def register_process(self, name: str, process: multiprocessing.Process) -> None:
        """
        Register a process instance with a unique name.
        If auto_daemon is True, sets the process as a daemon process if it hasn't started yet.

        Args:
            name: Unique identifier for the process
            process: Process instance that inherits from multiprocessing.Process
        """
        if name in self._processes:
            self._logger.warning(
                f"Process '{name}' already exists. Replacing existing process.")

        # Set daemon flag to ensure the process terminates when parent terminates
        if self._auto_daemon:
            if process.is_alive():
                self._logger.warning(
                    f"Process '{name}' is already running. Cannot set as daemon process. "
                    f"Process may not terminate if parent is killed with SIGKILL.")
            else:
                process.daemon = True
                self._logger.debug(f"Process '{name}' set as daemon process.")

        self._processes[name] = process

    def start_process(self, name: str) -> bool:
        """
        Start a specific process by name.

        Args:
            name: Name of the process to start

        Returns:
            bool: True if started successfully, False otherwise
        """
        if name not in self._processes:
            self._logger.error(f"Process '{name}' not found in registry.")
            return False

        process = self._processes[name]
        if process.is_alive():
            self._logger.warning(f"Process '{name}' is already running.")
            return False

        try:
            process.start()
            self._logger.info(f"Process '{name}' started successfully.")
            return True
        except Exception as e:
            self._logger.error(f"Failed to start process '{name}': {e}")
            return False

    def start_all_processes(self) -> Dict[str, bool]:
        """
        Start all registered processes.

        Returns:
            Dict[str, bool]: Dictionary mapping process names to start success status
        """
        results = {}
        for name in self._processes:
            results[name] = self.start_process(name)
        return results

    def stop_process(self, name: str, timeout: float = 5.0) -> bool:
        """
        Gracefully stop a specific process by name.

        Args:
            name: Name of the process to stop
            timeout: Time to wait for graceful shutdown before forcing termination

        Returns:
            bool: True if stopped successfully, False otherwise
        """
        if name not in self._processes:
            self._logger.error(f"Process '{name}' not found in registry.")
            return False

        process = self._processes[name]

        # Check if we're in the correct process context
        if os.getpid() != self._main_pid:
            self._logger.warning(
                f"Cannot stop process '{name}' from child process. "
                f"Process management should only be done from main process.")
            return False

        try:
            if not process.is_alive():
                self._logger.warning(f"Process '{name}' is not running.")
                return True
        except AssertionError:
            # Process might have been created in a different context
            self._logger.warning(
                f"Cannot check status of process '{name}' - may have been forked. "
                f"Attempting termination anyway.")

        try:
            # Attempt graceful shutdown
            process.terminate()
            process.join(timeout=timeout)

            # Force kill if still alive
            if process.is_alive():
                self._logger.warning(
                    f"Process '{name}' did not terminate gracefully. Force killing.")
                process.kill()
                process.join()

            self._logger.info(f"Process '{name}' stopped successfully.")
            return True
        except Exception as e:
            self._logger.error(f"Failed to stop process '{name}': {e}")
            return False

    def stop_all_processes(self, timeout: float = 5.0) -> Dict[str, bool]:
        """
        Gracefully stop all registered processes with parallel termination.

        Attempts graceful termination via SIGTERM for all processes, then force-kills
        any that don't terminate within the timeout. Distributes timeout evenly across
        all processes for efficient shutdown.

        Args:
            timeout: Total time in seconds to wait for all processes to shutdown

        Returns:
            Dict[str, bool]: Mapping process names to termination success status
        """
        # First, send terminate signal to all processes
        running_processes = {}
        for name, process in self._processes.items():
            if process.is_alive():
                try:
                    start_time = time.time()
                    process.terminate()
                    running_processes[name] = {
                        "process": process, "start_time": start_time}
                except Exception as e:
                    self._logger.error(
                        f"Failed to terminate process '{name}': {e}")

        # Set initial results
        results = {name: not self._processes[name].is_alive(
        ) for name in self._processes}

        if not running_processes:
            return results

        # Calculate per-process timeout
        per_process_timeout = max(
            0.5, timeout / max(len(running_processes), 1))

        # Join terminated processes with individual timeouts
        for name, data in running_processes.items():
            process = data["process"]
            process.join(timeout=per_process_timeout)
            results[name] = not process.is_alive()

            # Track termination time for future reference if process terminated
            if not process.is_alive():
                term_time = time.time() - data["start_time"]
                self.track_process_termination(name, term_time)

        # Force kill processes that didn't terminate
        for name, data in running_processes.items():
            process = data["process"]
            if process.is_alive():
                try:
                    self._logger.warning(
                        f"Process '{name}' did not terminate gracefully. Force killing.")
                    start_time = time.time()
                    process.kill()
                    process.join(timeout=0.5)
                    results[name] = not process.is_alive()

                    # Track the kill time too
                    if not process.is_alive():
                        kill_time = time.time() - start_time
                        self.track_process_termination(name, kill_time)

                except Exception as e:
                    self._logger.error(f"Failed to kill process '{name}': {e}")

        return results

    def terminate_process(self, name: str) -> bool:
        """
        Forcefully terminate a specific process by name.

        Args:
            name: Name of the process to terminate

        Returns:
            bool: True if terminated successfully, False otherwise
        """
        if name not in self._processes:
            self._logger.error(f"Process '{name}' not found in registry.")
            return False

        process = self._processes[name]
        if not process.is_alive():
            self._logger.warning(f"Process '{name}' is not running.")
            return True

        try:
            process.kill()
            process.join()
            self._logger.info(f"Process '{name}' terminated forcefully.")
            return True
        except Exception as e:
            self._logger.error(f"Failed to terminate process '{name}': {e}")
            return False

    def terminate_all_processes(self) -> Dict[str, bool]:
        """
        Forcefully terminate all registered processes.

        Returns:
            Dict[str, bool]: Dictionary mapping process names to termination success status
        """
        results = {}
        for name in self._processes:
            results[name] = self.terminate_process(name)
        return results

    def restart_process(self, name: str, timeout: float = 5.0) -> bool:
        """
        Restart a specific process by name (stop then start).

        Args:
            name: Name of the process to restart
            timeout: Time to wait for graceful shutdown

        Returns:
            bool: True if restarted successfully, False otherwise
        """
        if name not in self._processes:
            self._logger.error(f"Process '{name}' not found in registry.")
            return False

        # Stop the process first
        if not self.stop_process(name, timeout):
            return False

        # Create a new instance of the same process class
        old_process = self._processes[name]
        try:
            # Create new process with same target and args
            new_process = multiprocessing.Process(
                target=old_process._target,
                args=old_process._args,
                kwargs=old_process._kwargs,
                name=old_process.name,
                daemon=self._auto_daemon  # Set daemon flag based on configuration
            )
            self._processes[name] = new_process

            # Start the new process
            return self.start_process(name)
        except Exception as e:
            self._logger.error(f"Failed to restart process '{name}': {e}")
            return False

    def restart_all_processes(self, timeout: float = 5.0) -> Dict[str, bool]:
        """
        Restart all registered processes.

        Args:
            timeout: Time to wait for each process to shutdown gracefully

        Returns:
            Dict[str, bool]: Dictionary mapping process names to restart success status
        """
        results = {}
        for name in self._processes:
            results[name] = self.restart_process(name, timeout)
        return results

    def is_process_alive(self, name: str) -> Optional[bool]:
        """
        Check if a specific process is alive.

        Args:
            name: Name of the process to check

        Returns:
            Optional[bool]: True if alive, False if dead, None if not found
        """
        if name not in self._processes:
            # self._logger.error(f"Process '{name}' not found in registry.")
            return False

        return self._processes[name].is_alive()

    def health_check_all(self) -> Dict[str, bool]:
        """
        Check the health (alive status) of all registered processes.

        Returns:
            Dict[str, bool]: Dictionary mapping process names to their alive status
        """
        health_status = {}
        for name in self._processes:
            health_status[name] = self._processes[name].is_alive()
        return health_status

    def list_processes(self) -> Dict[str, Dict[str, Union[str, bool, int]]]:
        """
        List all managed processes along with their detailed status.

        Returns:
            Dict: Dictionary with process names as keys and status info as values
        """
        process_list = {}
        for name, process in self._processes.items():
            process_list[name] = {
                'name': process.name or name,
                'pid': process.pid,
                'is_alive': process.is_alive(),
                'exitcode': process.exitcode,
                'daemon': process.daemon
            }
        return process_list

    def remove_process(self, name: str) -> bool:
        """
        Remove a process from the registry (stops it first if running).

        Args:
            name: Name of the process to remove

        Returns:
            bool: True if removed successfully, False otherwise
        """
        if name not in self._processes:
            self._logger.error(f"Process '{name}' not found in registry.")
            return False

        # Stop the process if it's running
        if self._processes[name].is_alive():
            self.stop_process(name)

        # Remove from registry
        del self._processes[name]
        self._logger.info(f"Process '{name}' removed from registry.")
        return True

    def get_process_count(self) -> int:
        """
        Get the total number of registered processes.

        Returns:
            int: Number of registered processes
        """
        return len(self._processes)

    def track_process_termination(self, name: str, termination_time: float) -> None:
        """
        Track how long it takes for a process to terminate for future optimization.

        Args:
            name: Name of the process
            termination_time: Time taken for process to terminate in seconds
        """
        if name not in self._process_health:
            self._process_health[name] = {"termination_times": []}

        self._process_health[name]["termination_times"].append(
            termination_time)
        avg_time = sum(self._process_health[name]["termination_times"]) / \
            len(self._process_health[name]["termination_times"])
        self._process_health[name]["avg_termination_time"] = avg_time

        if termination_time > 1.0:  # Consider slow if takes over 1 second
            self._process_health[name]["slow_termination"] = True

    def cleanup(self) -> None:
        """
        Clean up all processes and clear the registry.
        Stops all running processes before clearing.
        """
        # Only cleanup from the main process
        if os.getpid() != self._main_pid:
            self._logger.debug(
                f"Skipping cleanup in child process (PID: {os.getpid()})")
            return

        self.stop_all_processes()
        self._processes.clear()
        self._logger.info("ProcessManager cleanup completed.")

    def __enter__(self) -> "ProcessManager":
        """
        Enter the context manager protocol.

        Returns:
            The ProcessManager instance
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the context manager protocol.
        Ensures all processes are properly cleaned up, even if an exception occurred.

        Args:
            exc_type: Exception type if an exception occurred, None otherwise
            exc_val: Exception value if an exception occurred, None otherwise
            exc_tb: Exception traceback if an exception occurred, None otherwise
        """
        self._logger.info(
            "Exiting ProcessManager context. Cleaning up processes...")
        self.cleanup()
