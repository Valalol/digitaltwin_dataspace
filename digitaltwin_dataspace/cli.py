import dotenv

dotenv.load_dotenv()

from multiprocessing import Process

from .configuration.load import (
    load_all_components,
)
from .data.sync_db import sync_db_from_configuration
from .runners import (
    run_collector,
    run_collector_on_schedule,
    run_handlers,
    run_harvester,
    run_harvester_on_schedule,
)

import argparse
import logging


def setup_logging(level):
    logging.basicConfig(level=level)
    logging.getLogger("Handler").setLevel(level)
    logging.getLogger("Collector").setLevel(level)
    logging.getLogger("Harvester").setLevel(level)


def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Run specific handlers, collectors, and harvesters."
    )
    # add a --config-file argument to specify the configuration file

    parser.add_argument(
        "--config-folder",
        type=str,
        default="config",
        help="Path to the configuration folder containing the components yaml files.",
    )

    parser.add_argument(
        "--init-dependencies",
        action="store_true",
        help=(
            "Runs all harvesters in the order that maximizes the number of dependencies "
            "that are satisfied."
        ),
    )
    parser.add_argument(
        "--handlers",
        nargs="*",
        default=[],
        help="List of handler names to run.",
    )
    parser.add_argument(
        "--collectors",
        nargs="*",
        default=[],
        help="List of collector names to run.",
    )
    parser.add_argument(
        "--harvesters",
        nargs="*",
        default=[],
        help="List of harvester names to run.",
    )
    parser.add_argument(
        "--now",
        action="store_true",
        help="Run harvesters or collectors once and exit.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8888,
        help="Port to run the handlers server on (default: 8888).",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="Host to run the handlers server on (default: localhost).",
    )
    parser.add_argument(
        "--allowed-hosts",
        nargs="*",
        default=["localhost", "127.0.0.1"],
        help="Allowed hosts for the handlers server (default: localhost, 127.0.0.1).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="WARNING",
        help="Set the logging level (default: WARNING).",
    )
    parser.add_argument(
        "--parquetize",
        nargs="*",
        default=[],
        help="List of harvester names to run.",
    )

    return parser.parse_args()


def launch_harvesters(args, config, processes, tables):
    """
    Launch harvester processes.

    Parameters:
        args (argparse.Namespace): Parsed command-line arguments.
        config: Configuration object.
        processes (list): List to append the created processes.
        tables (dict): Tables from the database synchronization.
    """
    harvester_names_to_run = (
        config.harvesters.keys() if "all" in args.harvesters else args.harvesters
    )

    for name, harvester_config in config.harvesters.items():
        if name in harvester_names_to_run:
            process = Process(
                target=run_harvester if args.now else run_harvester_on_schedule,
                args=(harvester_config, tables),
            )
            process.start()
            processes.append(process)


def launch_collectors(args, config, processes, tables):
    """
    Launch collector processes.

    Parameters:
        args (argparse.Namespace): Parsed command-line arguments.
        config: Configuration object.
        processes (list): List to append the created processes.
        tables (dict): Tables from the database synchronization.
    """
    collector_names_to_run = (
        config.collectors.keys() if "all" in args.collectors else args.collectors
    )

    for name, collector_config in config.collectors.items():
        if name in collector_names_to_run:
            process = Process(
                target=run_collector if args.now else run_collector_on_schedule,
                args=(collector_config, tables[name]),
                kwargs={"fail_on_error": False},
            )
            process.start()
            processes.append(process)


def launch_handlers(args, config, processes, tables):
    """
    Launch handlers server process.

    Parameters:
        args (argparse.Namespace): Parsed command-line arguments.
        config: Configuration object.
        processes (list): List to append the created processes.
        tables (dict): Tables from the database synchronization.
    """
    handlers_names_to_run = (
        config.handlers.keys() if "all" in args.handlers else args.handlers
    )

    handlers_to_run = {
        name: config.handlers[name]
        for name in handlers_names_to_run
        if name in config.handlers
    }

    if handlers_to_run:
        if args.now:
            raise ValueError("Cannot run handlers with --now flag.")
        handler_process = Process(
            target=run_handlers,
            args=(handlers_to_run, tables, args.host, args.port, args.allowed_hosts),
        )
        handler_process.start()
        processes.append(handler_process)


def main():
    args = parse_arguments()
    config = load_all_components(args.config_folder)
    tables = sync_db_from_configuration(config)
    setup_logging(args.log_level)

    processes = []

    # Launch handlers server
    launch_handlers(args, config, processes, tables)

    # Launch collectors
    launch_collectors(args, config, processes, tables)

    # Launch harvesters
    launch_harvesters(args, config, processes, tables)

    # If no processes were started, display a message
    if not processes:
        logging.warning("No handlers, collectors, or harvesters were specified to run.")
        return

    for process in processes:
        process.join()
