"""Main monitor service module."""
import sys

from loguru import logger as log
from infra.utils import add_log_sink, get_command_line_arguments
from infra.monitor import MonitorService


def main() -> None:
    """Run monitor service.

    Put `with_trace=True` into `safe` decorator like `safe(with_trace=True)`
    to see all tracebacks for debugging. You can find all allowed metrics
    (sqreamd utility functions) in ./infra/monitor.py

    Steps below:
    1) Read arguments from command-line
    2) Add sink to logger if provided
    3) Initialize monitor service (check customer metrics, connections) and run it

    Command-line interface for monitor-service project

    required arguments:
      --username            Sqream database username
      --password            Sqream database password

    optional arguments:
      -h, --help            show this help message and exit
      --host                Sqream ip address (default: `localhost`)
      --port                Sqream port (default: `5000`)
      --database            Sqream database (default: `master`)
      --clustered           Sqream clustered (default: `False`)
      --service             Sqream service (default: `monitor`)
      --loki_host           Loki remote address (default: `localhost`)
      --loki_port           Loki remote port (default: `3100`)
      --log_file_path       Path to file to store logs (default: `None`)

    :return: None
    """

    # 1. Read arguments from command-line
    args = get_command_line_arguments()
    # 2. Add sink to logger if provided
    add_log_sink(args.log_file_path)
    # 3. Initialize monitor service (check customer metrics, connections) and run it
    try:
        MonitorService(**vars(args)).run()
    except Exception as handled_exception:
        log.exception(handled_exception)
        sys.exit(2)


if __name__ == "__main__":
    main()
