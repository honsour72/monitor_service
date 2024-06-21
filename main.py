"""Main monitor service module."""
from infra.monitor import run_monitor
from infra.startups import do_startup_checkups
from infra.utils import add_log_sink, get_command_line_arguments, safe


@safe()
def main() -> None:
    """Run monitor service.

    Put `with_trace=True` into `safe` decorator like `safe(with_trace=True)`
    to see all tracebacks for debugging. You can find all allowed metrics
    (sqreamd utility functions) in ./infra/utils.py

    Steps below:
    1) Read arguments from command-line
    2) Add sink to logger if provided
    3) Do checkups (are metrics allowed, is connection established)
    4) Run monitor service is everything is ok, raise an exception otherwise

    usage: main.py [-h --help] [--host] [--port] [--database]
                   --username --password [--clustered] [--service]
                   [--loki_host] [--loki_port] [--log_file_path]

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
    # 3. Do checkups (are metrics allowed, is connection established)
    do_startup_checkups(username=args.username,
                        password=args.password,
                        host=args.host,
                        port=args.port,
                        database=args.database,
                        clustered=args.clustered,
                        service=args.service,
                        loki_host=args.loki_host,
                        loki_port=args.loki_port,
                        )
    # 4. Run monitor service is everything is ok, raise an exception otherwise
    run_monitor(loki_host=args.loki_host, loki_port=args.loki_port)


if __name__ == "__main__":
    main()
