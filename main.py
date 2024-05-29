from infra.utils import get_command_line_arguments, safe, do_startup_checkups
from infra.monitor import run_monitor


@safe()
def main() -> None:
    """
    Main function to run monitor service

    put `with_trace=True` into `safe` decorator to see traceback for debugging

    Steps:
    1) Read arguments from command-line
    2) Do checkups (are metrics allowed, is connection established)
    3) Run monitor service is everything is ok, raise an exception otherwise
    :return: None
    """
    # 1. Read arguments from command-line
    args = get_command_line_arguments()
    # 2. Do checkups (are metrics allowed, is connection established)
    do_startup_checkups(args)
    # 3. Run monitor service is everything is ok, raise an exception otherwise
    run_monitor(args)


if __name__ == "__main__":
    main()
