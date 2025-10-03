class Logger:
    """A logging utility for producing clean, colored terminal output."""

    # ANSI color codes
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    PURPLE = "\033[95m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    @staticmethod
    def info(message):
        """Print an info message in blue text."""
        print(f"{Logger.BLUE}{message}{Logger.RESET}")

    @staticmethod
    def success(message):
        """Print a success message in green text."""
        print(f"{Logger.GREEN}{message}{Logger.RESET}")

    @staticmethod
    def warning(message):
        """Print a warning message in yellow text."""
        print(f"{Logger.YELLOW}{message}{Logger.RESET}")

    @staticmethod
    def error(message):
        """Print an error message in red text."""
        print(f"{Logger.RED}{message}{Logger.RESET}")

    @staticmethod
    def header(message):
        """Print a header message in purple and bold text."""
        print(f"{Logger.PURPLE}{Logger.BOLD}{message}{Logger.RESET}")

    @staticmethod
    def step(title, description):
        """Print formatted step information."""
        print(f"\n{Logger.BLUE}{Logger.BOLD}{title}{Logger.RESET}: {description}")