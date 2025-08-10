"""
Snail Trader CLI Welcome Page
Simple welcome page with mode selection
"""

from pathlib import Path


class Colors:
    """Simple color codes"""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


def colorize(text: str, color: str) -> str:
    """Add color to text"""
    return f"{color}{text}{Colors.RESET}"


def display_welcome():
    """Display welcome message with colors"""
    welcome_file = Path(__file__).parent / "welcome.txt"
    if welcome_file.exists():
        welcome_text = welcome_file.read_text()
        lines = welcome_text.split('\n')
        for line in lines:
            if '🐌' in line and 'Welcome' in line:
                print(colorize(line, Colors.YELLOW + Colors.BOLD))
            elif any(char in line for char in ['_', '/', '\\', '|', '═']):
                print(colorize(line, Colors.CYAN))
            elif line.startswith('•'):
                print(colorize(line, Colors.GREEN))
            else:
                print(line)
    else:
        print(colorize("🐌 Welcome to Snail Trader!", Colors.YELLOW + Colors.BOLD))
    print()


def get_mode_selection() -> str:
    """Get user's mode selection with colors"""
    modes = {
        '1': 'live_trading',
        '2': 'backtest', 
        '3': 'component'
    }
    
    print(colorize("Available Trading Modes:", Colors.WHITE + Colors.BOLD))
    print(colorize("1. Live Trading ", Colors.RED + Colors.BOLD) + " - Execute real-time trading strategies")
    print(colorize("2. Backtest     ", Colors.BLUE + Colors.BOLD) + " - Test and validate trading algorithms")
    print(colorize("3. Component    ", Colors.GREEN + Colors.BOLD) + " - Develop and debug individual modules")
    print()
    
    while True:
        try:
            choice = input(colorize("Please select mode [1-3]: ", Colors.YELLOW)).strip()
            if choice in modes:
                selected_mode = modes[choice]
                print(colorize(f"✅ Selected mode: {selected_mode}", Colors.GREEN))
                return selected_mode
            else:
                print(colorize("❌ Invalid selection. Please choose 1, 2, or 3.", Colors.RED))
        except KeyboardInterrupt:
            print(colorize("\n\n👋 Goodbye!", Colors.CYAN))
            exit(0)


def run_welcome():
    """Run welcome page and return selected mode"""
    display_welcome()
    mode = get_mode_selection()
    print(colorize(f"\n🚀 Starting Snail Trader in {mode} mode...", Colors.CYAN + Colors.BOLD))
    return mode