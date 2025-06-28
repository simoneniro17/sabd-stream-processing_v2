from cli.constants import Colors

def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 60}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text.center(60)}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 60}{Colors.ENDC}\n")

def print_info(text: str):
    """Stampa un messaggio informativo."""
    print(f"{Colors.BLUE}ℹ {text}{Colors.ENDC}")

def print_success(text: str):
    """Stampa un messaggio di successo."""
    print(f"{Colors.GREEN}✓ {text}{Colors.ENDC}")

def print_warning(text: str):
    """Stampa un avviso."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.ENDC}")

def print_error(text: str):
    """Stampa un messaggio di errore."""
    print(f"{Colors.RED}✗ {text}{Colors.ENDC}")

def print_phase(text):
    """Stampa l'intestazione di una fase con un divisore"""
    print("\n" + Colors.BLUE + "=" * 60 + Colors.ENDC)
    print(f"{Colors.BLUE}{Colors.BOLD} {text.upper()} {Colors.ENDC}")
    print(Colors.BLUE + "=" * 60 + Colors.ENDC + "\n")