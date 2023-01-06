import pygments
from prettytable import PrettyTable
from prompt_toolkit import print_formatted_text
from prompt_toolkit import HTML, ANSI

# print = print_formatted_text

class Colors:
    """ ANSI color codes """
    BLACK = "\033[0;30m"
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    BROWN = "\033[0;33m"
    BLUE = "\033[0;34m"
    PURPLE = "\033[0;35m"
    CYAN = "\033[0;36m"
    LIGHT_GRAY = "\033[0;37m"
    DARK_GRAY = "\033[1;30m"
    LIGHT_RED = "\033[1;31m"
    LIGHT_GREEN = "\033[1;32m"
    YELLOW = "\033[1;33m"
    LIGHT_BLUE = "\033[1;34m"
    LIGHT_PURPLE = "\033[1;35m"
    LIGHT_CYAN = "\033[1;36m"
    LIGHT_WHITE = "\033[1;37m"
    BOLD = "\033[1m"
    FAINT = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    NEGATIVE = "\033[7m"
    CROSSED = "\033[9m"
    END = "\033[0m"

def print_html(s, *args, **kwargs):
    print_formatted_text(HTML(s), *args, **kwargs)

def print_bold(s, *args, **kwargs):
    print_html(f"<b>{s}</b>", *args, **kwargs)

def print_table(headers, rows, true_num=-1):
    if len(rows) == 0:
        print("Empty set")
        return
    table = PrettyTable()
    # prettytable will raise "Field names must be unique" on duplicate column
    # names, but we just need that.
    table._validate_field_names = lambda *a, **k: None
    table.field_names = headers
    table.align = "l"
    for r in rows:
        table.add_row(r)
    print(table)
    print(f"{len(rows) if true_num < 0 else true_num} rows in set")

def print_duration(duration):
    if (duration > 1e6):
        desc = f"{duration / 1e6:.3f}s"
    elif (duration > 1e3):
        desc = f"{duration / 1e3:.3f}ms"
    else:
        desc = f"{duration:.3f}us"
    print(Colors.FAINT + desc + Colors.END)

def transform_sql_literal(val: str):
    try:
        float(val)
        return val
    except ValueError:
        return "'" + val + "'"