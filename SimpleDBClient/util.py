import pygments
from prettytable import PrettyTable
from prompt_toolkit import print_formatted_text
from prompt_toolkit import HTML

print = print_formatted_text

def print_html(s, *args, **kwargs):
    print(HTML(s), *args, **kwargs)

def print_bold(s, *args, **kwargs):
    print_html(f"<b>{s}</b>", *args, **kwargs)

def print_table(headers, rows):
    table = PrettyTable()
    table.field_names = headers
    table.align = "l"
    for r in rows:
        table.add_row(r)
    print(table)
