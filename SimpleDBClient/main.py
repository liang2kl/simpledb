import argparse
import sys
import csv
from prompt_toolkit import prompt
from pygments.lexers.sql import SqlLexer
from prompt_toolkit.lexers import PygmentsLexer
import SimpleDBService.query_pb2 as query_pb2
import grpc

from util import print_html, print_bold, print_table, print_duration, transform_sql_literal
from client import SimpleDBClient

lexer = PygmentsLexer(SqlLexer)

STARTUP_PROMPT = """\
<b>SimpleDB Client</b>
  By: Liang Yesheng &lt;liang2kl@outlook.com&gt;
"""

USAGE = """\
<b>Usage</b>
  Execute:   Esc  + Enter
  Discard:   Ctrl + C
  Terminate: Ctrl + D
"""

client: SimpleDBClient = None
current_db = ""

def connect_server(addr: str):
    global client
    try:
        client = SimpleDBClient(addr)
        client.connect()
    except:
        return False
    return True

def value_desc(value) -> str:
    if value.HasField("int_value"):
        return str(value.int_value)
    elif value.HasField("float_value"):
        return str(value.float_value)
    elif value.HasField("varchar_value"):
        return value.varchar_value
    elif value.HasField("null_value"):
        return "NULL"

def print_resp(resp):
    if (resp.HasField("error")):
        err_name = query_pb2.ExecutionError.Type.Name(resp.error.type)
        print_bold(f"ERROR", end="")
        print_bold(f"({err_name}):", resp.error.message)
    elif resp.result.HasField("plain"):
        print(resp.result.plain.msg, end = "")
        if resp.result.plain.affected_rows >= 0:
            print(f", {resp.result.plain.affected_rows} rows affected")
        else:
            print("")
    elif resp.result.HasField("show_databases"):
        print_table(["Database"],
                    [[x] for x in resp.result.show_databases.databases])
    elif resp.result.HasField("show_table"):
        print_table(["Table"],
                    [[x] for x in resp.result.show_table.tables])
    elif resp.result.HasField("describe_table"):
        print_table(["Field", "Type", "Null", "Key", "Default"],
                    [[x.field, x.type, "YES" if x.nullable else "NO", "PRI" if x.primary_key else "",
                      x.default_value] for x in resp.result.describe_table.columns])
    elif resp.result.HasField("show_indexes"):
        print_table(["Table", "Column", "Key Name"],
                    [[x.table, x.column, "PRIMARY" if x.is_pk else x.column]
                        for x in resp.result.show_indexes.indexes])
    elif resp.result.HasField("query"):
        num = len(resp.result.query.rows)
        print_num = num
        if num > 100:
            try:
                text = prompt(f"Too many rows ({num}), print? [y/N/<num>] (default: N) ")
            except EOFError:
                return
            except KeyboardInterrupt:
                return
            try:
                print_num = int(text)
                if (print_num <= 0):
                    return
                print_num = print_num if print_num < num else num
                print(f"Showing the first {print_num} rows")
            except ValueError:
                if text != "y":
                    return
        print_table([x.name for x in resp.result.query.columns],
                    [[value_desc(x) for x in row.values] for row in resp.result.query.rows[:print_num]],
                    true_num=num)
    else:
        print(resp.result)

def send_request(sql: str):
    try:
        batch_resp = client.execute(sql)
        responses = batch_resp.responses
    except grpc.RpcError as e:
        print(f"RPC Error ({e.code()}):", e.details(), end="\n\n")
        return
    
    if len(responses) == 1:
        print_resp(responses[0])
    else:
        for i, resp in enumerate(responses):
            print_bold(f"Result [{i}]")
            print_resp(resp)
    
    print_duration(batch_resp.stats.elapse)
    print("")
    
    if len(responses) > 0:
        global current_db
        current_db = responses[-1].current_db

def send_request_silent(sql: str):
    try:
        batch_resp = client.execute(sql)
        responses = batch_resp.responses
    except grpc.RpcError as e:
        return f"RPC Error ({e.code()}): " + e.details()

    if len(responses) != 1:
        return "Multiple responses"
    resp = responses[0]
    if resp.HasField("error"):
        err_name = query_pb2.ExecutionError.Type.Name(resp.error.type)
        return f"ERROR ({err_name}): " + resp.error.message
    return None

def main_loop():
    while True:
        try:
            text = prompt(">>> " + (f"({current_db}) " if current_db else ""),
                          multiline=True, lexer=lexer)
            send_request(text)
        except KeyboardInterrupt:
            continue
        except EOFError:
            break
        except BaseException as e:
            print("Exception occured:", e)
            break
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", "-s", action="store", dest="server_addr",
                        default="127.0.0.0:9100",
                        help="Listening address of SimpleDB gRPC server")
    parser.add_argument("--csv", action="store", dest="csv_file",
                        help="CSV file to import")
    parser.add_argument("--db", "-d", action="store", dest="db_name", help="Database name to import")
    parser.add_argument("--table", "-t", action="store", dest="table_name", help="Table name to import")

    result = parser.parse_args(sys.argv[1:])

    print_html(STARTUP_PROMPT)
    print_bold("Run configurations")
    for k, v in result.__dict__.items():
        if v is not None:
            print(f"  {k.replace('_', ' ').capitalize()}: {v}")
    print("")

    success = connect_server(result.server_addr)

    if not success:
        print("Error: failed to connect to server, exiting...")
        sys.exit(1)
    
    if result.csv_file is not None:
        print(f"Importing {result.csv_file} to {result.db_name}.{result.table_name}...")
        err = send_request_silent("USE " + result.db_name + ";")
        if err is not None:
            print(err)
            exit(1)
        with open(result.csv_file, "r") as f:
            reader = csv.reader(f)
            lines = 0
            for row in reader:
                row = [transform_sql_literal(r) for r in row]
                sql = f"INSERT INTO {result.table_name} VALUES ({','.join(row)});"
                err = send_request_silent(sql)
                if err is not None:
                    print(err + " (" + sql + ")")
                    exit(1)
                else:
                    lines += 1
                    print("Processed %d rows" % lines, end="\r")
    else:
        print_html(USAGE)
        main_loop()

