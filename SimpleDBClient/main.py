import argparse
import sys
from prompt_toolkit import prompt
from pygments.lexers.sql import SqlLexer
from prompt_toolkit.lexers import PygmentsLexer
import SimpleDBService.query_pb2 as query_pb2
import grpc

from util import print_html, print_bold, print_table, print
from client import SimpleDBClient

lexer = PygmentsLexer(SqlLexer)

STARTUP_PROMPT = """\
<b>SimpleDB Client</b>
  By: Liang Yesheng &lt;liang2kl@outlook.com&gt;

<b>Usage</b>
  Execute:   Esc  + Enter
  Discard:   Ctrl + C
  Terminate: Ctrl + D
"""

client: SimpleDBClient = None

def connect_server(addr: str):
    global client
    try:
        client = SimpleDBClient(addr)
        client.connect()
    except:
        return False
    return True

def print_resp(resp):
    if (resp.HasField("error")):
        err_name = query_pb2.ExecutionError.Type.Name(resp.error.type)
        print_bold(f"ERROR", end="")
        print_bold(f"({err_name}):", resp.error.message)
    elif resp.result.HasField("plain"):
        print(resp.result.plain.msg)
    elif resp.result.HasField("show_databases"):
        print_table(["Database"],
                    [[x] for x in resp.result.show_databases.databases])
    elif resp.result.HasField("show_table"):
        print_table(["Table"],
                    [[x] for x in resp.result.show_table.tables])
    elif resp.result.HasField("describe_table"):
        print_table(["Field", "Type", "Nullable", "Default"],
                    [[x.field, x.type, x.nullable, x.default_value] for x in resp.result.describe_table.columns])
    else:
        print(resp.result)

def send_request(sql: str):
    try:
        responses = client.execute(sql).responses
    except grpc.RpcError as e:
        print(f"RPC Error ({e.code()}):", e.details(), end="\n\n")
        return
    
    if len(responses) == 1:
        print_resp(responses[0])
    else:
        for i, resp in enumerate(responses):
            print_bold(f"Result [{i}]")
            print_resp(resp)
    print("")

def main_loop():
    while True:
        try:
            text = prompt(">>> ", multiline=True, lexer=lexer)
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
    result = parser.parse_args(sys.argv[1:])

    print_html(STARTUP_PROMPT)
    print_bold("Run configurations")
    for k, v in result.__dict__.items():
        print(f"  {k.replace('_', ' ').capitalize()}: {v}")
    print("")

    success = connect_server(result.server_addr)

    if not success:
        print("Error: failed to connect to server, exiting...")
        sys.exit(1)
    
    main_loop()

