import argparse
import sys
import pygments
from prettytable import PrettyTable
from prompt_toolkit import prompt, HTML
from prompt_toolkit import print_formatted_text as print
from pygments.lexers.sql import SqlLexer
from prompt_toolkit.lexers import PygmentsLexer
import SimpleDBService.query_pb2_grpc as service
from SimpleDBService.query_pb2 import ExecutionRequest
import SimpleDBService.query_pb2 as query_pb2
import grpc

class SimpleDBClient(service.QueryServicer):
    def __init__(self, addr: str) -> None:
        super().__init__()
        self.addr = addr
        self.channel = None

    def connect(self):
        self.channel = grpc.insecure_channel(self.addr)
        self.stub = service.QueryStub(self.channel)

    def execute(self, sql: str):
        request = ExecutionRequest()
        request.sql = sql
        return self.ExecuteSQLProgram(request, None)

    def ExecuteSQLProgram(self, request, context):
        return self.stub.ExecuteSQLProgram(request)

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
    else:
        print(resp.result)

def connect_server(addr: str):
    global client
    try:
        client = SimpleDBClient(addr)
        client.connect()
    except:
        return False
    return True

def send_request(sql: str):
    try:
        responses = client.execute(sql).responses
    except grpc.RpcError as e:
        print(f"RPC Error ({e.code()}):", e.details())
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
        print(f"  {k.replace('_', ' ')}: {v}")
    print("")

    success = connect_server(result.server_addr)

    if not success:
        print("Error: failed to connect to server, exiting...")
        sys.exit(1)
    
    main_loop()

