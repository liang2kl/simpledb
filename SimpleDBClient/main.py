import argparse
import sys
import pygments
from prompt_toolkit import prompt
from prompt_toolkit import print_formatted_text as print
from pygments.lexers.sql import SqlLexer
from prompt_toolkit.lexers import PygmentsLexer
import SimpleDBService.query_pb2_grpc as service
from SimpleDBService.query_pb2 import ExecutionRequest
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
SimpleDB client by: Liang Yesheng <liang2kl@outlook.com>
  Execute:   Option + Enter
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

def send_request(sql: str):
    client.execute(sql)

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
            print(e)
            break
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", "-s", action="store", dest="server_addr",
                        default="127.0.0.0:9100",
                        help="Listening address of SimpleDB gRPC server")
    result = parser.parse_args(sys.argv[1:])

    print(STARTUP_PROMPT)
    print("Run configurations:")
    for k, v in result.__dict__.items():
        print(f"  {k}: {v}")
    print("")

    success = connect_server(result.server_addr)

    if not success:
        print("Error: failed to connect to server, exiting...")
        sys.exit(1)
    
    main_loop()

