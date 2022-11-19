import SimpleDBService.query_pb2_grpc as service
import grpc
from SimpleDBService.query_pb2 import ExecutionRequest

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
