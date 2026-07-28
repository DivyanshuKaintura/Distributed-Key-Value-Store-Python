import grpc
import raft_pb2
import raft_pb2_grpc
from concurrent import futures

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def Ping(self, request, context):
        print(f"[Server]: Received Request from {request.user_id} with Term: {request.term}")
        
        response = raft_pb2.PingResponse(message=f"Hello {request.user_id}, I am alive", alive=True)
        return response
    

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(
        RaftServicer(),
        server
    )
    
    server.add_insecure_port(f"0.0.0.0:50051")
    server.start()
    print(f"Server listening on Port: 50051")
    
    server.wait_for_termination()
    
if __name__ == "__main__":
    serve()