import grpc
import raft_pb2
import raft_pb2_grpc

def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = raft_pb2_grpc.RaftStub(channel)

        request = raft_pb2.PingRequest(
            user_id=1002,
            term=1
        )
    
        response = stub.Ping(request)
        
        print(f"[Client]: Received Response: {response.message}\nAlive status: {response.alive}")
        
if __name__ == "__main__":
    run()