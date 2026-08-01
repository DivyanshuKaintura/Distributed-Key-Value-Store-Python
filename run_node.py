"""
Entrypoint to run ONE Raft node as its own OS process.

Usage (run each in a separate terminal to form a 3-node cluster):

  python run_node.py --id node1 --port 50051 --peers node2=localhost:50052,node3=localhost:50053
  python run_node.py --id node2 --port 50052 --peers node1=localhost:50051,node3=localhost:50053
  python run_node.py --id node3 --port 50053 --peers node1=localhost:50051,node2=localhost:50052

Each process runs the exact same code — there's no special "first node" or
config difference beyond its own id/port and its peer list. This mirrors
how you'll later run this same script on 3 separate EC2 instances, just
swapping localhost:PORT for each instance's private IP.
"""

import argparse
import logging
import time
from concurrent import futures

import grpc

import raft_pb2_grpc
from node import RaftNode


def parse_peers(peers_arg: str) -> dict[str, str]:
    """
    Parses "node2=localhost:50052,node3=localhost:50053" into
    {"node2": "localhost:50052", "node3": "localhost:50053"}
    """
    peers = {}
    if not peers_arg:
        return peers
    for entry in peers_arg.split(","):
        peer_id, address = entry.split("=")
        peers[peer_id.strip()] = address.strip()
    return peers


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="This node's id, e.g. node1")
    parser.add_argument("--port", required=True, type=int, help="Port this node listens on")
    parser.add_argument(
        "--peers", default="",
        help="Comma-separated peer_id=host:port list, e.g. node2=localhost:50052,node3=localhost:50053"
    )
    parser.add_argument(
        "--storage-dir", default=".",
        help="Directory to store this node's persisted state file (default: current directory)"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    peers = parse_peers(args.peers)

    # Create the node (this also opens gRPC channels to each peer, though
    # nothing is sent over them until an RPC is actually called).
    node = RaftNode(node_id=args.id, peers=peers, storage_dir=args.storage_dir)

    # Stand up the gRPC server so this node can RECEIVE RequestVote /
    # AppendEntries calls from peers.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(node, server)
    server.add_insecure_port(f"0.0.0.0:{args.port}")
    server.start()

    print(f"[{args.id}] gRPC server listening on port {args.port}, peers={peers}")

    # NOW start the election timer - only after the server is already
    # listening, so this node is ready to receive RPCs the instant its
    # timer might fire or a peer's does.
    node.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[{args.id}] shutting down...")
        node.stop()
        server.stop(grace=1)


if __name__ == "__main__":
    main()
