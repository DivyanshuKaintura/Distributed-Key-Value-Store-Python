"""
Disk persistence for Raft's persistent state: currentTerm, votedFor, log.

Per the Raft paper (Figure 2), these three fields "must be persisted on
stable storage before responding to RPCs" — meaning a node has to know it
survives a crash with its most recent term/vote/log intact before it's
allowed to tell a peer "yes I voted for you" or "yes I replicated that
entry." Skipping this is what let node1 in the earlier test forget it had
already voted, after a restart.

We use plain JSON on disk. This is not the fastest option (SQLite or a
binary format would be faster for large logs), but it's simple, human
readable while debugging, and fast enough for a learning project.
"""

import json
import os

import raft_pb2


def _state_file_path(node_id: str, storage_dir: str) -> str:
    return os.path.join(storage_dir, f"{node_id}_raft_state.json")


def save_state(node_id: str, storage_dir: str, current_term: int, voted_for, log: list) -> None:
    """
    Writes current_term, voted_for, and log to disk ATOMICALLY.

    Why atomic matters: if we wrote directly to the real file and the
    process crashed mid-write (e.g. power loss, OS kill), we could be left
    with a half-written, corrupted JSON file — which would then fail to
    load on restart, losing the node's state entirely. Instead we:
      1. Write the full new content to a TEMPORARY file
      2. Use os.replace() to atomically swap it into place

    os.replace() is atomic on both POSIX and Windows — the target file
    either ends up as the complete OLD version or the complete NEW
    version, never something in between.
    """
    os.makedirs(storage_dir, exist_ok=True)
    path = _state_file_path(node_id, storage_dir)
    tmp_path = path + ".tmp"

    # Protobuf LogEntry objects aren't directly JSON-serializable, so we
    # convert each one to a plain dict first.
    serializable_log = [
        {"term": entry.term, "index": entry.index, "command": entry.command}
        for entry in log
    ]

    data = {
        "current_term": current_term,
        "voted_for": voted_for,
        "log": serializable_log,
    }

    with open(tmp_path, "w") as f:
        json.dump(data, f)
        f.flush()
        os.fsync(f.fileno())   # force the OS to actually write bytes to disk,
                                # not just hold them in a buffer, before we
                                # consider the write "done"

    os.replace(tmp_path, path)   # atomic rename/swap


def load_state(node_id: str, storage_dir: str):
    """
    Loads persisted state from disk, if it exists.
    Returns (current_term, voted_for, log) — log is reconstructed as a
    list of raft_pb2.LogEntry objects, ready to drop straight into
    RaftNode.log.

    If no state file exists yet (first-ever startup for this node),
    returns fresh defaults: term 0, no vote, empty log.
    """
    path = _state_file_path(node_id, storage_dir)

    if not os.path.exists(path):
        return 0, None, []

    with open(path, "r") as f:
        data = json.load(f)

    log = [
        raft_pb2.LogEntry(term=e["term"], index=e["index"], command=e["command"])
        for e in data.get("log", [])
    ]

    return data.get("current_term", 0), data.get("voted_for"), log
