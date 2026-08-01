"""
Quick sanity tests for RaftNode's RPC handlers, called directly (no real
network/gRPC server needed since RequestVote/AppendEntries are just
plain methods we can call with fake request objects).
"""

import tempfile
import raft_pb2
from node import RaftNode

# Each test creates its own fresh temp directory (via tempfile.mkdtemp())
# for its RaftNode's persisted state file, so tests never read/write real
# node state and never leak state between each other.


def test_request_vote_grants_when_log_up_to_date():
    node = RaftNode("node1", {}, storage_dir=tempfile.mkdtemp(prefix="raft_test_"))   # empty peers dict - fine for tests that only call handlers directly
    req = raft_pb2.RequestVoteRequest(
        term=1, candidate_id="node2", last_log_index=0, last_log_term=0
    )
    resp = node.RequestVote(req, None)
    assert resp.vote_granted is True
    assert node.voted_for == "node2"
    print("PASS: grants vote to candidate with equally up-to-date (empty) log")


def test_request_vote_rejects_stale_term():
    node = RaftNode("node1", {}, storage_dir=tempfile.mkdtemp(prefix="raft_test_"))
    node.current_term = 5
    req = raft_pb2.RequestVoteRequest(
        term=3, candidate_id="node2", last_log_index=0, last_log_term=0
    )
    resp = node.RequestVote(req, None)
    assert resp.vote_granted is False
    assert resp.term == 5
    print("PASS: rejects RequestVote with stale term")


def test_request_vote_rejects_double_vote_same_term():
    node = RaftNode("node1", {}, storage_dir=tempfile.mkdtemp(prefix="raft_test_"))
    req1 = raft_pb2.RequestVoteRequest(term=1, candidate_id="node2", last_log_index=0, last_log_term=0)
    req2 = raft_pb2.RequestVoteRequest(term=1, candidate_id="node3", last_log_index=0, last_log_term=0)
    resp1 = node.RequestVote(req1, None)
    resp2 = node.RequestVote(req2, None)
    assert resp1.vote_granted is True
    assert resp2.vote_granted is False   # already voted for node2 this term
    print("PASS: rejects a second candidate after already voting this term")


def test_append_entries_heartbeat_success():
    node = RaftNode("node1", {}, storage_dir=tempfile.mkdtemp(prefix="raft_test_"))
    req = raft_pb2.AppendEntriesRequest(
        term=1, leader_id="node2", prev_log_index=0, prev_log_term=0,
        entries=[], leader_commit=0
    )
    resp = node.AppendEntries(req, None)
    assert resp.success is True
    assert node.state == "FOLLOWER"
    print("PASS: empty AppendEntries (heartbeat) succeeds")


def test_append_entries_rejects_on_log_mismatch():
    node = RaftNode("node1", {}, storage_dir=tempfile.mkdtemp(prefix="raft_test_"))
    # Claim there should already be an entry at index 1 with term 1 —
    # but node's log is empty, so this should fail the consistency check.
    req = raft_pb2.AppendEntriesRequest(
        term=1, leader_id="node2", prev_log_index=1, prev_log_term=1,
        entries=[], leader_commit=0
    )
    resp = node.AppendEntries(req, None)
    assert resp.success is False
    print("PASS: rejects AppendEntries when prevLogIndex/Term don't match")


def test_append_entries_appends_and_advances_commit():
    node = RaftNode("node1", {}, storage_dir=tempfile.mkdtemp(prefix="raft_test_"))
    entry1 = raft_pb2.LogEntry(term=1, index=1, command="SET foo bar")
    req = raft_pb2.AppendEntriesRequest(
        term=1, leader_id="node2", prev_log_index=0, prev_log_term=0,
        entries=[entry1], leader_commit=1
    )
    resp = node.AppendEntries(req, None)
    assert resp.success is True
    assert len(node.log) == 1
    assert node.commit_index == 1
    print("PASS: appends new entry and advances commitIndex")


def test_append_entries_truncates_on_conflict():
    node = RaftNode("node1", {}, storage_dir=tempfile.mkdtemp(prefix="raft_test_"))
    # Seed the node with a conflicting entry at index 1, term 1
    node.log = [raft_pb2.LogEntry(term=1, index=1, command="OLD")]

    # Leader sends a DIFFERENT entry at index 1, term 2 -> conflict,
    # our old entry should be discarded and replaced.
    new_entry = raft_pb2.LogEntry(term=2, index=1, command="NEW")
    req = raft_pb2.AppendEntriesRequest(
        term=2, leader_id="node2", prev_log_index=0, prev_log_term=0,
        entries=[new_entry], leader_commit=0
    )
    resp = node.AppendEntries(req, None)
    assert resp.success is True
    assert len(node.log) == 1
    assert node.log[0].command == "NEW"
    print("PASS: truncates conflicting entry and replaces with leader's version")


def test_state_survives_restart():
    """
    Simulates exactly the scenario from real testing: node1 votes for a
    candidate, then the process "crashes" (we just stop using that
    RaftNode instance), and a brand NEW RaftNode instance is created
    pointing at the same storage_dir - simulating a real process restart.
    The new instance should load the persisted vote instead of starting
    fresh at term 0 / voted_for None.
    """
    storage_dir = tempfile.mkdtemp(prefix="raft_test_")

    node_before_crash = RaftNode("node1", {}, storage_dir=storage_dir)
    req = raft_pb2.RequestVoteRequest(term=1, candidate_id="node2", last_log_index=0, last_log_term=0)
    resp = node_before_crash.RequestVote(req, None)
    assert resp.vote_granted is True

    # "Crash" - just stop referencing the old instance, exactly like a
    # killed process would lose all its in-memory state.
    del node_before_crash

    # "Restart" - a fresh instance, same node_id, same storage_dir.
    node_after_restart = RaftNode("node1", {}, storage_dir=storage_dir)
    assert node_after_restart.current_term == 1
    assert node_after_restart.voted_for == "node2"

    # Confirms the real-world bug scenario is now fixed: a second
    # candidate in the SAME term should still be correctly rejected,
    # even after the restart, because we remembered our vote.
    req2 = raft_pb2.RequestVoteRequest(term=1, candidate_id="node3", last_log_index=0, last_log_term=0)
    resp2 = node_after_restart.RequestVote(req2, None)
    assert resp2.vote_granted is False
    print("PASS: voted_for/current_term survive a simulated crash + restart")


def test_log_entries_survive_restart():
    storage_dir = tempfile.mkdtemp(prefix="raft_test_")

    node_before_crash = RaftNode("node1", {}, storage_dir=storage_dir)
    entry = raft_pb2.LogEntry(term=1, index=1, command="SET foo bar")
    req = raft_pb2.AppendEntriesRequest(
        term=1, leader_id="node2", prev_log_index=0, prev_log_term=0,
        entries=[entry], leader_commit=0
    )
    node_before_crash.AppendEntries(req, None)
    del node_before_crash

    node_after_restart = RaftNode("node1", {}, storage_dir=storage_dir)
    assert len(node_after_restart.log) == 1
    assert node_after_restart.log[0].command == "SET foo bar"
    print("PASS: log entries survive a simulated crash + restart")


if __name__ == "__main__":
    test_request_vote_grants_when_log_up_to_date()
    test_request_vote_rejects_stale_term()
    test_request_vote_rejects_double_vote_same_term()
    test_append_entries_heartbeat_success()
    test_append_entries_rejects_on_log_mismatch()
    test_append_entries_appends_and_advances_commit()
    test_append_entries_truncates_on_conflict()
    test_state_survives_restart()
    test_log_entries_survive_restart()
    print("\nAll tests passed.")
