"""
RaftNode: core state, RPC handlers, election timer, and candidate/leader logic.

This file now covers BOTH sides of Raft:
  - Receiving side: RequestVote / AppendEntries handlers (a node reacting
    to RPCs from peers) — same as before.
  - Sending side (NEW): a background election timer that, if a node hears
    nothing from a leader for too long, turns it into a CANDIDATE that
    calls RequestVote on every peer and — if it wins a majority — becomes
    LEADER and starts sending heartbeats.

This is the part of Raft that makes the cluster self-organizing: no
external coordinator ever picks a leader, nodes just time out and vote.
"""

import threading
import random
import time
import logging

import grpc
import raft_pb2
import raft_pb2_grpc
import persistence


FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"

# ---------------------------------------------------------------------------
# Timing constants.
#
# The real Raft paper uses ~150-300ms election timeouts, tuned for
# datacenter-grade network latency. We use much longer windows (seconds)
# here purely so that when you watch the logs scroll by, you can actually
# read what's happening instead of it flying past. Once this works, these
# are easy to tune down.
#
# The IMPORTANT property to preserve: election timeout must be
# significantly larger than heartbeat interval, and randomized, so that:
#   (a) a heartbeat has time to arrive and reset the timer before it fires
#   (b) two followers don't time out at the exact same moment and become
#       candidates simultaneously, splitting the vote every single time
# ---------------------------------------------------------------------------
ELECTION_TIMEOUT_MIN = 3.0   # seconds
ELECTION_TIMEOUT_MAX = 6.0   # seconds
HEARTBEAT_INTERVAL = 1.0     # seconds (must be well under ELECTION_TIMEOUT_MIN)
RPC_TIMEOUT = 1.0            # seconds - how long we wait for a single peer to respond


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id: str, peers: dict[str, str], storage_dir: str = "."):
        """
        node_id:     this node's own identifier, e.g. "node1"
        peers:       dict mapping OTHER nodes' ids to their addresses,
                     e.g. {"node2": "localhost:50052", "node3": "localhost:50053"}
                     (does NOT include this node itself)
        storage_dir: directory where this node's persisted state file lives.
                     Each node writes to its OWN file (named after its
                     node_id), so multiple nodes can safely share the same
                     storage_dir if needed (e.g. when testing locally).
        """
        self.node_id = node_id
        self.peers = peers
        self.storage_dir = storage_dir

        # One persistent gRPC channel + stub per peer, created once up
        # front and reused for every RPC. Recreating a channel per call
        # would work but is wasteful — channels are meant to be long-lived.
        self.peer_stubs: dict[str, raft_pb2_grpc.RaftServiceStub] = {}
        for peer_id, address in peers.items():
            channel = grpc.insecure_channel(address)
            self.peer_stubs[peer_id] = raft_pb2_grpc.RaftServiceStub(channel)

        # --- Persistent state (Figure 2) ---
        # NEW: load whatever was last saved to disk, instead of always
        # starting fresh at term 0. On a brand-new node with no state
        # file yet, this just returns the same (0, None, []) defaults
        # as before.
        self.current_term, self.voted_for, self.log = persistence.load_state(
            self.node_id, self.storage_dir
        )

        # --- Volatile state (all servers) ---
        self.commit_index = 0
        self.last_applied = 0

        # --- Volatile state (leaders only) ---
        # Reinitialized every time this node becomes leader.
        self.next_index: dict[str, int] = {}    # peer_id -> next log index to send them
        self.match_index: dict[str, int] = {}    # peer_id -> highest log index known replicated on them

        self.state = FOLLOWER

        # Guards all the state above, since gRPC handler threads, the
        # election timer thread, and the heartbeat thread all touch it.
        self.lock = threading.Lock()

        # ---------------------------------------------------------------
        # Election reset signal.
        # Anytime we grant a vote OR accept a valid AppendEntries from a
        # legitimate current leader, we need to tell the election timer
        # "don't time out right now, something legitimate just happened."
        # A threading.Event is a clean way to do this: the timer thread
        # blocks on event.wait(timeout), and any other thread can call
        # event.set() to wake it up early.
        # ---------------------------------------------------------------
        self._reset_event = threading.Event()

        # Sentinel to allow clean shutdown of background threads later.
        self._running = True

        self._log = logging.getLogger(self.node_id)

    # =====================================================================
    # Helpers for inspecting the log (unchanged from before)
    # =====================================================================
    def _last_log_index(self) -> int:
        return len(self.log)

    def _last_log_term(self) -> int:
        if not self.log:
            return 0
        return self.log[-1].term

    def _get_entry(self, index: int):
        if index < 1 or index > len(self.log):
            return None
        return self.log[index - 1]

    def _persist(self):
        """
        Writes current_term/voted_for/log to disk.

        MUST be called (while still holding self.lock) any time ANY of
        those three fields changes, and BEFORE we let control leave the
        function that changed them — e.g. before returning an RPC
        response, so a crash right after we tell a peer "vote granted"
        can never leave that promise unrecorded on disk.
        """
        persistence.save_state(self.node_id, self.storage_dir, self.current_term, self.voted_for, self.log)

    # =====================================================================
    # RequestVote RPC handler
    # =====================================================================
    def RequestVote(self, request, context):
        with self.lock:
            if request.term < self.current_term:
                return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False)

            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.state = FOLLOWER
                # NEW: term changed and voted_for was cleared — persist
                # BEFORE we go any further, so even if we crash before
                # deciding on the vote below, we never forget we've
                # already seen this higher term.
                self._persist()

            candidate_log_is_up_to_date = (
                request.last_log_term > self._last_log_term()
                or (
                    request.last_log_term == self._last_log_term()
                    and request.last_log_index >= self._last_log_index()
                )
            )
            can_vote = self.voted_for in (None, request.candidate_id)

            if can_vote and candidate_log_is_up_to_date:
                self.voted_for = request.candidate_id
                # NEW: MUST persist the vote before responding — this is
                # the exact scenario from the paper: if we crash after
                # sending "vote_granted=True" but before this write lands
                # on disk, we could restart, forget we voted, and grant a
                # second vote to a different candidate in the same term.
                self._persist()
                # granting a vote means a legitimate election is in
                # progress — reset our own timer so we don't also become
                # a candidate a moment later and split the vote further.
                self._reset_event.set()
                self._log.info(f"Voted for {request.candidate_id} in term {self.current_term}")
                return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=True)

            return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False)

    # =====================================================================
    # AppendEntries RPC handler
    # =====================================================================
    def AppendEntries(self, request, context):
        with self.lock:
            if request.term < self.current_term:
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                # NEW: persist immediately - same reasoning as in RequestVote.
                self._persist()
            self.state = FOLLOWER

            # any valid AppendEntries (including a bare heartbeat) from a
            # same-or-newer-term leader proves a leader exists, so reset
            # our election timer.
            self._reset_event.set()

            if request.prev_log_index > 0:
                prev_entry = self._get_entry(request.prev_log_index)
                if prev_entry is None or prev_entry.term != request.prev_log_term:
                    return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

            log_changed = False
            for new_entry in request.entries:
                existing = self._get_entry(new_entry.index)
                if existing is not None and existing.term != new_entry.term:
                    self.log = self.log[: new_entry.index - 1]
                    existing = None
                if existing is None:
                    self.log.append(new_entry)
                    log_changed = True

            if log_changed:
                # NEW: any entries we actually appended/overwrote must hit
                # disk before we tell the leader "success=True" — otherwise
                # a crash right after responding could lose entries the
                # leader now believes are safely replicated here.
                self._persist()

            if request.leader_commit > self.commit_index:
                self.commit_index = min(request.leader_commit, self._last_log_index())

            return raft_pb2.AppendEntriesResponse(term=self.current_term, success=True)

    # =====================================================================
    # NEW: Election timer — runs forever in a background thread.
    # =====================================================================
    def _run_election_timer(self):
        while self._running:
            timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

            # Block until either the timeout elapses OR someone calls
            # self._reset_event.set() (because we voted for someone, or
            # accepted a heartbeat/AppendEntries from a real leader).
            self._reset_event.clear()
            woken_early = self._reset_event.wait(timeout=timeout)

            if woken_early:
                # A legitimate leader/candidate is active — loop back
                # and wait again with a fresh random timeout.
                continue

            # Timed out with no reset -> no leader has been heard from.
            # Leaders themselves ignore this (they're the ones sending
            # heartbeats, so they'd only reach here if something's wrong,
            # and a leader should not demote itself just from its own
            # timer — real Raft leaders step down only on seeing a higher
            # term from someone else, which AppendEntries/RequestVote
            # handlers already cover above).
            with self.lock:
                if self.state == LEADER:
                    continue
            self._start_election()

    # =====================================================================
    # NEW: Becoming a candidate and requesting votes
    # =====================================================================
    def _start_election(self):
        with self.lock:
            self.state = CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id   # a candidate always votes for itself
            # NEW: must persist before sending out any RequestVote calls —
            # if we crash right after asking for votes but before this
            # write lands, we could restart and vote for someone else in
            # a term we already started campaigning in ourselves.
            self._persist()
            election_term = self.current_term
            last_log_index = self._last_log_index()
            last_log_term = self._last_log_term()

        self._log.info(f"Election timeout -> becoming CANDIDATE for term {election_term}")

        votes_received = 1   # we already voted for ourselves
        votes_needed = (len(self.peers) + 1) // 2 + 1   # majority of the WHOLE cluster (peers + self)

        request = raft_pb2.RequestVoteRequest(
            term=election_term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )

        # Ask every peer for a vote. We do this sequentially for
        # simplicity here — with only 2-3 peers the total latency is
        # negligible, but a production implementation would fire these
        # concurrently (e.g. via threads or async) so one slow/dead peer
        # doesn't delay hearing back from the others.
        for peer_id, stub in self.peer_stubs.items():
            try:
                response = stub.RequestVote(request, timeout=RPC_TIMEOUT)
            except grpc.RpcError:
                # Peer is unreachable/down - just skip it. Raft is
                # designed to make progress as long as a majority of
                # nodes are up, so losing contact with a minority is fine.
                self._log.info(f"RequestVote to {peer_id} failed (unreachable)")
                continue

            with self.lock:
                # If we discover a newer term from anyone's response, we
                # are no longer a valid candidate - step down immediately.
                if response.term > self.current_term:
                    self.current_term = response.term
                    self.state = FOLLOWER
                    self.voted_for = None
                    self._persist()   # NEW
                    return

                # Make sure we're still a candidate IN THIS SAME TERM —
                # it's possible another RPC already moved us on (e.g. we
                # got a heartbeat from a new leader mid-election).
                if self.state != CANDIDATE or self.current_term != election_term:
                    return

            if response.vote_granted:
                votes_received += 1
                self._log.info(f"Received vote from {peer_id} ({votes_received}/{votes_needed} needed)")

            if votes_received >= votes_needed:
                self._become_leader(election_term)
                return

    # =====================================================================
    # NEW: Becoming leader and starting heartbeats
    # =====================================================================
    def _become_leader(self, election_term: int):
        with self.lock:
            # Guard against a late-arriving vote count changing things
            # after we've already moved on somehow.
            if self.state != CANDIDATE or self.current_term != election_term:
                return
            self.state = LEADER
            # Leader state is reinitialized fresh every time (Figure 2):
            # optimistically assume every peer's log matches ours exactly,
            # nextIndex/matchIndex will correct themselves via failed
            # AppendEntries responses once real replication logic is added.
            for peer_id in self.peers:
                self.next_index[peer_id] = self._last_log_index() + 1
                self.match_index[peer_id] = 0

        self._log.info(f"Won election for term {election_term} -> becoming LEADER")

        # Kick off the heartbeat loop in its own thread so this method
        # can return immediately (we don't want to block the caller).
        threading.Thread(target=self._run_heartbeats, args=(election_term,), daemon=True).start()

    # =====================================================================
    # NEW: Leader heartbeat loop
    # =====================================================================
    def _run_heartbeats(self, election_term: int):
        while self._running:
            with self.lock:
                # Stop as soon as we're no longer leader of this term
                # (e.g. we saw a higher term and stepped down).
                if self.state != LEADER or self.current_term != election_term:
                    return
                current_term = self.current_term

            # For now, every heartbeat is an EMPTY AppendEntries (no log
            # entries) — pure "I'm still alive" signal. Real log
            # replication (sending actual entries, tracking nextIndex per
            # peer) is the next step after this one.
            request = raft_pb2.AppendEntriesRequest(
                term=current_term,
                leader_id=self.node_id,
                prev_log_index=self._last_log_index(),
                prev_log_term=self._last_log_term(),
                entries=[],
                leader_commit=self.commit_index,
            )

            for peer_id, stub in self.peer_stubs.items():
                try:
                    response = stub.AppendEntries(request, timeout=RPC_TIMEOUT)
                except grpc.RpcError:
                    continue   # peer unreachable, will retry next heartbeat

                with self.lock:
                    if response.term > self.current_term:
                        self.current_term = response.term
                        self.state = FOLLOWER
                        self.voted_for = None
                        self._persist()   # NEW
                        return

            time.sleep(HEARTBEAT_INTERVAL)

    # =====================================================================
    # Public entrypoint to start this node's background behavior.
    # Call this AFTER the gRPC server is already up and listening, so
    # this node can immediately start receiving RPCs from peers who
    # might contact it as soon as their own timers fire.
    # =====================================================================
    def start(self):
        threading.Thread(target=self._run_election_timer, daemon=True).start()

    def stop(self):
        self._running = False
        self._reset_event.set()
