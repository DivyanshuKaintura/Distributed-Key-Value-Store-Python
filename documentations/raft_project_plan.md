# Raft Consensus Implementation & AWS Deployment — Project Plan

## Overview
Build a Raft consensus algorithm from scratch in Python, expose it as a replicated
key-value store, and deploy a 3-node cluster on AWS EC2.

## Stack
- **Language:** Python
- **Inter-node Raft RPCs:** gRPC + Protocol Buffers
- **Client-facing API:** FastAPI
- **Persistence:** Disk-backed (JSON or SQLite) for `currentTerm`, `votedFor`, and log entries
- **Infrastructure:** 3x AWS EC2 instances, provisioned manually via AWS Console
- **Process management:** systemd (auto-restart on crash/reboot)

---

## Phase 1 — Raft Core Fundamentals (local, single machine)

1. **Protobuf/gRPC crash course**
   - Define `.proto` for `RequestVote` and `AppendEntries` RPCs
   - Generate Python stubs
   - Build a trivial 2-service gRPC ping to get comfortable with the workflow

2. **Node skeleton**
   - States: Follower / Candidate / Leader
   - Persistent state: `currentTerm`, `votedFor`, `log[]`
   - Volatile state: `commitIndex`, `lastApplied`
   - Leader-only state: `nextIndex[]`, `matchIndex[]`

3. **Leader election**
   - Randomized election timeout
   - `RequestVote` RPC logic and vote counting
   - Term handling
   - Heartbeats (`AppendEntries` with empty entries) to suppress elections

4. **Log replication**
   - Client command → leader appends to log → replicates via `AppendEntries`
   - Commits on majority acknowledgment
   - Applies committed entries to the state machine

5. **Persistence layer**
   - Write `currentTerm`, `votedFor`, and log entries to disk on every change
   - Reload state on startup / crash recovery

---

## Phase 2 — KV Store + Client API

6. **State machine**
   - Simple in-memory dict, mutated only via committed log entries

7. **FastAPI REST layer** (separate port from gRPC Raft traffic)
   - `POST /kv/{key}` → propose write, block until committed, return success
   - `GET /kv/{key}` → serve from local state (or forward to leader — decide in Phase 2)
   - `DELETE /kv/{key}` → same commit path as POST
   - Non-leader nodes redirect/forward client requests to the current leader

---

## Phase 3 — Local Multi-Node Testing

8. Run 3 node processes on localhost (different ports) and verify:
   - Leader election happens and stabilizes
   - Writes replicate to all nodes
   - Killing the leader triggers re-election; cluster recovers
   - A restarted node reloads persisted state and catches up

---

## Phase 4 — AWS Deployment

9. Launch 3 EC2 instances via AWS Console
   - Same AMI (likely Ubuntu 22.04/24.04)
   - Same VPC/subnet for simplicity
   - Security groups open for:
     - gRPC port (node-to-node Raft traffic)
     - FastAPI port (client-facing)

10. Install Python + dependencies on each instance
    - Configure each node with its peer list (private IPs) via config file or env vars

11. Run each node as a **systemd service**
    - Auto-restart on crash/reboot
    - Pairs naturally with the disk persistence layer

12. Validate the cluster over the real network
    - Repeat Phase 3 tests (election, replication, failure/recovery) across actual instances/latency

---

## Phase 5 — Polish (optional, time-permitting)

- Basic logging/observability (current leader, term changes)
- CLI or curl-based test script for GET/POST/DELETE
- README documenting design and run instructions

---

## Open Decisions (to resolve as we build)

- **GET consistency:** serve reads directly from a follower's local state (fast, possibly stale)
  vs. forward all reads to the leader (slower, consistent). Start simple, revisit later.
- **AMI/OS choice** for EC2 — Ubuntu 22.04/24.04 is the default assumption; confirm at provisioning time.
- **Persistence format:** JSON file vs. SQLite for the Raft log/state — decide during Phase 1, step 5.

---

## Starting Point

Begin with **Phase 1, Step 1**: define the `.proto` file for `RequestVote` / `AppendEntries`
and build a minimal gRPC ping-pong between two Python processes to get comfortable with the
gRPC/protobuf workflow before writing any Raft logic.
