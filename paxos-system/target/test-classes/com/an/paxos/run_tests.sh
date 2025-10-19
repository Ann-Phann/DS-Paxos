#!/bin/bash

# --- Configuration ---
# Command template to run a member with specific arguments
JAVA_CMD_BASE="mvn exec:java -Dexec.mainClass=com.an.paxos.CouncilMember"

# Function to launch a member in the background
function launch_member() {
    MEMBER_ID=$1
    PROFILE=$2
    
    echo "Launching M${MEMBER_ID} with profile ${PROFILE}..."
    # The command is launched in the background (&)
    # We redirect output to a member-specific log file for better debugging/review
    ( ${JAVA_CMD_BASE} "M${MEMBER_ID} --profile ${PROFILE}" ) >> "logs/M${MEMBER_ID}_scenario1.log" 2>&1 &
}

# --- Client Simulation Function (CRITICAL FOR AUTOMATION) ---
# that connects to a member's port and sends the 'PROPOSE M[X] V[Y]' string.
CLIENT_CMD="java -cp ./target/classes com.an.paxos.ProposalClient"

function trigger_proposal() {
    PROPOSER_ID=$1
    VALUE=$2
    #  members are listening on ports 9001, 9002, etc.
    TARGET_PORT=$((9000 + PROPOSER_ID)) 

    PROPOSAL_STRING="PROPOSE M${PROPOSER_ID} V${VALUE}"
    
    echo "Triggering Proposal: M${PROPOSER_ID} proposes V=${VALUE} via port ${TARGET_PORT}"
    
    # Execute the client command.
    ${CLIENT_CMD} ${TARGET_PORT} "PROPOSE M${PROPOSER_ID} V${VALUE}" &
}

# Function to gracefully stop all members
function cleanup() {
    echo "Terminating all background members..."
    # Use the process name to kill all running instances
    pkill -f "com.an.paxos.CouncilMember"
    sleep 2
}

# --- SCENARIO 1 Execution (Example) ---
cleanup # Start clean
mkdir -p logs

echo "--- Starting Scenario 1: Ideal Network ---"
for i in {1..9}; do
    launch_member $i "reliable"
done

echo "Waiting 5 seconds for connections to establish..."
sleep 5

# M4 proposes V=5
trigger_proposal 4 5

echo "Waiting 5 seconds for consensus..."
sleep 5

echo "--- Scenario 1 Complete ---"
cleanup

# ====================================================================

## Scenario 2: Concurrent Proposals

This test verifies the **Safety** property of Paxos: even when two or more proposers start simultaneously, the system must agree on a single value, and no two members can decide on different values. The conflict resolution logic (Phase 1's $\text{highestPromisedN}$ checks and $\text{updateCounter}$) will be heavily tested here.

### 1. Setup

| Member ID | Profile | Description |
| :---: | :---: | :--- |
| M1-M9 | **reliable** | No artificial delay or failures (keeps the focus strictly on the Paxos conflict resolution). |

### 2. Test Execution Plan

1. Launch all 9 members with the `reliable` profile.
2. Trigger two proposals **simultaneously** (or as close to it as possible):
    * **P1:** $\text{M1 proposes V=1}$.
    * **P2:** $\text{M8 proposes V=8}$.

### 3. Expected Outcome

* **Conflict:** Both M1 and M8 will send $\text{PREPARE}$ messages (e.g., $N=11$ and $N=88$ respectively).
* **Resolution:** One proposal will successfully acquire a majority $\text{PROMISE}$ (likely $N=88$, since it's higher). The loser (M1) will receive a $\text{NACK}$ (a $\text{PROMISE}$ with a higher $\text{highestPromisedN}$), which triggers it to restart its proposal with an even higher $N$ (e.g., $N=91$).
* **Consensus:** The system will eventually agree on a single value (either 1 or 8).

### 4. Bash Script Implementation for Scenario 2

```bash
# --- SCENARIO 2 Execution ---
cleanup # Start clean
echo "--- Starting Scenario 2: Concurrent Proposals (M1 proposes V=1, M8 proposes V=8) ---"

for i in {1..9}; do
    launch_member $i "reliable"
done

echo "Waiting 5 seconds for connections to establish..."
sleep 5

# Trigger two proposals SIMULTANEOUSLY by backgrounding them
echo "Triggering concurrent proposals..."

# P1: M1 proposes V=1
trigger_proposal 1 1 &

# P2: M8 proposes V=8
trigger_proposal 8 8 &

# Wait for both proposals to run their course, including potential retries
echo "Waiting 10 seconds for conflict resolution and consensus..."
sleep 10 

echo "--- Scenario 2 Complete ---"
cleanup