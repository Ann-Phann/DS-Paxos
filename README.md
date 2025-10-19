# Distributed Paxos Implementation 
The project utilise Maven to set up the folder structure.

The project implements the core logic of classic Paxos consensus algorithm to simulate the voting procedure. There will be 9 Council Members (M1 - M9), each member must be capable of acting in all three Paxos roles, including:
- **Proposer**:  Initiates a proposal to elect a candidate, which will be handled by `ProposerLogic.java`.
- **Acceptor**: Considers and votes on proposals from Proposers, which will be handled by `AcceptorLogic.java`.
- **Learner**: Learns the final, decided-upon outcome of the election, which will be handled by `ProposerLogic.java`.

## Main Components
1. `CouncilMember`
- This is the main, where it starts reading the `network.config`. 
- Centralised hub for the two threads acting as client-server.
- Store the required state and data structures for Paxos system to send and receive messages.

2. `Messenger`
- This class is intended to handle message sending functionalities. 
- Include retry mechanisms, `broadcast()` for a Council Member to send to all its peers.

3. `ConnectionHandler`
- `ConnectionHandler` manages incoming messages from a peer Council Member.
- It reads messages from the input stream, simulates network conditions.

## Getting Started
### How to run
Before doing anything, run this into terminal to get into the correct directory:
```
cd paxos-system
```

To compile project:
```
make compile
```

The easiest way to run is to go through the interactive environment I made named `Test1.java`, which is get initialised with all members'profile to become `RELIABLE`.

You can send a `PREPARE` message through command line as demonstrate below:
```
# To run the Test1.java
make test NUM=1


# Once all the members are running, their connections to all the peers are established.
# CONSOLE: Use the format 'PROPOSE M[ID] V[VALUE]' (e.g., PROPOSE M4 V5) or 'QUIT'
# For example:

PROPOSE M4 V5
```

Now you can see their interactions.

### Running Tests
As mentioned above, we can run the test files using the following command line:
```
# NUM_TEST_ID can either be 1, 2, or 3.
make test NUM=<NUM_TEST_ID>
```

## Testing Scenarios
### Scenario 1: The Ideal Network
**Set up:**
All 9 members are launched with the reliable profile (i.e., no artificial delays or failures).

**Test:**
Trigger a single proposal from one member (e.g., M4 proposes M5 for president).

**Expected Outcome:**
Consensus is reached quickly and correctly. All members should output that M5 was elected.

### Scenario 2: Concurrent Proposals
**Set up:**
All 9 members are launched with the reliable profile.

**Test:**
Trigger two different proposals at approximately the same time (e.g., M1 proposes "M1" and M8 proposes "M8").

**Expected Outcome:**
The Paxos algorithm correctly resolves the conflict, and all members reach a consensus on a single winner.

### Scenario 2: Fault-Tolerance
**Set up:**
Launch the members with a mix of profiles:
- M1 (reliable)
- M2 (latent)
- M3 (failure)
- M4–M9 (standard)

**Test Sub-Scenarios:**
- **3a**: A standard member (M4) initiates a proposal.
- **3b**: A latent member (M2) initiates a proposal. Despite its high latency, the system should still reach consensus.
- **3c**: The failing member (M3) initiates a proposal, sends its PREPARE messages, and then "crashes" (terminates). The system must not stall; another member should be able to initiate a new proposal and successfully drive the election to a consensus.

**Expected Outcome:**
For all sub-scenarios, the remaining operational members must successfully reach consensus on a single winner.