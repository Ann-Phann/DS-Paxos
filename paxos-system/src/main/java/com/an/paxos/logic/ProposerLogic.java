package com.an.paxos.logic;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.an.paxos.CouncilMember;
import com.an.paxos.messages.*;

import com.an.paxos.profile.ConfigReader;
/*
 * Handles all Proposer responsibilities: Phase 1a (Prepare) and Phase 2a (Request Accept).
 */
public class ProposerLogic {
    private final CouncilMember member;
    private final int MAJORITY;
    private static final int TIMEOUT_MS = 15000; // Timeout for waiting responses

    public ProposerLogic(CouncilMember member) {
        this.member = member;
        this.MAJORITY = (ConfigReader.getAllMembersMap().size() / 2) + 1;
    }

    /*
     * Starts the entire proposal process.
     * @param initialValue The initial value to propose.
     * @return True  if a decision was reached, false if the proposal failed/timed out.
     */
    public Boolean propose(int initialValue) {
        while (member.isRunning() && !member.getDecided()) {
            // edge case: check the max valid member id 
            int maxMemberId = ConfigReader.getAllMembersMap().size();
            if (initialValue < 0 || initialValue > maxMemberId) {
                System.out.println("Initial value " + initialValue + " is invalid. It must be between 1 and " + maxMemberId);
                return false;
            }
            // 1. Get new proposal N from member.proposalGenerator.getNextProposalNumber()
            int n = member.getProposalNumberGenerator().getNextProposalNumber();

            // 2. Start Phase 1a (Send PREPARE and wait for PROMISES) 
            List<Promise> promises = phase1aPrepare(n);

            if (promises == null) {
                try {
                    Thread.sleep(2000); // Brief pause before retrying
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue; // Failed to get majority PROMISES, retry with new N
            }

            int v = adoptValue(promises, initialValue);

            boolean accepted = phase2aRequestAccept(n, v);
            if (!accepted) {
                try { Thread.sleep(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                continue; // Failed to get majority ACCEPTED, retry
            }

            phase3Decide(n, v);
            return true; // Proposal succeeded
        }
        return false;
    }

    // ======== Paxos phase methods ========
    /*
     * Phase 1a: Prepare
     * Sends PREPARE (n) messages to all Council Members and waits for a majority of PROMISEs.
     * @param n The proposal number to prepare.
     * @return List of successful Promise messages, or null if majority not reached or rejected.
     */
    private List<Promise> phase1aPrepare(int n) {
        member.promisedResponses.put(n, new CopyOnWriteArrayList<>());
        System.out.println("M" + member.getMemIdInt() + " starting Phase 1a with Prepare(n=" + n + ")");
        
        // NETWORK Stub: Send PREPARE(n) to all members
        Prepare prepareMsg = new Prepare(n);
        member.getMessenger().broadcast(prepareMsg);

        long startTime = System.currentTimeMillis();

        synchronized (member.lock) {
            List<Promise> promises = member.promisedResponses.get(n);

            while (member.isRunning() && (promises == null || promises.size() < MAJORITY)) {
                long timeElapsed = System.currentTimeMillis() - startTime;
                long timeRemaining = TIMEOUT_MS - timeElapsed;

                if (timeRemaining <= 0) {
                    System.out.println(member.getMemIdInt() + " Phase 1a FAILED (Timeout)");
                    member.promisedResponses.remove(n);
                    return null;
                }

                try {
                    // Wait for the Lock to be notified, or until the timeout
                    member.lock.wait(timeRemaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    member.promisedResponses.remove(n);
                    return null;
                }
                
                // Re-read promises list after waking up
                promises = member.promisedResponses.get(n);
            }
        
            // If  exit the loop, the quorum must have been met (or  shut down)
            if (promises != null && promises.size() >= MAJORITY) {
                System.out.println(member.getMemIdInt() + " received majority PROMISES for n=" + n);
                
                // ** 1. FIND THE HIGHEST N ACROSS ALL PROMISES **
                int highestN = n; // Start with our own N
                for (Promise p : promises) {
                    // p.proposalNumber is the highest N Acceptor has PROMISED to
                    if (p.proposalNumber > highestN) {
                        highestN = p.proposalNumber;
                    }
                }

                // ** 2. CRITICAL: UPDATE THE GENERATOR **
                if (highestN > n) {
                    member.getProposalNumberGenerator().updateCounter(highestN);
                    System.out.println("M" + member.getMemIdInt() + " was REJECTED by higher N=" + highestN + ". RETRYING Phase1a with new N");

                    member.promisedResponses.remove(n);
                    return null;
                }

                return promises;
            }
            // Should only be reached if member.isRunning() is false
            member.promisedResponses.remove(n);
            return null;
        }
    }

    /*
     * Phase 2a: Sends REQUEST_ACCEPT(n, v) messages and waits for a majority of ACCEPTEDs.
     * @param n The proposal number.
     * @param v The value to propose.
     * @return True if a majority of acceptors accepted (n, v), false otherwise.
     */
    private boolean phase2aRequestAccept(int n, int v) {
        member.acceptedResponses.put(n, new CopyOnWriteArrayList<>());
        System.out.println(member.getMemIdInt() + " starting Phase 2a with RequestAccept(n=" + n + ", v=" + v + ")");

        // NETWORK Stub: Send REQUEST_ACCEPT(n, v) to all members
        RequestAccept requestAcceptMsg = new RequestAccept(n, v);
        member.getMessenger().broadcast(requestAcceptMsg);

        long startTime = System.currentTimeMillis();

        synchronized (member.lock) {
            List<Accept> accepts = member.acceptedResponses.get(n);

            while (member.isRunning() && (accepts == null || accepts.size() < MAJORITY)) {
                long timeElapsed = System.currentTimeMillis() - startTime;
                long timeRemaining = TIMEOUT_MS - timeElapsed;

                if (timeRemaining <= 0) {
                    System.out.println(member.getMemIdInt() + " Phase 2a FAILED (Timeout)");
                    member.acceptedResponses.remove(n); 
                    return false;
                }

                try {
                    // Wait for the lock to be notified, or until the timeout
                    member.lock.wait(timeRemaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    member.acceptedResponses.remove(n); 
                    return false;
                }
                
                // Re-read accepts list after waking up
                accepts = member.acceptedResponses.get(n);
            }
        
            // If  exit the loop, the quorum must have been met (or  shut down)
            if (accepts != null && accepts.size() >= MAJORITY) {
                System.out.println("M" + member.getMemIdInt() + " received majority ACCEPTED for n=" + n);
                return true;
            }
            // Should only be reached if member.isRunning() is false
            return false;
        }
    }

    private void phase3Decide(int n, int v) {
        System.out.println("M" + member.getMemIdInt() + " starting Phase 3 with DECIDE(n=" + n + ", v=" + v + ")");

        // NETWORK Stub: Send DECIDE(n, v) to all members
        Decide decideMsg = new Decide(n, v);
        member.getMessenger().broadcast(decideMsg);

        // update learner state
        member.setDecided(true);
        member.setDecidedN(n);
        member.setDecidedValue(v);

        // clean up
        member.promisedResponses.remove(n);
        member.acceptedResponses.remove(n);
    }

    // ===== helper =====
    /*
     * Determines the value (v) to propose based on received Promises.
     * Paxos Rule: If any Promise returned an accepted value (v_a) with its N (n_a), 
     * the Proposer must propose the v_a corresponding to the highest n_a seen.
     * @param promises The list of successful PROMISE messages.
     * @param initialValue The value the proposer started with.
     * @return The adopted value.
     */

     private int adoptValue(List<Promise> promises, int initialValue) {
        int adoptedValue = initialValue;
        int highestAcceptedN = -1;

        for (Promise promise : promises) {
            if (promise.acceptedN > highestAcceptedN && promise.acceptedN != -1) {
                highestAcceptedN = promise.acceptedN;
                adoptedValue = promise.acceptedValue;
            }
        }

        if (highestAcceptedN > -1) {
            System.out.println(member.getMemIdInt() + " adopting value V=" + adoptedValue + " from N=" + highestAcceptedN);
        }
        return adoptedValue;
    }
    
    public int getMajority() {
        return MAJORITY;
    }
}