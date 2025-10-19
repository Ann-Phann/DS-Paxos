package com.an.paxos.logic;

/*
 * Generates a unique, monotonically increasing composite proposal number (n).
 * Format: n = (local_counter * ID_MULTIPLIER) + memberId.
 */
public class ProposalNumberGenerator {
    private static final int ID_MULTIPLIER = 10; 
    
    // The memberId: 1 for M1, ..., 9 for M9
    private final int memberId;

    // Local counter to ensure uniqueness
    private volatile int localCounter = 0;

    /**
     * Initializes the generator by extracting the integer ID from the member string ("M1" -> 1).
     * @param memId The member's string ID (e.g., "M1").
     */
    public ProposalNumberGenerator(int memId) {
        // int memberId = Integer.parseInt(memId.substring(1).trim());

        if (memId < 1 || memId > 9) {
            throw new IllegalArgumentException("Member ID must be between 1 and 9");
        }
        this.memberId = memId;
    }

    /*
     * Increments the local counter and generates the next composite proposal number.
     * This method must be synchronized to protect the shared 'counter' variable in a multi-threaded Proposer environment (e.g., when retrying proposals).
     * @return The next unique composite proposal number (n).
     */
    public synchronized int getNextProposalNumber() {
        localCounter++;
        return (localCounter * ID_MULTIPLIER) + memberId;
    }

    /**
     * Updates the local counter if a promise is received with a higher proposal number (n_highest).
     * This ensures the next proposal generated is strictly greater than the highest number seen.
     * @param highestN The highest proposal number seen globally (from a PROMISE).
     */
    public synchronized void updateCounter(int highestN) {
        int highestCounter = highestN / ID_MULTIPLIER;
        
        // generate a number based on a counter strictly greater than the highest counter seen.
        if (highestCounter >= localCounter) {
            localCounter = highestCounter + 1;
        }
    }

    public synchronized void setLocalCounter(int newCounter) {
        if (newCounter > localCounter) {
            this.localCounter = newCounter;
        }
    }
}
