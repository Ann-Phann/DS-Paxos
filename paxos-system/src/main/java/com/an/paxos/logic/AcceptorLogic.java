package com.an.paxos.logic;

import com.an.paxos.CouncilMember;
import com.an.paxos.messages.Accept;
import com.an.paxos.messages.Promise;

/*
 * Handles all Acceptor responsibilities: Phase 1b (Promise) and Phase 2b (Accept).
 * It delegates back to the CouncilMember to retrieve/update shared state.
 */
public class AcceptorLogic {
    private final CouncilMember member;

    public AcceptorLogic(CouncilMember member) {
        this.member = member;
    }

    /*
     * Phase 1b: Promise 
     * Responds to a Prepare (n) message from a Proposer.
     * If n > highestPromisedN, updates highestPromisedN and returns a PROMISE response with acceptedN and acceptedValue.
     * Otherwise, returns a NACK response with the current highestPromisedN.
     * @param n The proposal number from the Proposer.
     * @return A Promise object representing either a PROMISE.
     */
    public synchronized Promise promise (int n) {        
        if (n < member.getHighestPromisedN()) {
            return null; // Indicate reject the prepare request - NACK implicitly
        } 
        member.setHighestPromisedN(n);
        return new Promise(n, member.getAcceptedN(), member.getAcceptedValue());
    }

    /*
     * Phase 2b: Accept
     * Responds to an RequestAccept (n, v) message from a Proposer.
     * If n >= highestPromisedN, updates acceptedN and acceptedValue, and returns an ACCEPTED response.
     * Otherwise, returns a NACK response with the current highestPromisedN.
     * @param n The proposal number from the Proposer.
     * @param v The value from the Proposer.
     * @return A Promise object representing an ACCEPTED .
     */
    public synchronized Accept accept(int n, int v) {
        if (n < member.getHighestPromisedN()) {
           return null; // Indicate reject the accept request - NACK implicitly
        }
        member.setAcceptedN(n);
        member.setAcceptedValue(v);
        return new Accept(n, v);
    }
}
