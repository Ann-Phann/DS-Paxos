package com.an.paxos.communcation;

import com.an.paxos.CouncilMember;
import com.an.paxos.messages.*;

import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Handles message processing and routing within a CouncilMember.
 * It delegates requests to AcceptorLogic and updates Proposer/Learner state.
 */
public class MessageProcess {
    private final CouncilMember member;
    private final ObjectOutputStream outputStream;

    public MessageProcess(CouncilMember member, ObjectOutputStream outputStream) {
        this.member = member;
        this.outputStream = outputStream;
    }

    /**
     * Delegates the message to the appropriate Paxos role handler.
     * @param message The message received from the peer.
     */
    public void process(Message message) throws IOException {
        // Renamed from private handleIncomingMessage to public process
        switch (message.type) {
            //  Acceptor Role (Receives Requests) 
            case PREPARE:
                handlePrepare((Prepare) message);
                break;
            case REQUEST_ACCEPT:
                handleRequestAccept((RequestAccept) message);
                break;
                
            //  Proposer Role (Receives Responses) 
            case PROMISE:
                handlePromise((Promise) message);
                break;
            case ACCEPTED: // This is the ACCEPTED response message
                handleAccepted((Accept) message);
                break;

            // Learner Role (Receives Final Decision) 
            case DECIDE:
                handleDecide((Decide) message);
                break;
                
            default:
                System.err.println("Unknown message type received: " + message.type);
        }
    }

    // === Acceptor Request Handlers (Call AcceptorLogic, Send Response) ===

    private void handlePrepare(Prepare request) throws IOException {
        Promise response = member.getAcceptorLogic().promise(request.proposalNumber);
        
        // Implicit NACK: Only send the response if it's a PROMISE
        if (response != null) {
            synchronized (outputStream) {
                outputStream.writeObject(response);
                outputStream.flush();
                outputStream.reset();
            }
            
            System.out.println(member.getMemIdInt() + " sent PROMISE for N=" + request.proposalNumber);
        }
    }

    private void handleRequestAccept(RequestAccept request) throws IOException {
        Accept response = member.getAcceptorLogic().accept(request.proposalNumber, request.proposalValue);
        
        // Implicit NACK: Only send the response if it's an ACCEPTED message
        if (response != null) {
            synchronized (outputStream) {
                outputStream.writeObject(response);
                outputStream.flush();
                outputStream.reset();
            }
            
            System.out.println(member.getMemIdInt() + " sent ACCEPTED for N=" + request.proposalNumber);
        }
    }

    // === Proposer Response Handlers (Update CouncilMember State) ===

    private void handlePromise(Promise response) {
        // synchronized (member.lock) {
        // // Only add if the Proposer is currently tracking this proposal number
        //     if (member.promisedResponses.containsKey(response.proposalNumber)) {
        //         // Add the promise to the list
        //         member.promisedResponses.get(response.proposalNumber).add(response);
        //         System.out.println(member.getMemIdInt() + " received PROMISE for N=" + response.proposalNumber);
                
        //         // check for quorum and notify waiting proposer thread
        //         // synchronized (member.lock) {
        //         //     if (member.promisedResponses.get(response.proposalNumber).size() >= member.getProposerLogic().getMajority()) {
        //         //         // wake up all threads waiting on the lock
        //         //         member.lock.notifyAll();
        //         //     }
        //         // }
        //         if (member.promisedResponses.get(response.proposalNumber).size() >= member.getProposerLogic().getMajority()) {
        //             member.lock.notifyAll();
        //         }
        //     }
        // }

        synchronized (member.lock) { 
        
            // 1. Only proceed if the Proposer is currently tracking this proposal number
            if (member.promisedResponses.containsKey(response.proposalNumber)) {
                
                // 2. Add the response (protected by the lock)
                member.promisedResponses.get(response.proposalNumber).add(response);
                System.out.println(member.getMemIdInt() + " received PROMISE for N=" + response.proposalNumber);
                
                // 3. Check for quorum and notify (while still holding the lock)
                if (member.promisedResponses.get(response.proposalNumber).size() >= member.getProposerLogic().getMajority()) {
                    // This call ensures the waiting Proposer is woken up INSTANTLY.
                    member.lock.notifyAll(); 
                }
            }
        }
    }

    private void handleAccepted(Accept response) {
        // synchronized (member.lock) {
        // // Only add if the Proposer is currently tracking this proposal number
        //     if (member.acceptedResponses.containsKey(response.acceptedN)) {
        //         // Add the accept response to the list
        //         member.acceptedResponses.get(response.acceptedN).add(response);
        //         System.out.println(member.getMemIdInt() + " received ACCEPTED for N=" + response.acceptedN);
            
        //         // check for quorum and notify waiting proposer thread
        //         // synchronized (member.lock) {
        //         //     // Check if the Quorum condition is met *after* adding the response
        //         //     if (member.acceptedResponses.get(response.acceptedN).size() >= member.getProposerLogic().getMajority()) {
        //         //         // Wake up all threads waiting on this lock (i.e., the Proposer thread)
        //         //         member.lock.notifyAll();
        //         //     }
        //         // }

        //         if (member.acceptedResponses.get(response.acceptedN).size() >= member.getProposerLogic().getMajority()) {
        //             member.lock.notifyAll();
        //         }
        //     }
        // }
        synchronized (member.lock) {
            if (member.acceptedResponses.containsKey(response.acceptedN)) {
                
                // 1. Add the response (protected by the lock)
                member.acceptedResponses.get(response.acceptedN).add(response);
                System.out.println(member.getMemIdInt() + " received ACCEPTED for N=" + response.acceptedN);
            
                // 2. Check for quorum and notify (while still holding the lock)
                if (member.acceptedResponses.get(response.acceptedN).size() >= member.getProposerLogic().getMajority()) {
                    member.lock.notifyAll();
                }
            }
        }
    }
    
    // === Learner Handler ===
    private void handleDecide(Decide request) {
        // Learner role: Update local decision state and stop Proposer retries
        member.setDecidedN(request.decidedN);
        member.setDecidedValue(request.decidedValue);
        member.setDecided(true);
        System.out.println("CONSENSUS: M" + request.decidedValue + " has been elected Council President!");
    }
}
