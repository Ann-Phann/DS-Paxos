package com.an.paxos.messages;

public class Prepare extends Message {
    public int proposalNumber; // The proposal number (n)

    public Prepare(int proposalNumber) {
        super(MessageType.PREPARE);
        this.proposalNumber = proposalNumber;
    }
    
}
