package com.an.paxos.messages;

public class RequestAccept extends Message {
    public int proposalNumber; // n
    public int proposalValue;  // v

    public RequestAccept(int proposalNumber, int proposalValue) {
        super(MessageType.REQUEST_ACCEPT);
        this.proposalNumber = proposalNumber;
        this.proposalValue = proposalValue;
    }
    
}
