package com.an.paxos.messages;

public class Promise extends Message {
    public int proposalNumber; // n
    public int acceptedN = -1;      // n_a
    public int acceptedValue = -1; // v_a

    public Promise(int proposalNumber) {
        super(MessageType.PROMISE);
        this.proposalNumber = proposalNumber;
    }

    public Promise(int proposalNumber, int acceptedN, int acceptedValue) {
        super(MessageType.PROMISE);
        this.proposalNumber = proposalNumber;
        this.acceptedN = acceptedN;
        this.acceptedValue = acceptedValue;
    }
}
