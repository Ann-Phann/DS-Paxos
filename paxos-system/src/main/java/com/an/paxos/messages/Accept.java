package com.an.paxos.messages;

public class Accept extends Message {
    public int acceptedN; // The proposal number that was accepted (n_a)
    public int acceptedValue; // The value that was accepted (v_a)

    public Accept(int acceptedN, int acceptedValue) {
        super(MessageType.ACCEPTED);
        this.acceptedN = acceptedN;
        this.acceptedValue = acceptedValue;
    }
    
}
