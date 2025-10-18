package com.an.paxos.messages;

public class Decide extends Message {
    public int decidedN;
    public int decidedValue;

    public Decide(int decidedN, int decidedValue) {
        super(MessageType.DECIDE);
        this.decidedN = decidedN;
        this.decidedValue = decidedValue;
    }
}
