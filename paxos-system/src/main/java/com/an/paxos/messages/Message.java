package com.an.paxos.messages;

public abstract class Message implements java.io.Serializable {
    public MessageType type;

    public Message(MessageType type) {
        this.type = type;
    }   
}
