package com.an.paxos.messages;

public enum MessageType {
    PREPARE,
    PROMISE,
    REQUEST_ACCEPT,
    ACCEPTED,
    DECIDE,
}
