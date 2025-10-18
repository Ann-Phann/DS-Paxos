package com.an.paxos.profile;

public final class MemberInfo {
    private final int memIdInt;
    private final String host;
    private final int port;

    public MemberInfo(int memberIdInt, String host, int port) {
        this.memIdInt = memberIdInt;
        this.host = host;
        this.port = port;
    }

    // Public getter methods
    public String getHost() { return host; }

    public int getPort() { return port; }

    public int getMemIdInt() { return memIdInt; }

    @Override
    public String toString() {
        return "MemberInfo{" +
                "id='" + memIdInt + '\'' +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}