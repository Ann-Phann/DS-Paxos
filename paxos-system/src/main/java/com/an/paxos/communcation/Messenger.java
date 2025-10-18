package com.an.paxos.communcation;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.an.paxos.CouncilMember;
import com.an.paxos.messages.Message;
import com.an.paxos.profile.MemberInfo;
/*
 * This class is intended to handle message sending functionalities.
 */
public class Messenger {
    private final CouncilMember member;
    private static final int RETRY_INTERVAL_MS = 5000;

    // List of peers we have not successfully connected to yet 
    private final List<MemberInfo> failedPeers = Collections.synchronizedList(new ArrayList<>());

    public Messenger(CouncilMember member) {
        this.member = member;
        
        // intialise attempt to connect to all other members
        initialConnectionAttempt();

        // start retry mechanism in a separate thread
        startRetryThread();
    }

    private void initialConnectionAttempt() {
        // Attempt to connect to all peers once at startup
        List<MemberInfo> allMembers = member.getAllPeers();

        for (MemberInfo peer : allMembers) {
            if (peer.getMemIdInt() == member.getMemIdInt()) {
                continue; // Skip self
            }

            if (!attemptConnection(peer)) {
                failedPeers.add(peer);
            }
            
        }
    }

    /**
     * Attempts a single connection to a peer and registers the output stream if successful.
     * @return true if successful, false otherwise.
     */
    private boolean attemptConnection(MemberInfo peer) {
        try {
            // establish connection
            Socket socket = new Socket(peer.getHost(), peer.getPort());
            member.addOutgoingSocket(peer.getPort(), socket);

            // open the output stream
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());

            // cache the stream for future broadcasts
            member.addOutgoingStream(peer.getPort(), outputStream);

            System.out.println("M" + member.getMemIdInt() + " connected to peer M" + peer.getMemIdInt() + 
                                " on port " + peer.getPort());

            return true;

        } catch (IOException e) {
            System.err.println("M" + member.getMemIdInt() + " failed to connect to M" + peer.getMemIdInt() + 
                                " at " + peer.getHost() + ":" + peer.getPort() + ". Retrying later...");
        
            return false;
        }
    }

    private void startRetryThread() {
        Thread retryThread = new Thread(() -> {
            // Run as long as the CouncilMember is running
            while (member.isRunning()) {
                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                // Iterate over the failed list and retry the connection
                synchronized (failedPeers) {
                    Iterator<MemberInfo> iterator = failedPeers.iterator();
                    while (iterator.hasNext()) {
                        MemberInfo peer = iterator.next();
                        if (attemptConnection(peer)) {
                            // Connection succeeded! Remove it from the failed list
                            iterator.remove();
                            if (failedPeers.isEmpty()) {
                                System.out.println("M" + member.getMemIdInt() + " established connections to all peers.");
                            }
                        }
                    }
                }
            }
        }, "Messenger-Retry-Thread-M" + member.getMemIdInt());

        // Daemon thread ensures it doesn't prevent JVM shutdown
        retryThread.setDaemon(true); 
        retryThread.start();
    }

    /**
     * Broadcasts a message to all connected peers using cached output streams.
     * This method also handles immediate stream failure by triggering a retry for that peer.
     * @param message The Paxos message to send (PREPARE, REQUEST_ACCEPT, or DECIDE).
     */
    public void broadcast(Message message) {
        List<Integer> failedPorts = new ArrayList<>();

        for (var entry : member.outgoingStreams.entrySet()) {
            ObjectOutputStream outputStream = entry.getValue();
            int port = entry.getKey();
            try {
                outputStream.writeObject(message);
                outputStream.flush();
            } catch (IOException e) {
                System.err.println("M" + member.getMemIdInt() + " failed to send message " + message.type + 
                                   " to port " + port + ": " + e.getMessage());
                failedPorts.add(port);
            }
        }

        for (int port : failedPorts) {
            // remove the failed stream
            member.outgoingStreams.remove(port);
            // 2. Find the corresponding MemberInfo object to restart the connection
            MemberInfo peerInfo = member.getAllPeers().stream()
                    .filter(p -> p.getPort() == port)
                    .findFirst()
                    .orElse(null);
            
            if (peerInfo != null) {
                System.err.println("M" + member.getMemIdInt() + " lost connection to M" + peerInfo.getMemIdInt() + ". Scheduling retry.");
                
                // 3. Add back to the failedPeers list if not already present
                synchronized (failedPeers) {
                    if (!failedPeers.contains(peerInfo)) {
                        failedPeers.add(peerInfo);
                    }
                }
            }
        }
    }
}
