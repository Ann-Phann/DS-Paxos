package com.an.paxos;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.an.paxos.profile.ConfigReader;
import com.an.paxos.profile.MemberInfo;
import com.an.paxos.profile.MemberProfile;
import com.an.paxos.communcation.ConnectionHandler;
import com.an.paxos.communcation.Messenger;
import com.an.paxos.logic.AcceptorLogic;
import com.an.paxos.logic.ProposalNumberGenerator;
import com.an.paxos.logic.ProposerLogic;
import com.an.paxos.messages.Accept;
import com.an.paxos.messages.Promise;

/*
 * Cotains Proposal, Acceptor, Learner logic
 */
public class CouncilMember implements Runnable {
    // core member state
    private final int memIdInt;
    int port;
    MemberProfile profile;
    ServerSocket serverSocket;

    boolean isRunning = true;
    // add lock for synchronisation instead of relying on sleep for busy-waiting
    public final Object lock = new Object();

    // proposal number generator
    private final ProposalNumberGenerator proposalNumberGenerator;
    
    // ========= Paxos state variables =========
    // Acceptor state
    private volatile int highestPromisedN = -1; // highest proposal number promised (n)
    private volatile int acceptedN = -1;        // proposal number of accepted proposal (n_a)
    private volatile int acceptedValue = -1; // value of accepted proposal (v_a)

    // Learner state 
    private volatile int decidedN = -1; // final, decided proposal number
    private volatile int decidedValue = -1; // final decided value
    private volatile boolean decided = false; // flag to indicate if a value has been decided

    // Maps a Proposal N to the list of PROMISE messages received for that N
    public final ConcurrentHashMap<Integer, CopyOnWriteArrayList<Promise>> promisedResponses = new ConcurrentHashMap<>();
    
    // Maps a Proposal N to the list of ACCEPTED messages received for that N
    public final ConcurrentHashMap<Integer, CopyOnWriteArrayList<Accept>> acceptedResponses = new ConcurrentHashMap<>();

    //========= Acceptor/Proposer logic handlers =========
    private final AcceptorLogic acceptorLogic;
    private final ProposerLogic proposerLogic;

    // messenger for outgoing messages
    private final Messenger messenger;

    // Maps a peer's port to its active output stream (used to send messages)
    public final ConcurrentHashMap<Integer, ObjectOutputStream> outgoingStreams = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<Integer, Socket> outgoingSockets = new ConcurrentHashMap<>();

    public CouncilMember(int memId, int port, MemberProfile profile) {
        this.memIdInt = memId;
        this.port = port;
        this.profile = profile;

        this.proposalNumberGenerator = new ProposalNumberGenerator(memId);
        this.acceptorLogic = new AcceptorLogic(this);
        this.proposerLogic = new ProposerLogic(this);
        this.messenger = new Messenger(this);
    }

    @Override
    public void run() {
        // Initialise server socket
        try {
            this.serverSocket = new ServerSocket(port);
            System.out.println("Member M" + memIdInt + " listening on port " + port + " with profile: " + profile);
        } catch (IOException e) {
            System.err.println("FATAL: Could not start server on port " + port);
            e.printStackTrace();
            this.isRunning = false;
            return; // Exit if the server socket cannot be opened
        }

        // Start listening for incoming connections
        while (isRunning) {
            try {
                // block until a peer (another Proposer/Acceptor) connects
                Socket peerSocket = serverSocket.accept();

                // Spawn a new thread to handle the incoming message
                new Thread(new ConnectionHandler(this, peerSocket)).start();

            } catch (IOException e) {
                // This exception usually means the serverSocket was closed (e.g., if isRunning was set to false)
                if (isRunning) {
                    System.err.println("Accept loop interrupted for M" + memIdInt + ": " + e.getMessage());
                }
            }
        }

        // Cleanup on shutdown
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (Exception e) {
            System.err.println("Error closing server socket for M" + memIdInt + ": " + e.getMessage());
        }
        System.out.println("Member M" + memIdInt + " has stopped running.");
    }

    public void stop() {
        this.isRunning = false;

        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (Exception e) {}

        // close all outgoing streams
        for (Socket socket : outgoingSockets.values()) {
            try {
                socket.close();
            } catch (IOException e) {}
        }

        outgoingSockets.clear();
        outgoingStreams.clear();
    }

    public void addOutgoingStream(int port, ObjectOutputStream stream) {
        outgoingStreams.put(port, stream);
    }

    public void addOutgoingSocket(int port, Socket socket) {
        outgoingSockets.put(port, socket);
    }
    public List<MemberInfo> getAllPeers() { 
        return ConfigReader.getAllMembersInfo(); 
    }

    public MemberProfile getProfile() { return profile; }

    public int getHighestPromisedN() { return highestPromisedN; }
    public void setHighestPromisedN(int highestPromisedN) {
        this.highestPromisedN = highestPromisedN;
    }

    public int getAcceptedN() { return acceptedN; }
    public void setAcceptedN(int acceptedN) { this.acceptedN = acceptedN; }
    public int getAcceptedValue() { return acceptedValue; }

    public void setAcceptedValue(int acceptedValue) { this.acceptedValue = acceptedValue; }

    public boolean isRunning() { return isRunning; }
    public int getPort() { return port; }
    public int getMemIdInt() { return memIdInt; }

    public void setDecided(boolean decided) { this.decided = decided; }
    public boolean getDecided() { return decided; }

    public void setDecidedN(int decidedN) { this.decidedN = decidedN; }
    public void setDecidedValue(int decidedValue) { this.decidedValue = decidedValue; }

    public ProposalNumberGenerator getProposalNumberGenerator() { return proposalNumberGenerator; }

    public AcceptorLogic getAcceptorLogic() { return acceptorLogic; }
    public ProposerLogic getProposerLogic() { return proposerLogic; }

    public Messenger getMessenger() { return messenger; }

    public static void main(String[] args) {
        if (args.length < 3 || !args[1].equals("--profile")) {
            System.err.println("Usage: java CouncilMember <MemberID> --profile <ProfileName>");
            System.err.println("Example: java CouncilMember M1 --profile reliable");
            return;
        }
        // Initialize configuration
        // 1. Parse command line arguments
        String memId = args[0].toUpperCase();
        int memIdInt = Integer.parseInt(memId.substring(1).trim());
        String profile = args[2].toUpperCase();

        // 2. Initialise the global config map
        ConfigReader.initialise();

        // 3. Get newtwork info for this member
        MemberInfo myInfo = ConfigReader.getMemberInfo(memIdInt);

        if (myInfo == null) {
            System.err.println("FATAL: Member ID " + memId + " not found in config file.");
            System.exit(1);
        }
        // Get profile enum from string
        MemberProfile profileEnum = MemberProfile.fromString(profile);

        // 4. Start the member instance with network info and profile
        CouncilMember member = new CouncilMember(memIdInt, myInfo.getPort(), profileEnum);
        Thread memberThread = new Thread(member);
        memberThread.start();

        try {
            memberThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Main thread interrupted.");
        }

    }
}
