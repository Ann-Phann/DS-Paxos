package com.an.paxos.communcation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.an.paxos.CouncilMember;
import com.an.paxos.messages.Message;

/*
 * ConnectionHandler manages incoming messages from a peer Council Member.
 * It reads messages from the input stream, simulates network conditions
 */
public class ConnectionHandler implements Runnable {
    private final CouncilMember member;
    private final Socket peerSocket;
    private final ObjectOutputStream outputStream;
    private final ObjectInputStream inputStream;
    private final MessageProcess messageProcess;

    public ConnectionHandler(CouncilMember member, Socket peerSocket) throws IOException{
        this.member = member;
        this.peerSocket = peerSocket;

        this.outputStream = new ObjectOutputStream(peerSocket.getOutputStream());
        this.inputStream = new ObjectInputStream(peerSocket.getInputStream());
        this.outputStream.flush(); // Ensure header is sent immediately
        this.messageProcess = new MessageProcess(member, outputStream);
    }

    public ConnectionHandler(CouncilMember member, Socket peerSocket, 
                         ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        this.member = member;
        this.peerSocket = peerSocket;
        this.inputStream = inputStream; // Use the existing stream
        this.outputStream = outputStream; // Use the existing stream
        this.messageProcess = new MessageProcess(member, outputStream);
    }
    @Override
    public void run() {
        try {
            while (member.isRunning()) {
                Message message = (Message) inputStream.readObject();
                
                // Get member profile rule 
                if (member.getProfile().shouldDropMessage()) {
                    System.out.println(member.getMemIdInt() + " DROPPED message: " + message.type);
                    continue; // Skip processing and wait for next message
                }

                // Simulate network delay based on member profile
                Thread.sleep(member.getProfile().getRandomDelay());

                // Process the message
                messageProcess.process(message);
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Connection Handler Error for " + member.getMemIdInt() + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            try {
                inputStream.close();
                outputStream.close();
                peerSocket.close();
            } catch (IOException ignored) {}
        }
    }


}
