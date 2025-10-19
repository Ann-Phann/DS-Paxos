package com.an.paxos;

import com.an.paxos.profile.ConfigReader;
import com.an.paxos.profile.MemberInfo;
import com.an.paxos.profile.MemberProfile;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Test1 {
    // Map to hold references to all running CouncilMember instances
    private static final Map<Integer, CouncilMember> members = new HashMap<>();

    public static void main(String[] args) {
        System.out.println("==========================================");
        System.out.println("    Testing Scenario 1: Ideal Network     ");
        System.out.println("==========================================");

        System.out.println("Starting Paxos Council...");
        
        try {
            // Load configuration
            ConfigReader.initialise();
            
            // 1. Start all Council Members
            for (MemberInfo info : ConfigReader.getAllMembersInfo()) {
                CouncilMember member = new CouncilMember(
                    info.getMemIdInt(),
                    info.getPort(),
                    MemberProfile.RELIABLE // Default profile; can be adjusted per member if needed
                );
                members.put(info.getMemIdInt(), member);
                
                // Start the CouncilMember (which runs the server and connection establishment)
                new Thread(member, "Member-M" + info.getMemIdInt() + "-Main").start();
                System.out.println("Started M" + info.getMemIdInt() + " on port " + info.getPort());
            }

            // Give the system a moment to establish all peer-to-peer connections
            Thread.sleep(5000); 
            System.out.println("\nAll members are running. Connections should be established.");
            System.out.println("---------------------------------------------------------");

            // 2. Start the Interactive Console
            startInteractiveConsole();

        } catch (Exception e) {
            System.err.println("Startup Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void startInteractiveConsole() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("CONSOLE: Use the format 'PROPOSE M[ID] V[VALUE]' (e.g., PROPOSE M4 V5) or 'QUIT'");

        while (true) {
            System.out.print("> ");
            if (!scanner.hasNextLine()) continue;
            
            String line = scanner.nextLine().trim();
            if (line.equalsIgnoreCase("QUIT")) {
                System.out.println("Shutting down...");
                shutdownAllMembers();
                break;
            }

            // Parse command: PROPOSE M<ID> V<VALUE>
            String[] parts = line.toUpperCase().split("\\s+");
            if (parts.length == 3 && parts[0].equals("PROPOSE")) {
                try {
                    int memberId = Integer.parseInt(parts[1].substring(1)); // Extract ID from M1
                    int value = Integer.parseInt(parts[2].substring(1)); // Extract Value from V10
                    
                    CouncilMember proposer = members.get(memberId);
                    if (proposer == null) {
                        System.err.println("Error: Member M" + memberId + " not found.");
                        continue;
                    }

                    // --- Action: Launch the proposal in a new thread ---
                    // This is crucial: the propose() method in ProposerLogic is blocking,
                    // so it must run in a background thread to keep the console responsive.
                    new Thread(() -> {
                        System.out.println("M" + memberId + " initiating proposal for value " + value + "...");
                        proposer.getProposerLogic().propose(value);
                        System.out.println("M" + memberId + " proposal attempt finished.");
                    }, "Proposer-M" + memberId).start();

                } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
                    System.err.println("Invalid format. Use: PROPOSE M[ID] V[VALUE].");
                }
            } else {
                System.err.println("Unknown command. Use: PROPOSE M[ID] V[VALUE] or QUIT.");
            }
        }
        scanner.close();
    }
    
    private static void shutdownAllMembers() {
        for (CouncilMember member : members.values()) {
            member.stop(); 
        }
    }
}