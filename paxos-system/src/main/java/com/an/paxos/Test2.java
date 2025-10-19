package com.an.paxos;
import com.an.paxos.profile.ConfigReader;
import com.an.paxos.profile.MemberInfo;
import com.an.paxos.profile.MemberProfile;

import java.util.HashMap;
import java.util.Map;

/**
 * Executes Scenario 2: Concurrent Proposals (M1 proposes 1, M8 proposes 8)
 * in a controlled, non-interactive, single-JVM environment.
 */
public class Test2 {

    private static final Map<Integer, CouncilMember> members = new HashMap<>();

    public static void main(String[] args) {
        System.out.println("Starting Paxos Council for Scenario 2 (Concurrent Proposals)...");

        try {
            // 0. Initial Setup
            ConfigReader.initialise();
            
            // 1. Start all Council Members with RELIABLE profile
            for (MemberInfo info : ConfigReader.getAllMembersInfo()) {
                CouncilMember member = new CouncilMember(
                    info.getMemIdInt(),
                    info.getPort(),
                    MemberProfile.RELIABLE 
                );
                members.put(info.getMemIdInt(), member);
                
                new Thread(member, "Member-M" + info.getMemIdInt() + "-Main").start();
                System.out.println("Started M" + info.getMemIdInt() + " (RELIABLE)");
            }

            // Give time for all internal connections to establish
            System.out.println("\nWaiting 5 seconds for connections to establish...");
            Thread.sleep(5000); 
            System.out.println("---------------------------------------------------------");

            // 2. Launch Concurrent Proposals
            CouncilMember proposerM1 = members.get(1);
            CouncilMember proposerM8 = members.get(8);

            if (proposerM1 == null || proposerM8 == null) {
                System.err.println("Error: Proposer members M1 or M8 not found in configuration.");
                shutdownAllMembers();
                return;
            }
            
            // Proposal A: M1 proposes value 1 (representing "M1")
            Thread proposalA = new Thread(() -> {
                System.out.println("M1 initiating proposal for value 1...");
                proposerM1.getProposerLogic().propose(1);
                System.out.println("M1 proposal attempt finished.");
            }, "Proposer-M1-Thread");

            // Proposal B: M8 proposes value 8 (representing "M8")
            Thread proposalB = new Thread(() -> {
                System.out.println("M8 initiating proposal for value 8...");
                proposerM8.getProposerLogic().propose(8);
                System.out.println("M8 proposal attempt finished.");
            }, "Proposer-M8-Thread");

            // Start them almost simultaneously
            proposalA.start();
            proposalB.start();
            
            System.out.println("\n--- Proposals Launched Concurrently ---");

            // 3. Wait for both proposals to complete (Paxos is blocking/retrying until decision)
            proposalA.join();
            proposalB.join();

            System.out.println("\n--- Both Proposals Finished. Checking for Consensus. ---");
            
            // Give a moment for final DECIDE messages to propagate and output
            Thread.sleep(1000); 

            // 4. Clean Shutdown
            shutdownAllMembers();

        } catch (Exception e) {
            System.err.println("Test Execution Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Ensure the main process exits
            System.out.println("\nScenario 2 Test Complete.");
            System.exit(0);
        }
    }

    private static void shutdownAllMembers() {
        System.out.println("\nShutting down all members...");
        // Stop all CouncilMember run loops (which will lead to Socket Closed errors in handlers)
        for (CouncilMember member : members.values()) {
            member.stop(); 
        }
        
        // Wait briefly for connection handler threads to catch their exceptions and terminate
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
