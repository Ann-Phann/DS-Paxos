package com.an.paxos;

import com.an.paxos.profile.ConfigReader;
import com.an.paxos.profile.MemberInfo;
import com.an.paxos.profile.MemberProfile;

import java.util.HashMap;
import java.util.Map;

/**
 * Executes Scenario 3: Fault Tolerance (3a, 3b, 3c).
 * Uses a mixed profile setup and tests proposal initiation from different member types.
 */
public class Test3 {

    private static final Map<Integer, CouncilMember> members = new HashMap<>();

    public static void main(String[] args) {
        System.out.println("Starting Paxos Council for Scenario 3 (Fault Tolerance Tests)...");

        try {
            // 0. Initial Setup
            ConfigReader.initialise();
            
            // 1. Start all Council Members with MIXED profiles
            setupMixedProfiles();

            // Give time for all internal connections to establish
            System.out.println("\nWaiting 5 seconds for connections to establish...");
            Thread.sleep(5000); 
            System.out.println("---------------------------------------------------------");
            
            // --- Execute Sub-Scenarios Sequentially ---
            
            // Test 3a: Standard member (M4) proposes
            runTest3a();
            System.out.println("\n--- Resetting all member decision states for Test 3C ---");
            for (CouncilMember member : members.values()) {
                member.resetLearnerState();
            }

            // Test 3b: Latent member (M2) proposes
            runTest3b();
            System.out.println("\n--- Resetting all member decision states for Test 3C ---");
            for (CouncilMember member : members.values()) {
                member.resetLearnerState();
            }
            
            // Test 3c: Failing member (M3) proposes, crashes, and is overridden by M4
            runTest3c();

        } catch (Exception e) {
            System.err.println("Test Execution Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 4. Clean Shutdown
            System.out.println("\n--- All Scenarios Complete. Initiating Shutdown. ---");
            shutdownAllMembers();
            System.out.println("\nScenario 3 Test Complete.");
            // Ensure the main process exits
            System.exit(0);
        }
    }

    private static void setupMixedProfiles() throws Exception {
        System.out.println("Setting up members with mixed profiles:");
        for (MemberInfo info : ConfigReader.getAllMembersInfo()) {
            int id = info.getMemIdInt();
            MemberProfile profile;

            if (id == 1) {
                profile = MemberProfile.RELIABLE; // M1: Reliable
            } else if (id == 2) {
                profile = MemberProfile.LATENT;   // M2: Latent
            } else if (id == 3) {
                profile = MemberProfile.FAILURE;  // M3: Failure (Crashes)
            } else {
                profile = MemberProfile.RELIABLE; // M4-M9: Standard (using RELIABLE as proxy for fully functional)
            }

            CouncilMember member = new CouncilMember(
                id,
                info.getPort(),
                profile 
            );
            members.put(id, member);
            
            new Thread(member, "Member-M" + id + "-Main").start();
            System.out.printf("Started M%d with profile: %s%n", id, profile.name());
        }
    }

    private static void runTest3a() throws InterruptedException {
        System.out.println("\n=========================================================");
        System.out.println("TEST 3A: Standard Member (M4) Proposes '4'");
        System.out.println("Expected: Consensus reached quickly despite M2 (latent) and M3 (failure).");
        System.out.println("=========================================================");
        
        CouncilMember proposerM4 = members.get(4);
        
        Thread proposalThread = new Thread(() -> {
            System.out.println("M4 initiating proposal for value 4...");
            proposerM4.getProposerLogic().propose(4);
            System.out.println("M4 proposal attempt finished.");
        }, "Proposer-M4-Thread-3a");

        proposalThread.start();
        proposalThread.join(); // Wait for proposal to complete
        System.out.println("\n--- Test 3A Finished ---\n");
        Thread.sleep(2000); // Wait for consensus messages to print
    }

    // private static void runTest3b() throws InterruptedException {
    //     System.out.println("\n=========================================================");
    //     System.out.println("TEST 3B: Latent Member (M2) Proposes '2'");
    //     System.out.println("Expected: Consensus reached, possibly after a delay/retries due to M2's latency.");
    //     System.out.println("=========================================================");
        
    //     CouncilMember proposerM2 = members.get(2);
        
    //     Thread proposalThread = new Thread(() -> {
    //         System.out.println("M2 initiating proposal for value 2...");
    //         proposerM2.getProposerLogic().propose(2);
    //         System.out.println("M2 proposal attempt finished.");
    //     }, "Proposer-M2-Thread-3b");

    //     proposalThread.start();
    //     proposalThread.join(); // Wait for proposal to complete (could be long)
    //     System.out.println("\n--- Test 3B Finished ---\n");
    //     Thread.sleep(10000); // Wait for consensus messages to print
    // }

    private static void runTest3b() throws InterruptedException {
        System.out.println("\n=========================================================");
        System.out.println("TEST 3B: Latent Member (M2) Proposes '2'");
        System.out.println("Expected: Consensus reached, possibly after a delay/retries due to M2's latency.");
        System.out.println("=========================================================");
        
        CouncilMember proposerM2 = members.get(2);
        
        Thread proposalThread = new Thread(() -> {
            System.out.println("M2 initiating proposal for value 2...");
            // Use a simple retry mechanism here to force the latent proposer to succeed, 
            // as its slow messages often cause initial internal timeouts.
            boolean success = false;
            int attempt = 0;
            while (!success && attempt < 5) { // Try up to 5 times
                if (attempt > 0) {
                    System.out.printf("M2 retrying proposal for value 2 (Attempt %d).%n", attempt + 1);
                    try {
                        Thread.sleep(5000); // Wait a bit before retrying
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                success = proposerM2.getProposerLogic().propose(2);
                attempt++;
            }
            if (success) {
                System.out.println("M2 proposal attempt finished successfully.");
            } else {
                System.out.println("M2 proposal attempt failed after all retries.");
            }
        }, "Proposer-M2-Thread-3b");

        proposalThread.start();
        proposalThread.join(); // Wait for proposal to complete (could be long due to retries)
        System.out.println("\n--- Test 3B Finished ---\n");
        // Maintain the 10s wait to ensure the final slow DECIDE message propagates and is logged.
        Thread.sleep(10000); 
    }

    private static void runTest3c() throws InterruptedException {
        System.out.println("\n=========================================================");
        System.out.println("TEST 3C: Failing Member (M3) Proposes, Crashes, M4 Retries with '4'");
        System.out.println("Expected: M3 fails and M4 successfully drives consensus on '4'.");
        System.out.println("=========================================================");
        
        CouncilMember proposerM3 = members.get(3);
        CouncilMember proposerM4 = members.get(4);

        // M3 starts the proposal (it should crash/fail internally during the run)
        Thread proposalM3 = new Thread(() -> {
            System.out.println("M3 initiating proposal for value 3 (and crashing after PREPARE)...");
            proposerM3.getProposerLogic().propose(3);
            System.out.println("M3 proposal attempt finished (due to crash or failure).");
        }, "Proposer-M3-Crash-Thread");

        // M4 starts a conflicting proposal immediately after M3, designed to win/retry
        Thread proposalM4 = new Thread(() -> {
            System.out.println("M4 initiating proposal for value 4 (to ensure consensus is reached)...");
            proposerM4.getProposerLogic().propose(4);
            System.out.println("M4 proposal attempt finished.");
        }, "Proposer-M4-Winner-Thread");

        proposalM3.start();
        // Introduce a slight delay (or none) to let M3 send its initial Prepare before M4 starts
        Thread.sleep(200); 
        
        
        System.out.println("CRASH SIMULATED: Member M3 is forcibly stopped.");
        proposerM3.stop(); // Assuming CouncilMember.stop() handles proper shutdown
        proposalM3.interrupt(); // Ensure its proposer thread wakes up and terminates

        proposalM4.start();

        // Wait for both threads to finish. M3's thread will likely finish quickly due to the crash.
        // M4's thread will block until it successfully forces consensus.
        proposalM3.join();
        proposalM4.join(); 

        System.out.println("\n--- Test 3C Finished ---\n");
        Thread.sleep(25000); // Wait for final consensus messages to print
    }
    


    private static void shutdownAllMembers() {
        System.out.println("\nShutting down all members...");
        // Stop all CouncilMember run loops (which will lead to Socket Closed errors in handlers)
        for (CouncilMember member : members.values()) {
            member.stop(); 
        }
        
        // Wait briefly for connection handler threads to catch their exceptions and terminate
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }   
}
