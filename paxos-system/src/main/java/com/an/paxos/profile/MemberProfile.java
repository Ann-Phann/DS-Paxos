package com.an.paxos.profile;

import java.util.concurrent.ThreadLocalRandom;

/*
 * Define the characteristics (delay range, drop rate, etc.) of a member
 */
public enum MemberProfile {
    // MinDelay, MaxDelay (ms), MessageDropRate (0.0 to 1.0)
    RELIABLE(0, 50, 0.0),      // Near-zero latency, no drops
    STANDARD(100, 300, 0.01),  // Normal jitter, 1% drop rate
    LATENT(400, 800, 0.0),     // High, consistent delay, no drops
    FAILURE(100, 300, 0.1);   // Normal jitter, 5% drop rate (simulating slow/faulty members)

    private final int minDelay;
    private final int maxDelay;
    private final double messageDropRate;


    MemberProfile(int minDelay, int maxDelay, double messageDropRate) {
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
        this.messageDropRate = messageDropRate;
    }

    /*
     * @return The delay in milliseconds for a message based on the profile's delay range
     */
    public int getRandomDelay() {
        if (minDelay >= maxDelay) {
            return minDelay;
        }
        return ThreadLocalRandom.current().nextInt(minDelay, maxDelay + 1);
    }

    /*
     * @return True if a message should be dropped
     */
    public boolean shouldDropMessage() {
        return ThreadLocalRandom.current().nextDouble() < messageDropRate;
    }

    /*
     * Find the matching MemberProfile enum from string read from config file
     */

    public static MemberProfile fromString(String profile) {
        try {
            return MemberProfile.valueOf(profile.toUpperCase());
        } catch (Exception e) {
            System.err.println("Unknown profile: " + profile + ". Defaulting to STANDARD.");
            return STANDARD;
        }
    }
    
}
