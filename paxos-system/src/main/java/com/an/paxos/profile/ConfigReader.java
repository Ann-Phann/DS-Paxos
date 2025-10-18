package com.an.paxos.profile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Read network.config file and parse the config into usable data structure
 */
public class ConfigReader {
    private static final ConcurrentHashMap<Integer, MemberInfo> MEMBER_MAP = new ConcurrentHashMap<>();
    private static final String CONFIG_FILE = "src/main/java/com/an/paxos/network.config";

    public static void initialise() {
        if (!MEMBER_MAP.isEmpty()) {
            return; // Already initialized
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(CONFIG_FILE))) {
            String line;

            while ((line = reader.readLine()) != null) {
                if ((line.startsWith("#") || line.isEmpty())) continue;

                String[] parts = line.split(",");

                if (parts.length != 3) {
                    System.err.println("Invalid config line: " + line);
                    continue;
                }

                String memId = parts[0].trim();
                int memIdInt = Integer.parseInt(memId.substring(1).trim());
                String host = parts[1].trim();
                int port = Integer.parseInt(parts[2].trim());

                MEMBER_MAP.put(memIdInt, new MemberInfo(memIdInt, host, port));
                System.out.println("Loaded config for M" + memIdInt + ": " + host + ":" + port);
            }
        } catch (IOException e) {
            System.err.println("FATAL: Could not read configuration file " + CONFIG_FILE);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static MemberInfo getMemberInfo(int memIdInt) {
        return MEMBER_MAP.get(memIdInt);
    }

    public static ConcurrentHashMap<Integer, MemberInfo> getAllMembersMap() {
        return MEMBER_MAP;
    }

    public static List<MemberInfo> getAllMembersInfo() {
        return new ArrayList<>(MEMBER_MAP.values());
    }

}
