package org.chail;

import java.util.UUID;

public class ShortUUIDGenerator {

    public static String generateShortUUID() {
        String uuid = UUID.randomUUID().toString();
        long mostSigBits = UUID.fromString(uuid).getMostSignificantBits();
        long leastSigBits = UUID.fromString(uuid).getLeastSignificantBits();
        String hexString = Long.toHexString(mostSigBits);
        String hexString1 = Long.toHexString(leastSigBits);
        String shortUUID = Long.toHexString(mostSigBits) + Long.toHexString(leastSigBits);
        return shortUUID;
    }

    public static void main(String[] args) {
        String shortUUID = generateShortUUID();
        System.out.println("Short UUID: " + shortUUID);
    }
}
