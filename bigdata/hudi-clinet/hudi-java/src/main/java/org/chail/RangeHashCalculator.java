package org.chail;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class RangeHashCalculator {

    public static void main(String[] args) {
        String input = "Hello, Worldxxxxxxxx!";
        int range = 1000; // 范围值

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = md.digest(input.getBytes());

            // 将哈希值转换为整数
            int hashValue = 0;
            for (int i = 0; i < hashBytes.length; i++) {
                hashValue += (hashBytes[i] & 0xFF) << (8 * i);
            }

            // 计算范围内的哈希值
            int rangeHash = Math.abs(hashValue % range);

            System.out.println("Input: " + input);
            System.out.println("Range: " + range);
            System.out.println("Range Hash: " + rangeHash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}