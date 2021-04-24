package org.aueb.ds.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class Hashing {

    public String md5Hash(String topic) {
        MessageDigest md5;
        try {
             md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }

        byte[] hashed = md5.digest(topic.getBytes());
        return Base64.getEncoder().encodeToString(hashed);
    }
}
