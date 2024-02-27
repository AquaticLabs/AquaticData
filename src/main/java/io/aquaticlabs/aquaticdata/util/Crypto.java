package io.aquaticlabs.aquaticdata.util;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;


public class Crypto {

    private final String ALGORITHM = "AES";
    private final String KEY; // 16 character key


    public Crypto (String key) {
        StringBuilder keyBuilder = new StringBuilder(key);
        while (keyBuilder.length() < 16) {
            keyBuilder.append('f');
        }
        key = keyBuilder.toString();
        if (key.length() > 16) {
            key = key.substring(0, 16);
        }
        this.KEY = key;
    }

    public String getKEY() {
        return KEY;
    }

    public String encrypt(String valueToEncrypt) throws Exception {
        SecretKeySpec secretKeySpec = new SecretKeySpec(KEY.getBytes(), ALGORITHM);
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
        byte[] encryptedValueBytes = cipher.doFinal(valueToEncrypt.getBytes());
        return Base64.getEncoder().encodeToString(encryptedValueBytes);
    }

    public String decrypt(String encryptedValue) throws Exception {
        SecretKeySpec secretKeySpec = new SecretKeySpec(KEY.getBytes(), ALGORITHM);
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
        byte[] decodedValueBytes = Base64.getDecoder().decode(encryptedValue);
        byte[] decryptedValueBytes = cipher.doFinal(decodedValueBytes);
        return new String(decryptedValueBytes);
    }

}