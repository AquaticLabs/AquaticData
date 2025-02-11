package testing.newtest;

import java.util.Base64;

/**
 * @Author: extremesnow
 * On: 11/3/2024
 * At: 14:51
 */
public class Utils {

    public Utils() {
    }
    public static String encodeByteArrayToBase64(byte[] bytes) {
        // encode, byte[] to Base64 encoded string
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static byte[] getBytesFromBase64(String string) {
        // encode, byte[] to Base64 encoded string
        return Base64.getDecoder().decode(string);
    }

}
