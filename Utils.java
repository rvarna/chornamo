import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

public class Utils {
    private final static String DELIMITER = ":";

    public static BigInteger getHash(String nodeKey) {
        try {
            MessageDigest hasher = MessageDigest.getInstance("SHA-256");
            return new BigInteger(hasher.digest(nodeKey.getBytes()));
        } catch(NoSuchAlgorithmException e)
        {
            return null;
        }
    }

    public static boolean isHashcodeBetween(BigInteger hashcode, BigInteger begin, BigInteger end) {
        if (end.compareTo(begin) < 0) {
            if (hashcode.compareTo(end) > 0 && hashcode.compareTo(begin) > 0) {
                return true;
            }

            if (hashcode.compareTo(end) < 0 && hashcode.compareTo(begin) < 0) {
                return true;
            }
        }
        else if (hashcode.compareTo(begin) > 0 && hashcode.compareTo(end) < 0) {
            return true;
        }

        return false;
    }

    public static String[] decodeNode(String nodeKey) {
        return nodeKey.split(DELIMITER);
    }
    

    public static String encode_node(String hostname, Integer port) {
        return hostname + DELIMITER + port;
    }

    public static BigInteger getPower(Integer a, int b) {
        return (new BigInteger(a.toString()).pow(b));
    }

    public static BigInteger getModulus(BigInteger a, BigInteger b) {
        return a.mod(b);
    }
}