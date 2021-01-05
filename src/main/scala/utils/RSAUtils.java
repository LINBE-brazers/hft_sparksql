package utils;

import Extract_Transform_Load.ETLUtils;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RSAUtils {

    private static Cipher cipher;
    final private static SecureRandom paramSecureRandom = new SecureRandom();
    final private static String publicKey = ETLUtils.readFileFromHDFS("/user/hive/udf/rsa_public_key");

    static {
        try {
            cipher = Cipher.getInstance("RSA");
            paramSecureRandom.setSeed(123L);
            System.out.println(publicKey);
            cipher.init(Cipher.ENCRYPT_MODE, getPublicKey(publicKey), paramSecureRandom);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 得到公钥
     *
     * @param key 密钥字符串（经过base64编码）
     * @throws Exception
     */
    public static PublicKey getPublicKey(String key) throws Exception {
        byte[] keyBytes;
        keyBytes = (new BASE64Decoder()).decodeBuffer(key);
        X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PublicKey publicKey = keyFactory.generatePublic(keySpec);
        return publicKey;
    }

    /**
     * 得到私钥
     *
     * @param key 密钥字符串（经过base64编码）
     * @throws Exception
     */
    public static PrivateKey getPrivateKey(String key) throws Exception {
        byte[] keyBytes;
        keyBytes = (new BASE64Decoder()).decodeBuffer(key);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
        return privateKey;
    }

    /**
     * 使用公钥对明文进行加密
     *
     * @param publicKey 公钥
     * @param plainText 明文
     * @return
     */
    public static String encrypt(String publicKey, String plainText) {
        try {
//            SecureRandom paramSecureRandom = new SecureRandom();
//            paramSecureRandom.setSeed(123L);
            cipher.init(Cipher.ENCRYPT_MODE, getPublicKey(publicKey), paramSecureRandom);
            byte[] enBytes = cipher.doFinal(plainText.getBytes("UTF-8"));
//            byte[] enBytes = cipher.doFinal(plainText.getBytes());
            return replaceAllBlank((new BASE64Encoder()).encode(enBytes));
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String encrypt(String plainText) {
        try {
//            SecureRandom paramSecureRandom = new SecureRandom();
//            paramSecureRandom.setSeed(123L);
            byte[] enBytes = cipher.doFinal(plainText.getBytes("UTF-8"));
            return replaceAllBlank((new BASE64Encoder()).encode(enBytes));
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 使用私钥对密文进行解密
     *
     * @param privateKey 私钥
     * @param enStr      密文
     * @return
     */
    public static String decrypt(String privateKey, String enStr) {
        try {
            cipher.init(Cipher.DECRYPT_MODE, getPrivateKey(privateKey));
            byte[] deBytes = cipher.doFinal((new BASE64Decoder()).decodeBuffer(enStr));
            return new String(deBytes, Charset.forName("UTF-8"));
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     *
     * @param str
     * @return
     */
    public static String replaceAllBlank(String str) {
        String s = "";
        if (str != null) {
            Pattern p = Pattern.compile("\r|\n");
            Matcher m = p.matcher(str);
            s = m.replaceAll("");
        }
        return s;
    }

    public static void main(String[] args) throws Exception {




        String publicKey = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCbsAhtq82pNlz7G16LuHiyLmbMVn1+AqzHBqVzSZ9cFZSrNqZdT+ZFMOSK/LoXIPbyawRtHkAkQW1t8bDPueoy95W1xF7x2VijUp4YpL5bdNrKScWj+GcvhiV1JhELQy7tAb5W25sNaj6DdnuQ5I3JFe7MlTeqp2JKlnVoBCOlQQIDAQAB";

        String privateKey = "MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAJuwCG2rzak2XPsbXou4eLIuZsxW\n" +
                "fX4CrMcGpXNJn1wVlKs2pl1P5kUw5Ir8uhcg9vJrBG0eQCRBbW3xsM+56jL3lbXEXvHZWKNSnhik\n" +
                "vlt02spJxaP4Zy+GJXUmEQtDLu0Bvlbbmw1qPoN2e5DkjckV7syVN6qnYkqWdWgEI6VBAgMBAAEC\n" +
                "gYAPhGgSpkEFUInL7VprCqPc/or4atZvLM0TuTHcX8YmY3BB8Fx8iG4nD0x4HeBeVcbHOqtiRNWX\n" +
                "x32kq6Y3zgvth4tntLVpMcGYX+6lbNRLDgSXs9Gdp89yYJBCgZl45emjLNejpfaRPkiR7+aDOPlI\n" +
                "TF/aXLeBTGJRvHun+KABEQJBAP7HU9QEKBOOdIDiCncWgWIqdvmrqoyObsLFr15gv/ytQp+JNAOH\n" +
                "RayJtmP3kHg/XuLcbVp0Re5t436HfJcAAIcCQQCcbxkJtQLeSvYqqOBsS4VUEwViPBd27xW69k4Y\n" +
                "HF18BXa1z4g1lIRMSvmTXDegBP+IgPEoMQ0YIIce7ebee4X3AkBhmtVHjQwZaeLCGVavBsUsaV5J\n" +
                "CfX9gPd30Kn9ew0x7OJwIez2SRVtIxjntUj4eDaOrKmMFK1RyXF04MzfQFXzAkBRJ3WWypgNWFgy\n" +
                "s1+R7u/hOOjvGHuX0Nq2HndPHNAGuhLmqR5hpYWoyrCFGS8mTdF/MF1rW18OqDlQ+1xtCSnrAkEA\n" +
                "vxwkCUrThXVIXGrNSe7774K7CCd7R6bKbHDF6YpnGjLnYG/WEGOz9nSkp5xnwUAVCpF/tv9yYWjJ\n" +
                "TZ74rdH1QQ==";


        System.out.println(encrypt(publicKey, "王洪卫"));

        System.out.println(decrypt(privateKey, "Tpyaz1Up7zbr60yXB0HISNKaf6g8PS9ebiANJqmPIw+uyuPFX01ZVX5Kd7KGOTpvdPAfkbrJniABETSuhOAJYXpdtxVX0hqcGxKlskBL0XeVODXHmUwvRlgHxKhmsuAJcGECF90fcu1DGRe9qGkZXUKnU5RAnUVZZuaddhKIYT0="));
    }
}
