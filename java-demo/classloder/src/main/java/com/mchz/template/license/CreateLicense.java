

package com.mchz.template.license;



import javax.crypto.*;
import java.io.*;
import java.security.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class CreateLicense {
    private static Integer ENCODE;
    private boolean useIBMJCE = false;

    static {
        //Security.addProvider(new SunJCE());
        ENCODE = 1;
    }

    public CreateLicense() {
    }

    public boolean isUseIBMJCE() {
        return this.useIBMJCE;
    }

    public void setUseIBMJCE(boolean useIBMJCE) {
        this.useIBMJCE = useIBMJCE;
    }

    private byte[] fileDes(String order, Integer type, String src) throws Exception {
        KeyGenerator generator = KeyGenerator.getInstance("DES");
        SecureRandom secureRandom = null;
        if (!this.isUseIBMJCE()) {
            secureRandom = SecureRandom.getInstance("SHA1PRNG");
        }

        secureRandom.setSeed(order.getBytes("utf-8"));
        generator.init(56, secureRandom);
        Key key = generator.generateKey();
        Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
        if (type == ENCODE) {
            cipher.init(1, key);
            return cipher.doFinal(src.getBytes("utf-8"));
        } else {
            cipher.init(2, key);
            return cipher.doFinal(parseHexStr2Byte(src));
        }
    }

    public void createPairKey(String keyUrl) {
        try {
            KeyPairGenerator keygen = KeyPairGenerator.getInstance("RSA");
            SecureRandom random = new SecureRandom();
            random.setSeed(1000L);
            keygen.initialize(512, random);
            KeyPair keys = keygen.generateKeyPair();
            PublicKey pubkey = keys.getPublic();
            PrivateKey prikey = keys.getPrivate();
            this.doObjToFile(keyUrl + "//public.key", new Object[]{pubkey});
            this.doObjToFile(keyUrl + "//private.key", new Object[]{prikey});
            System.out.println("create key done!");
        } catch (NoSuchAlgorithmException var7) {
            var7.printStackTrace();
            System.out.println("create key fail!");
        }

    }

    private Object[] signToInfo(List<String> infos, String signfile, String keyUrl) {
        PrivateKey myprikey = (PrivateKey)this.getObjFromFile(keyUrl, 1);

        try {
            Signature signet = Signature.getInstance("MD5WithRSA");
            signet.initSign(myprikey);
            Object[] licenses = new Object[infos.size() * 2];

            for(int i = 0; i < infos.size(); ++i) {
                String info = (String)infos.get(i);
                signet.update(info.getBytes());
                byte[] signed = signet.sign();
                String singed16 = this.byte2hex(signed);
                licenses[2 * i] = singed16;
                licenses[2 * i + 1] = info;
            }

            return licenses;
        } catch (Exception var11) {
            var11.printStackTrace();
            return null;
        }
    }

    private void doObjToFile(String file, Object[] objs) {
        ObjectOutputStream oos = null;

        try {
            FileOutputStream fos = new FileOutputStream(file);
            oos = new ObjectOutputStream(fos);

            for(int i = 0; i < objs.length; ++i) {
                oos.writeObject(objs[i]);
            }
        } catch (Exception var14) {
            var14.printStackTrace();
        } finally {
            try {
                oos.close();
            } catch (IOException var13) {
                var13.printStackTrace();
            }

        }

    }

    private Object getObjFromFile(String file, int i) {
        ObjectInputStream ois = null;
        Object obj = null;

        try {
            InputStream in = CreateLicense.class.getClassLoader().getResourceAsStream(file);
            ois = new ObjectInputStream(in);

            for(int j = 0; j < i; ++j) {
                obj = ois.readObject();
            }
        } catch (Exception var16) {
            var16.printStackTrace();
        } finally {
            try {
                ois.close();
            } catch (IOException var15) {
                var15.printStackTrace();
            }

        }

        return obj;
    }

    private String byte2hex(byte[] b) {
        String hs = "";
        String stmp = "";

        for(int n = 0; n < b.length; ++n) {
            stmp = Integer.toHexString(b[n] & 255);
            if (stmp.length() == 1) {
                hs = hs + "0" + stmp;
            } else {
                hs = hs + stmp;
            }
        }

        return hs.toUpperCase();
    }

    public static byte[] parseHexStr2Byte(String hexStr) {
        if (hexStr.length() < 1) {
            return null;
        } else {
            byte[] result = new byte[hexStr.length() / 2];

            for(int i = 0; i < hexStr.length() / 2; ++i) {
                int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
                int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2), 16);
                result[i] = (byte)(high * 16 + low);
            }

            return result;
        }
    }

    public Object[] createLicense(List<String> srcstr, String order, String fileurl, String keyurl) {
        try {
            List<String> infos = new ArrayList();
            Iterator var7 = srcstr.iterator();

            while(var7.hasNext()) {
                String info = (String)var7.next();
                byte[] encode = this.fileDes(order, ENCODE, info);
                infos.add(this.byte2hex(encode));
            }

            return this.signToInfo(infos, fileurl, keyurl);
        } catch (NoSuchAlgorithmException var9) {
            var9.printStackTrace();
            System.out.println("create License fail!");
        } catch (NoSuchPaddingException var10) {
            var10.printStackTrace();
            System.out.println("create License fail!");
        } catch (InvalidKeyException var11) {
            var11.printStackTrace();
            System.out.println("create License fail!");
        } catch (IllegalBlockSizeException var12) {
            var12.printStackTrace();
            System.out.println("create License fail!");
        } catch (BadPaddingException var13) {
            var13.printStackTrace();
            System.out.println("create License fail!");
        } catch (Exception var14) {
            var14.printStackTrace();
            System.out.println("create License fail!");
        }

        return null;
    }

    public static void main(String[] args) throws Exception {
        CreateLicense jiami = new CreateLicense();
        List<String> srcstr = new ArrayList();
        srcstr.add("BFEBFBFF000406E3|ALL|2017-12-30");
        jiami.createLicense(srcstr, "capaa", "d://des//mchz.license", "d://des//private.key");
    }
}
