package com.couchbase.lite.util;

import com.couchbase.lite.Database;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public class FileEncryptionUtils {
	private static Map<String, SecretKey> secretKeys = new HashMap<>();

    private static byte[] mSalt = new byte[] {
            (byte) 0xfa, (byte) 0x12, (byte) 0x21, (byte) 0x94,
            (byte) 0x99, (byte) 0x41, (byte) 0xbc, (byte) 0x03,
            (byte) 0x29, (byte) 0x73, (byte) 0x61, (byte) 0xdf,
            (byte) 0xa1, (byte) 0x10, (byte) 0x5b, (byte) 0x00
    };

    private static final String CIPHER = "AES/CBC/PKCS5Padding";

    private static Cipher getEncryptionCipher(final String dbPass) {
        try {
            final Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.ENCRYPT_MODE, getDatabaseKey(dbPass), new IvParameterSpec(mSalt));

            return cipher;
        } catch (Exception e) {
            Log.e(Database.TAG, "Failed to create encryption cipher", e);
            return null;
        }
    }

    private static Cipher getDecryptionCypher(final String dbPass) {
        try {
            final Cipher cipher = Cipher.getInstance(CIPHER);
            final AlgorithmParameterSpec spec = new IvParameterSpec(mSalt);
            cipher.init(Cipher.DECRYPT_MODE, getDatabaseKey(dbPass), spec);

            return cipher;
        } catch (Exception e) {
            Log.e(Database.TAG, "Failed to create decryption cipher", e);
            return null;
        }
    }

    private static SecretKey generateKey(final String key) {
        try {
            final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            final KeySpec spec = new PBEKeySpec(key.toCharArray(), mSalt, 1024, 256);

            return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");
        } catch (Exception e) {
            return null;
        }
    }

	private static SecretKey getDatabaseKey(String dbPass) {
		if (secretKeys.containsKey(dbPass)) return secretKeys.get(dbPass);

		SecretKey key = generateKey(dbPass);
		secretKeys.put(dbPass, key);
		return key;
	}

    public static InputStream readFile(final File file, final String dbPass) {
        FileInputStream fs = null;

        try {
            fs = new FileInputStream(file);

			if (getDatabaseKey(dbPass) == null) return fs;

            return new CipherInputStream(fs, getDecryptionCypher(dbPass));
        } catch (Exception e) {
            Log.e(Database.TAG, "FileEncryptionUtils failed to write to a file", e);
            return null;
        }
    }

    public static OutputStream writeFile(final File file, final String dbPass) {
        FileOutputStream fs = null;

        try {
            fs = new FileOutputStream(file);

			if (getDatabaseKey(dbPass) == null) return fs;

            return new CipherOutputStream(fs, getEncryptionCipher(dbPass));
        } catch (Exception e) {
            Log.e(Database.TAG, "FileEncryptionUtils failed to write to a file", e);
            return null;
        }
    }

    public static boolean streamToFile(final InputStream in, final File file, final String dbPass) {
        OutputStream out = null;

        try {
            final OutputStream fs = new FileOutputStream(file);

			if (getDatabaseKey(dbPass) == null) {
				StreamUtils.copyStream(in, fs);
				return true;
			}

            out = new CipherOutputStream(fs, getEncryptionCipher(dbPass));

            StreamUtils.copyStream(in, out);

            return true;
        } catch (Exception e) {
            Log.e(Database.TAG, "FileEncryptionUtils failed to write to a file", e);
            return false;
        } finally {
            try { in.close(); out.close(); } catch (Exception e) { /** ignore **/ }
        }
    }
}
