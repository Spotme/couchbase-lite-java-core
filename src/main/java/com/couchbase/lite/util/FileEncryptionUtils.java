package com.couchbase.lite.util;

import com.couchbase.lite.Database;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class FileEncryptionUtils {

    private static Cipher mECipher;
    private static Cipher mDCipher;

    private static SecretKeySpec mKey;

    private static IvParameterSpec mIV;

    private static Cipher getEncryptionCipher() {
        if (mECipher == null) {
            try {
                mECipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
                mECipher.init(Cipher.ENCRYPT_MODE, mKey, mIV);
            } catch (Exception e) {
                // ..
            }
        }

        return mECipher;
    }

    private static Cipher getDecryptionCypher() {
        if (mDCipher == null) {
            try {
                mDCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
                mDCipher.init(Cipher.DECRYPT_MODE, mKey, mIV);
            } catch (Exception e) {
                // ..
            }
        }

        return mDCipher;
    }

    public static void setKey(final SecretKeySpec key, final IvParameterSpec iv) {
        mKey = key;
        mIV = iv;
    }

    public static InputStream readFile(final File file) {
        FileInputStream fs = null;

        try {
            fs = new FileInputStream(file);

//            return new CipherInputStream(fs, getDecryptionCypher());
            return fs;
        } catch (Exception e) {
            Log.e(Database.TAG, "FileEncryptionUtils failed to write to a file", e);
            return null;
        } finally {
//            try { fs.close(); } catch (Exception e) { /** Ignore **/ }
        }
    }

    public static OutputStream writeFile(final File file) {
        FileOutputStream fs = null;

        try {
            fs = new FileOutputStream(file);

//            return new CipherOutputStream(fs, getEncryptionCipher());
            return fs;
        } catch (Exception e) {
            Log.e(Database.TAG, "FileEncryptionUtils failed to write to a file", e);
            return null;
        } finally {
//            try { fs.close(); } catch (Exception e) { /** Ignore **/ }
        }
    }

    public static boolean streamToFile(final InputStream in, final File file) {
        OutputStream out = null;

        try {
            final OutputStream fs = new FileOutputStream(file);

            out = new CipherOutputStream(fs, getEncryptionCipher());

            StreamUtils.copyStream(in, fs);
//            StreamUtils.copyStream(in, out);

            return true;
        } catch (Exception e) {
            Log.e(Database.TAG, "FileEncryptionUtils failed to write to a file", e);
            return false;
        } finally {
            try { in.close(); out.close(); } catch (Exception e) { /** ignore **/ }
        }
    }
}
