package com.couchbase.lite.util;

import com.couchbase.lite.Database;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

public class FileEncryptionUtils {

    private static Cipher mCipher;
    private static SecretKey mKey;
    private static IvParameterSpec mIv;

    private static Cipher getCipher() {
        if (mCipher == null) {
            try {
                mCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            } catch (Exception e) {
                // ..
            }
        }

        return mCipher;
    }

    public static void setKey(final SecretKey key, final IvParameterSpec iv) {
        mKey = key;
        mIv = iv;
    }

    public static InputStream readFile(final File file) {
        FileInputStream fs = null;

        try {
            getCipher().init(Cipher.DECRYPT_MODE, mKey);

            fs = new FileInputStream(file);

//            return new CipherInputStream(fs, getCipher());
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
            getCipher().init(Cipher.ENCRYPT_MODE, mKey);

            fs = new FileOutputStream(file);

//            return new CipherOutputStream(fs, getCipher());
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
            getCipher().init(Cipher.ENCRYPT_MODE, mKey);

            final OutputStream fs = new FileOutputStream(file);

            out = new CipherOutputStream(fs, getCipher());

//            StreamUtils.copyStream(in, out);
            StreamUtils.copyStream(in, fs);

            return true;
        } catch (Exception e) {
            Log.e(Database.TAG, "FileEncryptionUtils failed to write to a file", e);
            return false;
        } finally {
            try { in.close(); out.close(); } catch (Exception e) { /** ignore **/ }
        }
    }
}
