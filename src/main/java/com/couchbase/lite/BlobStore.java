/**
 * Original iOS version by  Jens Alfke
 * Ported to Android by Marty Schoch
 *
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.couchbase.lite;

import com.couchbase.lite.support.FileDirUtils;
import com.couchbase.lite.util.FileEncryptionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * A persistent content-addressable store for arbitrary-size data blobs.
 * Each blob is stored as a file named by its SHA-1 digest.
 * @exclude
 */
public class BlobStore {

    public static String FILE_EXTENSION = ".blob";
    public static String TMP_FILE_EXTENSION = ".blobtmp";
    public static String TMP_FILE_PREFIX = "tmp";

    private final String storeDir;
	private final Database db;

    public BlobStore(final String dir, final Database db) {
        storeDir = dir;
		this.db = db;

        final File directory = new File(storeDir);
        directory.mkdirs();

        if (!directory.isDirectory()) {
            throw new IllegalStateException(String.format("Unable to create directory for: %s", directory));
        }
    }

    public static BlobKey keyForBlob(byte[] data) {
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(data, 0, data.length);

            return new BlobKey(md.digest());
        } catch (NoSuchAlgorithmException e) {
            Log.e(Log.TAG_BLOB_STORE, "Error, SHA-1 digest is unavailable.");
            return null;
        }
    }

    public static BlobKey keyForBlobFromFile(File file, final String dbPass) {
        InputStream is = null;

        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-1");

            byte[] buffer = new byte[1024*4];
            int len;

            is = FileEncryptionUtils.readFile(file, dbPass);

            while((len = is.read(buffer)) > 0) {
                md.update(buffer, 0, len);
            }

            return new BlobKey(md.digest());
        } catch (NoSuchAlgorithmException e) {
            Log.e(Log.TAG_BLOB_STORE, "Error, SHA-1 digest is unavailable.");
            return null;
        } catch (IOException e) {
            Log.e(Log.TAG_BLOB_STORE, "Error readin tmp file to compute key");
            return null;
        } finally {
            try { is.close(); } catch (Exception e) { /** Ignore **/ }
        }
    }

	/**
	 * @return path to ENCRYPTED file. Never use outside of this class.
	 *
	 * To get decrypted data use {@link #blobForKey(BlobKey)}  or {@link #blobStreamForKey(BlobKey)}
	 */
    public String pathForKey(BlobKey key) {
        return storeDir + File.separator + BlobKey.convertToHex(key.getBytes()) + FILE_EXTENSION;
    }

	/**
	 * @return size of decrypted attachment stored in requested blob
	 */
    public long getSizeOfBlob(BlobKey key) {
	    byte[] decryptedAttachmentBytes = blobForKey(key);
	    return decryptedAttachmentBytes != null
			    ? decryptedAttachmentBytes.length
			    : 0L;
    }

    public boolean getKeyForFilename(BlobKey outKey, String filename) {
        if (!filename.endsWith(FILE_EXTENSION)) return false;

        //trim off extension
        final String rest = filename.substring(storeDir.length() + 1, filename.length() - FILE_EXTENSION.length());
        outKey.setBytes(BlobKey.convertFromHex(rest));

        return true;
    }

	/**
	 * @return bytes of decrypted file from requested blob
	 */
    public byte[] blobForKey(BlobKey key) {
	    InputStream inStream = blobStreamForKey(key);
	    if (inStream == null) return null;

	    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
	    try {
		    StreamUtils.copyStream(inStream, outStream);
		    return outStream.toByteArray();
	    } catch (IOException e) {
		    Log.e(Log.TAG_BLOB_STORE, "Error reading file", e);
		    return null;
	    }
    }

	/**
	 * @return stream of decrypted file from requested blob
	 */
	public InputStream blobStreamForKey(BlobKey key) {
        try {
            final String path = pathForKey(key);
            final File file = new File(path);

            if (!file.exists()) throw new FileNotFoundException(file.getAbsolutePath());
            if (!file.canRead()) throw new FileNotFoundException("Cannot read from file");

            return FileEncryptionUtils.readFile(file, db.getPassword());
        } catch (FileNotFoundException f) {
            Log.e(Log.TAG_BLOB_STORE, "Unexpected file not found in blob store", f);
            return null;
        } catch (Exception e) {
            Log.e(Log.TAG_BLOB_STORE, "Unable to open file", e);
            return null;
        }
    }

    public boolean storeBlobStream(InputStream inputStream, BlobKey outKey) {
        try {
            final File tmp = File.createTempFile(TMP_FILE_PREFIX, TMP_FILE_EXTENSION, new File(storeDir));
            final boolean written = FileEncryptionUtils.streamToFile(inputStream, tmp, db.getPassword());

            if (!written) {
                Log.e(Log.TAG_BLOB_STORE, "Failed to write file");
                return false;
            }

            final BlobKey newKey = keyForBlobFromFile(tmp, db.getPassword());
            outKey.setBytes(newKey.getBytes());

            final String path = pathForKey(outKey);
            final File file = new File(path);

            // object with this hash already exists, we should delete tmp file
            // or does not exist, we should rename tmp file to this name
            return (file.exists()) ? tmp.delete() : tmp.renameTo(file);
        } catch (IOException e) {
            Log.e(Log.TAG_BLOB_STORE, "Error writing blog to tmp file", e);
            return false;
        } catch (Exception ex) {
            Log.e(Log.TAG_BLOB_STORE, "Error encrypting tmp file", ex);
            return false;
        }
    }

    public boolean storeBlob(byte[] data, BlobKey outKey) {
        final BlobKey newKey = keyForBlob(data);
        outKey.setBytes(newKey.getBytes());

        final String path = pathForKey(outKey);
        final File file = new File(path);

        if (file.canRead()) return true; // file exists

        try {
            if (!FileEncryptionUtils.streamToFile(new ByteArrayInputStream(data), file, db.getPassword())) {
                throw new FileNotFoundException("Unable to write to file: " + file.getAbsolutePath());
            }
        } catch (FileNotFoundException e) {
            Log.e(Log.TAG_BLOB_STORE, "Error opening file for output", e);
            return false;
        } catch(Exception ioe) {
            Log.e(Log.TAG_BLOB_STORE, "Error writing to file", ioe);
            return false;
        }

        return true;
    }

    public Set<BlobKey> allKeys() {
        final Set<BlobKey> result = new HashSet<BlobKey>();
        final File file = new File(storeDir);
        final File[] contents = file.listFiles();

        for (final File attachment : contents) {
            if (attachment.isDirectory()) continue;

            final BlobKey attachmentKey = new BlobKey();

            getKeyForFilename(attachmentKey, attachment.getPath());
            result.add(attachmentKey);
        }

        return result;
    }

    public int count() {
        final File file = new File(storeDir);
        final File[] contents = file.listFiles();

        return contents.length;
    }

    public long totalDataSize() {
        long total = 0;

        final File file = new File(storeDir);
        final File[] contents = file.listFiles();

        for (final File attachment : contents) {
            total += attachment.length();
        }

        return total;
    }

    public int deleteBlobsExceptWithKeys(final List<BlobKey> keysToKeep) {
        int numDeleted = 0;

        final File file = new File(storeDir);
        final File[] contents = file.listFiles();

        for (final File attachment : contents) {
            final BlobKey attachmentKey = new BlobKey();
            getKeyForFilename(attachmentKey, attachment.getPath());

            if (keysToKeep != null && !keysToKeep.contains(attachmentKey)) {
                if (attachment.delete()) {
                    ++numDeleted;
                } else {
                    Log.e(Log.TAG_BLOB_STORE, "Error deleting attachment: %s", attachment);
                }
            }
        }

        return numDeleted;
    }

    public int deleteBlobs() {
        return deleteBlobsExceptWithKeys(null);
    }
    
    public boolean isGZipped(final BlobKey key) {
        int magic = 0;

        final String path = pathForKey(key);
        final File file = new File(path);

        if (file.canRead()) {
            InputStream raf = null;

            try {
                raf = FileEncryptionUtils.readFile(file, db.getPassword());

                magic = raf.read() & 0xff | ((raf.read() << 8) & 0xff00);
            } catch (Throwable e) {
                e.printStackTrace(System.err);
            } finally {
                try { raf.close(); } catch (Exception e) { /** Ignore **/ }
            }
        }

        return magic == GZIPInputStream.GZIP_MAGIC;
    }

    public File tempDir() {
        final File directory = new File(storeDir);
        final File tempDirectory = new File(directory, "temp_attachments");

        tempDirectory.mkdirs();

        if (!tempDirectory.isDirectory()) {
            throw new IllegalStateException(String.format("Unable to create directory for: %s", tempDirectory));
        }

        return tempDirectory;
    }

    /**
     * Encrypt attachments of non-encrypted DB
     *
     * @throws IllegalStateException if db can't be encrypted.
     */
    public void encryptBlobStore() {
        final String encryptionKey = db.getPassword();
        if (encryptionKey == null || encryptionKey.isEmpty()) {
            throw new IllegalStateException("No encryptionKey found: " + encryptionKey);
        }

        final File attachmentDirectory = new File(storeDir);
        final File[] blobs = attachmentDirectory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(FILE_EXTENSION);
            }
        });

        //skip encryption if nothing to encrypt
        if (blobs == null || blobs.length == 0) return;

        final File tempDir = new File(attachmentDirectory.getParent(), "dbtmp_" + attachmentDirectory.getName());
        final boolean tempDirCreated = tempDir.mkdir();
        if (!tempDirCreated) {
            throw new IllegalStateException("Unable to create temp dir for encrypting attachments: " + tempDir);
        }

        for (File blobFile : blobs) {
            try {
                final File encryptedBlobFile = new File(tempDir, blobFile.getName());
                final boolean blobEncrypted = FileEncryptionUtils.streamToFile(new FileInputStream(blobFile), encryptedBlobFile, encryptionKey);
                if (!blobEncrypted) {
                    throw new IllegalStateException("Unable to encrypt attachment " + blobFile);
                }
            } catch (FileNotFoundException e) {
                throw new IllegalStateException("Unable to encrypt attachment " + blobFile, e);
            }
        }

        final boolean originalDbAttachmentsDeleted = FileDirUtils.deleteRecursive(attachmentDirectory);
        if (!originalDbAttachmentsDeleted) {
            throw new IllegalStateException("Unable to swap with encrypted attachments. Unable to delete original attachments: " + attachmentDirectory);
        }

        final boolean encryptedAttachmentsMovedToOriginal = tempDir.renameTo(attachmentDirectory);
        if (!encryptedAttachmentsMovedToOriginal) {
            throw new IllegalStateException("Unable to move encrypted attachments to original attachments. " + tempDir + " -> " + attachmentDirectory);
        }
    }
}
