/*
 * Copyright (C) 2006 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.lite.util;

import java.io.IOException;
import java.io.InputStream;

import cz.msebera.android.httpclient.util.ByteArrayBuffer;

// COPY: Partially copied from android.text.TextUtils
public class TextUtils {
    /**
     * Returns a string containing the tokens joined by delimiters.
     * @param tokens an array objects to be joined. Strings will be formed from
     *     the objects by calling object.toString().
     */
    public static String join(CharSequence delimiter, Iterable tokens) {
        StringBuilder sb = new StringBuilder();
        boolean firstTime = true;
        for (Object token: tokens) {
            if (firstTime) {
                firstTime = false;
            } else {
                sb.append(delimiter);
            }
            sb.append(token);
        }
        return sb.toString();
    }

    public static byte[] read(InputStream is) throws IOException {
        final int initialCapacity = 1024;
        ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(initialCapacity);
        byte[] bytes = new byte[512];
        int offset = 0;
        int numRead = 0;

        while ((numRead = is.read(bytes, offset, bytes.length-offset)) >= 0) {
            byteArrayBuffer.append(bytes, 0, numRead);
            offset += numRead;
        }
        return byteArrayBuffer.toByteArray();
    }

}
