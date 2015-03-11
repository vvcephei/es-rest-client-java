package com.bazaarvoice.elasticsearch.client.core.util;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

public class InputStreams {
    public static InputStream of(BytesReference bytes) {
        return new ByteArrayInputStream(bytes.array());
    }

    /** useful for debugging */
    public static String toString(final InputStream response) {
        final StringWriter stringWriter = new StringWriter();
        int character;
        try {
            while ((character = response.read()) != -1) {
                stringWriter.write(character);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                response.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return stringWriter.toString();
    }

    /** sometimes the response comes back with nulls in the string. As far as I can tell, we just want to strip them out... */
    public static InputStream stripNulls(final InputStream inputStream) {
        return new InputStream() {
            @Override public int read() throws IOException {
                int read;
                while ((read = inputStream.read()) == 0) {}
                return read;
            }

            @Override public void close() throws IOException {
                inputStream.close();
            }
        };
    }
}
