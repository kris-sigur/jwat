/**
 * Java Web Archive Toolkit - Software to read and validate ARC, WARC
 * and GZip files. (http://jwat.org/)
 * Copyright 2011-2012 Netarkivet.dk (http://netarkivet.dk/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jwat.warc;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.jwat.gzip.GzipConstants;
import org.jwat.gzip.GzipEntry;
import org.jwat.gzip.GzipWriter;

/**
 * WARC Writer implementation for writing GZip compressed files.
 * Use WarcWriterFactory to get an instance of this class.
 *
 * @author nicl
 */
public class WarcWriterCompressed extends WarcWriter {

    /** GZip Writer used. */
    protected GzipWriter writer;

    /** Current GZip entry. */
    protected GzipEntry entry;

    /**
     * Construct an unbuffered WARC writer used to write compressed records.
     * @param out outputstream to write to
     */
    WarcWriterCompressed(OutputStream out) {
        if (out == null) {
            throw new IllegalArgumentException(
                    "The 'out' parameter is null!");
        }
        writer = new GzipWriter(out);
        init();
    }

    /**
     * Construct a buffered WARC writer used to write compressed records.
     * @param out outputstream to stream to
     * @param buffer_size outputstream buffer size
     * @throws IllegalArgumentException if out is null or buffer_size <= 0
     */
    WarcWriterCompressed(OutputStream out, int buffer_size) {
        if (out == null) {
            throw new IllegalArgumentException(
                    "The 'out' parameter is null!");
        }
        if (buffer_size <= 0) {
            throw new IllegalArgumentException(
                    "The 'buffer_size' parameter is less than or equal to zero!");
        }
        writer = new GzipWriter(new BufferedOutputStream(out, buffer_size));
        init();
    }

    @Override
    public boolean isCompressed() {
        return true;
    }

    @Override
    public void close() throws IOException {
        if (entry != null) {
            closeRecord();
        }
        if (out != null) {
            out.flush();
            out.close();
            out = null;
        }
    }

    @Override
    public void closeRecord() throws IOException {
        if (state == S_INIT) {
            throw new IllegalStateException("Please write a record before closing it!");
        }
        if (entry != null) {
            closeRecord_impl();
            state = S_RECORD_CLOSED;
            entry.close();
            entry = null;
        }
    }

    /*
     * In this class "out" is the GZip output stream of the current GZip entry.
     * @see org.jwat.warc.WarcWriter#writeHeader(byte[], java.lang.Long)
     */
    @Override
    public void writeRawHeader(byte[] header_bytes, Long contentLength) throws IOException {
        if (header_bytes == null) {
            throw new IllegalArgumentException(
                    "The 'header_bytes' parameter is null!");
        }
        if (contentLength != null && contentLength < 0) {
            throw new IllegalArgumentException(
                    "The 'contentLength' parameter is negative!");
        }
        if (state == S_HEADER_WRITTEN) {
            throw new IllegalStateException("Headers written back to back!");
        } else if (state == S_PAYLOAD_WRITTEN) {
            closeRecord();
        }
        entry = new GzipEntry();
        entry.magic = GzipConstants.GZIP_MAGIC;
        entry.cm = GzipConstants.CM_DEFLATE;
        entry.flg = 0;
        entry.mtime = System.currentTimeMillis() / 1000;
        entry.xfl = 0;
        entry.os = GzipConstants.OS_UNKNOWN;
        writer.writeEntryHeader(entry);
        out = entry.getOutputStream();
        out.write(header_bytes);
        state = S_HEADER_WRITTEN;
        header = null;
        headerContentLength = contentLength;
        payloadWrittenTotal = 0;
    }

    /*
     * In this class "out" is the GZip output stream of the current GZip entry.
     * state changed to S_HEADER_WRITTEN
     * Sets the header and headerContentLength fields.
     * payloadWrittenTotal is set to 0
     * @see org.jwat.warc.WarcWriter#writeHeader(org.jwat.warc.WarcRecord)
     */
    @Override
    public byte[] writeHeader(WarcRecord record) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException(
                    "The 'record' parameter is null!");
        }
        if (state == S_HEADER_WRITTEN) {
            throw new IllegalStateException("Headers written back to back!");
        } else if (state == S_PAYLOAD_WRITTEN) {
            closeRecord();
        }
        entry = new GzipEntry();
        entry.magic = GzipConstants.GZIP_MAGIC;
        entry.cm = GzipConstants.CM_DEFLATE;
        entry.flg = 0;
        entry.mtime = System.currentTimeMillis() / 1000;
        entry.xfl = 0;
        entry.os = GzipConstants.OS_UNKNOWN;
        writer.writeEntryHeader(entry);
        out = entry.getOutputStream();
        return writeHeader_impl(record);
    }

    /*
     * state changed to S_PAYLOAD_WRITTEN;
     * @see org.jwat.warc.WarcWriter#streamPayload(java.io.InputStream)
     */
    @Override
    public long streamPayload(InputStream in) throws IOException {
        if (entry == null) {
            throw new IllegalStateException("Write a header before writing payload!");
        }
        return super.streamPayload(in);
    }

    /*
     * state changed to S_PAYLOAD_WRITTEN
     * @see org.jwat.warc.WarcWriter#writePayload(byte[])
     */
    @Override
    public long writePayload(byte[] b) throws IOException {
        if (entry == null) {
            throw new IllegalStateException("Write a header before writing payload!");
        }
        return super.writePayload(b);
    }

    /*
     * state changed to S_PAYLOAD_WRITTEN
     * @see org.jwat.warc.WarcWriter#writePayload(byte[], int, int)
     */
    @Override
    public long writePayload(byte[] b, int offset, int len) throws IOException {
        if (entry == null) {
            throw new IllegalStateException("Write a header before writing payload!");
        }
        return super.writePayload(b, offset, len);
    }

}
