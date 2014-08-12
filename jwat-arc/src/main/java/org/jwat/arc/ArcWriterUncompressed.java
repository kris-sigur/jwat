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
package org.jwat.arc;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * ARC Writer implementation for writing uncompressed files.
 *
 * @author nicl
 */
public class ArcWriterUncompressed extends ArcWriter {

    /**
     * Construct an unbuffered ARC writer used to write uncompressed records.
     * @param out outputstream to write to
     */
    ArcWriterUncompressed(OutputStream out) {
        if (out == null) {
            throw new IllegalArgumentException(
                    "The 'out' parameter is null!");
        }
        this.out = out;
        init();
    }

    /**
     * Construct a buffered ARC writer used to write uncompressed records.
     * @param out outputstream to stream to
     * @param buffer_size outputstream buffer size
     * @throws IllegalArgumentException if out is null.
     * @throws IllegalArgumentException if buffer_size <= 0.
     */
    ArcWriterUncompressed(OutputStream out, int buffer_size) {
        if (out == null) {
            throw new IllegalArgumentException(
                    "The 'out' parameter is null!");
        }
        if (buffer_size <= 0) {
            throw new IllegalArgumentException(
                    "The 'buffer_size' parameter is less than or equal to zero!");
        }
        this.out = new BufferedOutputStream(out, buffer_size);
        init();
    }

    @Override
    public boolean isCompressed() {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (state == S_HEADER_WRITTEN || state == S_PAYLOAD_WRITTEN) {
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
        if (state == S_HEADER_WRITTEN || state == S_PAYLOAD_WRITTEN) {
            closeRecord_impl();
            state = S_RECORD_CLOSED;
        } else if (state == S_INIT) {
            throw new IllegalStateException("Write a record before closing it!");
        }
    }

    /*
     * state changed to S_HEADER_WRITTEN
     * Sets the header and headerContentLength fields.
     * payloadWrittenTotal is set to 0
     * @see org.jwat.arc.ArcWriter#writeHeader(org.jwat.arc.ArcRecordBase)
     */
    @Override
    public byte[] writeHeader(ArcRecordBase record) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException(
                    "The 'record' parameter is null!");
        }
        if (state == S_HEADER_WRITTEN) {
            throw new IllegalStateException("Headers written back to back!");
        } else if (state == S_PAYLOAD_WRITTEN) {
            closeRecord_impl();
        }
        return writeHeader_impl(record);
    }

}
