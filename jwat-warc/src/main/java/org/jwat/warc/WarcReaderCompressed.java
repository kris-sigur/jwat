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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.jwat.common.ByteCountingPushBackInputStream;
import org.jwat.gzip.GzipEntry;
import org.jwat.gzip.GzipReader;

/**
 * WARC Reader implementation for reading GZip compressed files.
 * Use WarcReaderFactory to get an instance of this class.
 *
 * @author nicl
 */
public class WarcReaderCompressed extends WarcReader {

    /** Buffer size used by <code>PushbackInputStream</code>. */
    public static final int PUSHBACK_BUFFER_SIZE = 32;

    /** WARC file <code>InputStream</code>. */
    protected GzipReader reader;

    /** Buffer size, if any, to use on GZip entry <code>InputStream</code>. */
    protected int bufferSize;

    /** GZip reader used for the current record, if random access methods used. */
    protected GzipReader currentReader;

    /** GZip entry for the current record, if random access methods used. */
    protected GzipEntry currentEntry;

    /**
     * This constructor is used to get random access to records.
     * The records are then accessed using the getNextRecordFrom methods
     * using a supplied input stream for each record.
     */
    public WarcReaderCompressed() {
        init();
    }

    /**
     * Construct reader using the supplied input stream.
     * This method is primarily for sequential access to records.
     * @param reader GZip reader
     */
    public WarcReaderCompressed(GzipReader reader) {
        if (reader == null) {
            throw new IllegalArgumentException(
                    "'reader' is null");
        }
        this.reader = reader;
        init();
    }

    /**
     * Construct object using supplied <code>GzipInputStream</code>.
     * This method is primarily for sequential access to records.
     * @param reader GZip reader
     * @param buffer_size buffer size used on entries
     */
    public WarcReaderCompressed(GzipReader reader, int buffer_size) {
        if (reader == null) {
            throw new IllegalArgumentException(
                    "'reader' is null");
        }
        if (buffer_size <= 0) {
            throw new IllegalArgumentException(
                    "The 'buffer_size' is less than or equal to zero: "
                    + buffer_size);
        }
        this.reader = reader;
        this.bufferSize = buffer_size;
        init();
    }

    @Override
    public boolean isCompressed() {
        return true;
    }

    @Override
    public void close() {
        if (currentRecord != null) {
            try {
                currentRecord.close();
            } catch (IOException e) { /* ignore */ }
            currentRecord = null;
        }
        if (reader != null) {
            startOffset = reader.getStartOffset();
            consumed = reader.getOffset();
            try {
                reader.close();
            } catch (IOException e) { /* ignore */ }
            reader = null;
        }
    }

    @Override
    protected void recordClosed() {
        if (currentEntry != null) {
            try {
                currentEntry.close();
                consumed += currentEntry.consumed;
            } catch (IOException e) { /* ignore */ }
            currentEntry = null;
        } else {
            throw new IllegalStateException("'currentEntry' is null, this should never happen!");
        }
    }

    /** Cached start offset used after the reader is closed. */
    protected long startOffset = -1;

    /**
     * Get the offset of the current WARC record from the GZip entry or -1 if
     * no records have been read yet.
     * @return offset of the current WARC record from the GZip entry or -1
     */
    @Override
    public long getStartOffset() {
        if (reader != null) {
            return reader.getStartOffset();
        } else {
            return startOffset;
        }
    }

    /**
     * Get the current offset in the WARC <code>GzipReader</code>.
     * @return offset in WARC <code>InputStream</code>
     */
    @Override
    public long getOffset() {
        if (reader != null) {
            return reader.getOffset();
        } else {
            return consumed;
        }
    }

    /** Get number of bytes consumed by the WARC <code>GzipReader</code>.
     * @return number of bytes consumed by the WARC <code>GzipReader</code>
     */
    @Override
    public long getConsumed() {
        if (reader != null) {
            return reader.getOffset();
        } else {
            return consumed;
        }
    }

    @Override
    public WarcRecord getNextRecord() throws IOException {
        if (currentRecord != null) {
            currentRecord.close();
        }
        if (reader == null) {
            throw new IllegalStateException(
                    "This reader has been initialized with an incompatible constructor, 'reader' is null");
        }
        currentRecord = null;
        currentReader = reader;
        currentEntry = reader.getNextEntry();
        if (currentEntry != null) {
            ByteCountingPushBackInputStream pbin;
            if (bufferSize > 0) {
                pbin = new ByteCountingPushBackInputStream(
                        new BufferedInputStream(
                                currentEntry.getInputStream(), bufferSize),
                                PUSHBACK_BUFFER_SIZE);
            }
            else {
                pbin = new ByteCountingPushBackInputStream(
                        currentEntry.getInputStream(), PUSHBACK_BUFFER_SIZE);
            }
            currentRecord = WarcRecord.parseRecord(pbin, this);
        }
        if (currentRecord != null) {
            startOffset = currentEntry.getStartOffset();
            currentRecord.header.startOffset = currentEntry.getStartOffset();
        }
        return currentRecord;
    }

    @Override
    public WarcRecord getNextRecordFrom(InputStream rin, long offset)
                                                        throws IOException {
        if (currentRecord != null) {
            currentRecord.close();
        }
        if (reader != null) {
            throw new IllegalStateException(
                    "This reader has been initialized with an incompatible constructor, 'reader' is not null");
        }
        if (rin == null) {
            throw new IllegalArgumentException(
                    "The inputstream 'rin' is null");
        }
        if (offset < -1) {
            throw new IllegalArgumentException(
                    "The 'offset' is less than -1: " + offset);
        }
        currentRecord = null;
        currentReader = new GzipReader(rin);
        currentEntry = currentReader.getNextEntry();
        if (currentEntry != null) {
            ByteCountingPushBackInputStream pbin =
                    new ByteCountingPushBackInputStream(
                            currentEntry.getInputStream(), PUSHBACK_BUFFER_SIZE);
            currentRecord = WarcRecord.parseRecord(pbin, this);
        }
        if (currentRecord != null) {
            startOffset = offset;
            currentRecord.header.startOffset = offset;
        }
        return currentRecord;
    }

    @Override
    public WarcRecord getNextRecordFrom(InputStream rin, long offset,
                                        int buffer_size) throws IOException {
        if (currentRecord != null) {
            currentRecord.close();
        }
        if (reader != null) {
            throw new IllegalStateException(
                    "This reader has been initialized with an incompatible constructor, 'reader' is not null");
        }
        if (rin == null) {
            throw new IllegalArgumentException(
                    "The inputstream 'rin' is null");
        }
        if (offset < -1) {
            throw new IllegalArgumentException(
                    "The 'offset' is less than -1: " + offset);
        }
        if (buffer_size <= 0) {
            throw new IllegalArgumentException(
                    "The 'buffer_size' is less than or equal to zero: "
                    + buffer_size);
        }
        currentRecord = null;
        currentReader = new GzipReader(rin);
        currentEntry = currentReader.getNextEntry();
        if (currentEntry != null) {
            ByteCountingPushBackInputStream pbin =
                    new ByteCountingPushBackInputStream(
                            new BufferedInputStream(
                                    currentEntry.getInputStream(), buffer_size),
                                    PUSHBACK_BUFFER_SIZE);
            currentRecord = WarcRecord.parseRecord(pbin, this);
        }
        if (currentRecord != null) {
            startOffset = offset;
            currentRecord.header.startOffset = offset;
        }
        return currentRecord;
    }

}
