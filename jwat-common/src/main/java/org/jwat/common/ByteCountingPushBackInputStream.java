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
package org.jwat.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

/**
 * Basic <code>PushBackInputStream</code> that also keeps track of the number
 * of consumed bytes at any given time.
 *
 * @author nicl
 */
public class ByteCountingPushBackInputStream extends PushbackInputStream {

    /** Read line initial size. */
    public static final int READLINE_INITIAL_SIZE = 128;

    /** Pushback buffer size. */
    protected int pushback_size;

    /** Offset relative to beginning of stream. */
    protected long consumed = 0;

    /** Byte counter which can also be changed. */
    protected long counter = 0;
    
    // First 'pushback_size' bytes are reserved for the pushback buffer, the rest are for read ahead buffer
    protected byte[] buf; 
    protected int bufPos; // Position in buf of next byte to read
    protected int bufLen; // Position in buf of the last byte to read +1
    protected boolean underlyingEmpty;

    /**
     * Given an <code>InputStream</code> and a push back buffer size returns
     * a wrapped input stream with push back capabilities.
     * @param in <code>InputStream</code> to wrap
     * @param size push back buffer size
     */
    public ByteCountingPushBackInputStream(InputStream in, int size) {
        super(in, 1); // We dont actually use the underlying buffer
        pushback_size = size;
        buf = new byte[pushback_size+10000];
        bufPos = pushback_size;
        bufLen = pushback_size;
        underlyingEmpty=false;
    }

    /**
     * Get the pushback buffer size.
     * @return pushback buffer size
     */
    public int getPushbackSize() {
        return pushback_size;
    }

    /**
     * Retrieve the number of bytes consumed by this stream.
     * @return current byte offset in this stream
     */
    public long getConsumed() {
        return consumed;
    }

    /**
     * Change the counter value.
     * Useful for reading zero indexed relative data.
     * @param bytes new counter value
     */
    public void setCounter(long bytes) {
        counter = bytes;
    }

    /**
     * Retrieve the current counter value.
     * @return current counter value
     */
    public long getCounter() {
        return counter;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int readlimit) {
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read() throws IOException {
    	while (bufPos>=bufLen && !underlyingEmpty) {
    		fillBuffer();
    	}
    	if (underlyingEmpty) {
    		return -1;
    	}
    	consumed++;
    	counter++;
    	return buf[bufPos++] & 0xff;
    }

    /*
     * The super method did this anyway causing a double amount of
     * consumed bytes.
     * @see java.io.FilterInputStream#read(byte[])
     */
    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    protected void fillBuffer() throws IOException {
    	if (bufPos<bufLen) {
   			throw new IllegalStateException("Internal buffer not exhausted yet");
    	}
    	bufPos = pushback_size;
    	int read = super.read(buf,pushback_size,buf.length-pushback_size);
    	bufLen = bufPos + read;
    	underlyingEmpty = read==-1;
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
    	if (bufPos>=bufLen) {
    		fillBuffer();
        	if (underlyingEmpty) {
        		return -1;
        	}
    	}
    	int bytesRead = -1; 
		int leftInBuffer = bufLen-bufPos;
    	if (bufLen<0) {
    		bytesRead = -1;
    	} else {
    		// Return what is in the buffer up to len or end of buffer
    		bytesRead = Math.min(leftInBuffer, len);
    		System.arraycopy(buf, bufPos, b, off, bytesRead);
    		bufPos+=bytesRead;
    	} 
    	consumed+=bytesRead;
    	counter+=bytesRead;
        return bytesRead;
    }

    @Override
    public long skip(long n) throws IOException {
		int bytesInBuffer = bufLen-bufPos;
    	long bytesSkipped = 0;
		if (n<bytesInBuffer) {
			bufPos+=n;
			bytesSkipped=n;
		} else if (bytesInBuffer<0) {
			bytesSkipped+=super.skip(n); 
		} else {
			long skipUnderlying = n-bytesInBuffer;
			bufPos = bufLen;
			bytesSkipped = bytesInBuffer;
			//We've exhausted the buffer at this point, so can go straight to underlying stream for the rest
			bytesSkipped+=super.skip(skipUnderlying); 
		}
        consumed += bytesSkipped;
        counter += bytesSkipped;
        return bytesSkipped;
    }

    @Override
    public void unread(int b) throws IOException {
    	if (bufPos==0) {
            throw new IOException("Push back buffer too small");
    	}
   		// Back up in the buffer to make space for the unread byte
        buf[--bufPos]=(byte)b;
        consumed--;
        counter--;
    }

    /*
     * The super method did this anyway causing a double amount of
     * un-consumed bytes.
     * @see java.io.PushbackInputStream#unread(byte[])
     */
    @Override
    public void unread(byte[] b) throws IOException {
        unread(b, 0, b.length);
    }

    @Override
    public void unread(byte[] b, int off, int len) throws IOException {
    	if (len > bufPos) {
            throw new IOException("Push back buffer too small");
    	}
   		bufPos=bufPos-len; // Back up enough in the buffer to make space for the unread bytes
        System.arraycopy(b, off, buf, bufPos, len);
        consumed -= len;
        counter -= len;
    }

    /**
     * Read a single line into a string.
     * @return single string line
     * @throws IOException if an i/o error occurs while reading line
     */
    public String readLine() throws IOException {
        StringBuffer sb = new StringBuffer(READLINE_INITIAL_SIZE);
        int b;
        while (true) {
            b = read();
            if (b == -1) {
                return null;    //Unexpected EOF
            }
            if (b == '\n'){
                break;
            }
            if (b != '\r') {
                sb.append((char) b);
            }
        }
        return sb.toString();
    }

    /**
     * Guaranteed to read the exact number of bytes that are in the array,
     * if not, the bytes are pushed back into the stream before returning.
     * @param buffer byte buffer to read bytes into
     * @return the number of bytes read into array
     * @throws IOException if an i/o error occurs while reading array
     */
    public int readFully(byte[] buffer) throws IOException {
        int readOffset = 0;
        int readRemaining = buffer.length;
        int readLast = 0;
        while (readRemaining > 0 && readLast != -1) {
            readRemaining -= readLast;
            readOffset += readLast;
            readLast = read(buffer, readOffset, readRemaining);
        }
        if (readRemaining > 0) {
            unread(buffer, 0, readOffset);
            readOffset = 0;
        }
        return readOffset;
    }

    /**
     * Peek into the stream by filling as much of the supplied array as
     * possible before unreading the stream and returning.
     * @param buffer byte buffer to read bytes into
     * @return the number of bytes read into array
     * @throws IOException if an i/o error occurs while reading array
     */
    public int peek(byte[] buffer) throws IOException {
        int readOffset = 0;
        int readRemaining = buffer.length;
        int readLast = 0;
        while (readRemaining > 0 && readLast != -1) {
            readRemaining -= readLast;
            readOffset += readLast;
            readLast = read(buffer, readOffset, readRemaining);
        }
        if (readOffset > 0) {
            unread(buffer, 0, readOffset);
        }
        return readOffset;
    }

}
