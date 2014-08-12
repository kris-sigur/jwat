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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.jwat.gzip.GzipReader;

@RunWith(JUnit4.class)
public class TestWarcReaders_Params {

    @Test
    public void test_parameters_readers() {
        InputStream is;

        try {
            /*
             * WarcReaderUncompressed.
             */

            WarcReaderUncompressed readerUncompressed;

            readerUncompressed = new WarcReaderUncompressed();
            Assert.assertFalse(readerUncompressed.isCompressed());

            Assert.assertFalse(readerUncompressed.getBlockDigestEnabled());
            readerUncompressed.setBlockDigestEnabled(true);
            Assert.assertTrue(readerUncompressed.getBlockDigestEnabled());
            readerUncompressed.setBlockDigestEnabled(false);
            Assert.assertFalse(readerUncompressed.getBlockDigestEnabled());

            Assert.assertFalse(readerUncompressed.getPayloadDigestEnabled());
            readerUncompressed.setPayloadDigestEnabled(true);
            Assert.assertTrue(readerUncompressed.getPayloadDigestEnabled());
            readerUncompressed.setPayloadDigestEnabled(false);
            Assert.assertFalse(readerUncompressed.getPayloadDigestEnabled());

            Assert.assertNull(readerUncompressed.getBlockDigestAlgorithm());
            Assert.assertTrue(readerUncompressed.setBlockDigestAlgorithm("sha1"));
            Assert.assertNotNull(readerUncompressed.getBlockDigestAlgorithm());
            Assert.assertTrue(readerUncompressed.setBlockDigestAlgorithm(null));
            Assert.assertNull(readerUncompressed.getBlockDigestAlgorithm());
            Assert.assertTrue(readerUncompressed.setBlockDigestAlgorithm(""));
            Assert.assertNull(readerUncompressed.getBlockDigestAlgorithm());
            Assert.assertFalse(readerUncompressed.setBlockDigestAlgorithm("shaft1"));
            Assert.assertTrue(readerUncompressed.setBlockDigestAlgorithm(null));

            Assert.assertNull(readerUncompressed.getPayloadDigestAlgorithm());
            Assert.assertTrue(readerUncompressed.setPayloadDigestAlgorithm("sha1"));
            Assert.assertNotNull(readerUncompressed.getPayloadDigestAlgorithm());
            Assert.assertTrue(readerUncompressed.setPayloadDigestAlgorithm(null));
            Assert.assertNull(readerUncompressed.getPayloadDigestAlgorithm());
            Assert.assertTrue(readerUncompressed.setPayloadDigestAlgorithm(""));
            Assert.assertNull(readerUncompressed.getPayloadDigestAlgorithm());
            Assert.assertFalse(readerUncompressed.setPayloadDigestAlgorithm("shaft1"));
            Assert.assertNull(readerUncompressed.getPayloadDigestAlgorithm());

            Assert.assertEquals("base32", readerUncompressed.getBlockDigestEncoding());
            readerUncompressed.setBlockDigestEncoding("BASE16");
            Assert.assertEquals("base16", readerUncompressed.getBlockDigestEncoding());
            readerUncompressed.setBlockDigestEncoding("base64");
            Assert.assertEquals("base64", readerUncompressed.getBlockDigestEncoding());
            readerUncompressed.setBlockDigestEncoding(null);
            Assert.assertNull(readerUncompressed.getBlockDigestEncoding());
            readerUncompressed.setBlockDigestEncoding("");
            Assert.assertNull(readerUncompressed.getBlockDigestEncoding());

            Assert.assertEquals("base32", readerUncompressed.getPayloadDigestEncoding());
            readerUncompressed.setPayloadDigestEncoding("BASE16");
            Assert.assertEquals("base16", readerUncompressed.getPayloadDigestEncoding());
            readerUncompressed.setPayloadDigestEncoding("base64");
            Assert.assertEquals("base64", readerUncompressed.getPayloadDigestEncoding());
            readerUncompressed.setPayloadDigestEncoding(null);
            Assert.assertNull(readerUncompressed.getPayloadDigestEncoding());
            readerUncompressed.setPayloadDigestEncoding("");
            Assert.assertNull(readerUncompressed.getPayloadDigestEncoding());

            readerUncompressed = new WarcReaderUncompressed();
            try {
                readerUncompressed = new WarcReaderUncompressed(null);
            } catch (IllegalArgumentException e) {
                readerUncompressed = null;
            }
            Assert.assertNull(readerUncompressed);

            readerUncompressed = new WarcReaderUncompressed();
            try {
                readerUncompressed.getNextRecord();
                Assert.fail("Exception expected!");
            } catch (IllegalStateException e) {
            }

            try {
                readerUncompressed.getNextRecordFrom(null, -2);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                readerUncompressed.getNextRecordFrom(null, 0, 42);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            is = new ByteArrayInputStream(new byte[] {42});

            try {
                readerUncompressed.getNextRecordFrom(is, -2);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                readerUncompressed.getNextRecordFrom(is, -2, 42);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                readerUncompressed.getNextRecordFrom(is, -1, -1);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                readerUncompressed.getNextRecordFrom(is, 0, 0);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            is.close();
            is = null;

            /*
             * WarcReaderCompressed.
             */

            WarcReaderCompressed readerCompressed;

            readerCompressed = new WarcReaderCompressed();
            Assert.assertTrue(readerCompressed.isCompressed());

            Assert.assertFalse(readerCompressed.getBlockDigestEnabled());
            readerCompressed.setBlockDigestEnabled(true);
            Assert.assertTrue(readerCompressed.getBlockDigestEnabled());
            readerCompressed.setBlockDigestEnabled(false);
            Assert.assertFalse(readerCompressed.getBlockDigestEnabled());

            Assert.assertFalse(readerCompressed.getPayloadDigestEnabled());
            readerCompressed.setPayloadDigestEnabled(true);
            Assert.assertTrue(readerCompressed.getPayloadDigestEnabled());
            readerCompressed.setPayloadDigestEnabled(false);
            Assert.assertFalse(readerCompressed.getPayloadDigestEnabled());

            Assert.assertNull(readerCompressed.getBlockDigestAlgorithm());
            Assert.assertTrue(readerCompressed.setBlockDigestAlgorithm("sha1"));
            Assert.assertNotNull(readerCompressed.getBlockDigestAlgorithm());
            Assert.assertTrue(readerCompressed.setBlockDigestAlgorithm(null));
            Assert.assertNull(readerCompressed.getBlockDigestAlgorithm());
            Assert.assertTrue(readerCompressed.setBlockDigestAlgorithm(""));
            Assert.assertNull(readerCompressed.getBlockDigestAlgorithm());
            Assert.assertFalse(readerCompressed.setBlockDigestAlgorithm("shaft1"));
            Assert.assertNull(readerCompressed.getBlockDigestAlgorithm());

            Assert.assertNull(readerCompressed.getPayloadDigestAlgorithm());
            Assert.assertTrue(readerCompressed.setPayloadDigestAlgorithm("sha1"));
            Assert.assertNotNull(readerCompressed.getPayloadDigestAlgorithm());
            Assert.assertTrue(readerCompressed.setPayloadDigestAlgorithm(null));
            Assert.assertNull(readerCompressed.getPayloadDigestAlgorithm());
            Assert.assertTrue(readerCompressed.setPayloadDigestAlgorithm(""));
            Assert.assertNull(readerCompressed.getPayloadDigestAlgorithm());
            Assert.assertFalse(readerCompressed.setPayloadDigestAlgorithm("shaft1"));
            Assert.assertNull(readerCompressed.getPayloadDigestAlgorithm());

            Assert.assertEquals("base32", readerUncompressed.getBlockDigestEncoding());
            readerUncompressed.setBlockDigestEncoding("BASE16");
            Assert.assertEquals("base16", readerUncompressed.getBlockDigestEncoding());
            readerUncompressed.setBlockDigestEncoding("base64");
            Assert.assertEquals("base64", readerUncompressed.getBlockDigestEncoding());
            readerUncompressed.setBlockDigestEncoding(null);
            Assert.assertNull(readerUncompressed.getBlockDigestEncoding());
            readerUncompressed.setBlockDigestEncoding("");
            Assert.assertNull(readerUncompressed.getBlockDigestEncoding());

            Assert.assertEquals("base32", readerUncompressed.getPayloadDigestEncoding());
            readerUncompressed.setPayloadDigestEncoding("BASE16");
            Assert.assertEquals("base16", readerUncompressed.getPayloadDigestEncoding());
            readerUncompressed.setPayloadDigestEncoding("base64");
            Assert.assertEquals("base64", readerUncompressed.getPayloadDigestEncoding());
            readerUncompressed.setPayloadDigestEncoding(null);
            Assert.assertNull(readerUncompressed.getPayloadDigestEncoding());
            readerUncompressed.setPayloadDigestEncoding("");
            Assert.assertNull(readerUncompressed.getPayloadDigestEncoding());

            readerCompressed = new WarcReaderCompressed();
            try {
                readerCompressed = new WarcReaderCompressed(null);
            } catch (IllegalArgumentException e) {
                readerCompressed = null;
            }
            Assert.assertNull(readerCompressed);

            readerCompressed = new WarcReaderCompressed();
            try {
                readerCompressed = new WarcReaderCompressed(null, 42);
            } catch (IllegalArgumentException e) {
                readerCompressed = null;
            }
            Assert.assertNull(readerCompressed);

            GzipReader gzipReader = new GzipReader(new ByteArrayInputStream(new byte[] {42}));

            readerCompressed = new WarcReaderCompressed();
            try {
                readerCompressed = new WarcReaderCompressed(gzipReader, -1);
            } catch (IllegalArgumentException e) {
                readerCompressed = null;
            }
            Assert.assertNull(readerCompressed);

            readerCompressed = new WarcReaderCompressed();
            try {
                readerCompressed = new WarcReaderCompressed(gzipReader, 0);
            } catch (IllegalArgumentException e) {
                readerCompressed = null;
            }
            Assert.assertNull(readerCompressed);

            gzipReader.close();
            gzipReader = null;

            readerCompressed = new WarcReaderCompressed();
            try {
                readerCompressed.getNextRecord();
                Assert.fail("Exception expected!");
            } catch (IllegalStateException e) {
            }

            try {
                readerCompressed.getNextRecordFrom(null, -2L);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                readerCompressed.getNextRecordFrom(null, 0, 42);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            is = new ByteArrayInputStream(new byte[] {42});

            try {
                readerCompressed.getNextRecordFrom(is, -2L);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                readerCompressed.getNextRecordFrom(is, -2, 42);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                readerCompressed.getNextRecordFrom(is, -1, -1);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                readerCompressed.getNextRecordFrom(is, 0, 0);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            is.close();
            is = null;

            /*
             * WarcReaderFactory.
             */

            is = new ByteArrayInputStream(new byte[] {42});

            try {
                WarcReaderFactory.getReader(null, 42);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReader(is, -1);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReader(is, 0);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReader(null);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReaderUncompressed(null, 42);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReaderUncompressed(is, -1);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReaderUncompressed(is, 0);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReaderUncompressed(null);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReaderCompressed(null, 42);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReaderCompressed(is, -1);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReaderCompressed(is, 0);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            try {
                WarcReaderFactory.getReaderCompressed(null);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }

            is.close();
            is = null;
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception!");
        }
    }

}
