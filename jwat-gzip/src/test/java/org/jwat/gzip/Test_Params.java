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
package org.jwat.gzip;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class Test_Params {

    @Test
    public void test_parameters() throws IOException {
        GzipReader reader;

        GzipConstants constants = new GzipConstants();
        Assert.assertNotNull(constants);

        ByteArrayInputStream in = new ByteArrayInputStream(new byte[] {42});

        try {
            reader = new GzipReader(null);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }
        try {
            reader = new GzipReader(null, 1024);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }
        try {
            reader = new GzipReader(in, -1);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }
        try {
            reader = new GzipReader(in, -0);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }
        reader = new GzipReader(in, 1024);
        Assert.assertNotNull(reader);

        GzipWriter writer;

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
        writer = new GzipWriter(null);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }
        try {
            writer = new GzipWriter(null, 1024);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }
        try {
            writer = new GzipWriter(out, -1);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }
        try {
            writer = new GzipWriter(out, 0);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }
        writer = new GzipWriter(out, 1024);

        GzipEntry entry = new GzipEntry();
        entry.magic = GzipConstants.GZIP_MAGIC;
        entry.cm = GzipConstants.CM_DEFLATE;
        entry.flg = 0;
        entry.mtime = System.currentTimeMillis() / 1000;
        entry.xfl = 0;
        entry.os = GzipConstants.OS_AMIGA;

        Assert.assertEquals(Deflater.DEFAULT_COMPRESSION, writer.getCompressionLevel());
        writer.setCompressionLevel(Deflater.NO_COMPRESSION);
        Assert.assertEquals(Deflater.NO_COMPRESSION, writer.getCompressionLevel());
        writer.setCompressionLevel(Deflater.DEFAULT_COMPRESSION);
        Assert.assertEquals(Deflater.DEFAULT_COMPRESSION, writer.getCompressionLevel());
        writer.setCompressionLevel(1);
        Assert.assertEquals(1, writer.getCompressionLevel());
        writer.setCompressionLevel(2);
        Assert.assertEquals(2, writer.getCompressionLevel());
        writer.setCompressionLevel(3);
        Assert.assertEquals(3, writer.getCompressionLevel());
        writer.setCompressionLevel(4);
        Assert.assertEquals(4, writer.getCompressionLevel());
        writer.setCompressionLevel(5);
        Assert.assertEquals(5, writer.getCompressionLevel());
        writer.setCompressionLevel(6);
        Assert.assertEquals(6, writer.getCompressionLevel());
        writer.setCompressionLevel(7);
        Assert.assertEquals(7, writer.getCompressionLevel());
        writer.setCompressionLevel(8);
        Assert.assertEquals(8, writer.getCompressionLevel());
        writer.setCompressionLevel(9);
        Assert.assertEquals(9, writer.getCompressionLevel());
        try {
            writer.setCompressionLevel(-2);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }
        Assert.assertEquals(9, writer.getCompressionLevel());
        try {
            writer.setCompressionLevel(10);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }
        Assert.assertEquals(9, writer.getCompressionLevel());

        try {
            writer.writeEntryHeader(null);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }

        try {
            entry.writeFrom(null);
            Assert.fail("Exception expected!");
        } catch (IllegalStateException e) {
        }

        writer.writeEntryHeader(entry);

        try {
            entry.writeFrom(null);
            Assert.fail("Exception expected!");
        } catch (IllegalArgumentException e) {
        }

        entry.writeFrom(in);
        entry.close();
        Assert.assertFalse(entry.diagnostics.hasErrors());
        Assert.assertFalse(entry.diagnostics.hasWarnings());

        writer.setCompressionLevel(1);
        writer.writeEntryHeader(entry);
        entry.writeFrom(in);
        entry.close();
        Assert.assertFalse(entry.diagnostics.hasErrors());
        Assert.assertFalse(entry.diagnostics.hasWarnings());

        writer.close();
    }

}
