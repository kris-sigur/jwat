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
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestCharCountingStringReader {

    private int min;
    private int max;
    private int runs;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                {1, 1024, 2}
        });
    }

    public TestCharCountingStringReader(int min, int max, int runs) {
        this.min = min;
        this.max = max;
        this.runs = runs;
    }

    @Test
    public void test_stringreader_charcounting() {
        SecureRandom random = new SecureRandom();

        byte[] srcArr;
        StringBuffer srcSb = new StringBuffer( 256 );
        String srcStr;
        StringBuffer dstOut = new StringBuffer( 256 );
        //byte[] dstArr;
        String dstStr;

        CharCountingStringReader in;

        try {
            in = new CharCountingStringReader( null );
            Assert.fail( "Exception expected!" );
        } catch (NullPointerException e) {
        }

        in = new CharCountingStringReader( "" );
        Assert.assertNotNull( in );

        Assert.assertFalse( in.markSupported() );
        in.mark( 1 );
        try {
            in.reset();
            Assert.fail( "Exception expected!" );
        } catch (IOException e) {
            Assert.fail( "Exception expected!" );
        } catch (UnsupportedOperationException e) {
        }

        long remaining;
        long consumed;
        char[] tmpBuf = new char[ 16 ];
        int read;
        int mod;

        for ( int r=0; r<runs; ++r) {
            for ( int n=min; n<max; ++n ) {
                srcArr = new byte[ n ];
                random.nextBytes( srcArr );

                srcSb.setLength( 0 );
                for ( int i=0; i<srcArr.length; ++i ) {
                    srcSb.append( (char)(srcArr[ i ] & 255) );
                }
                srcStr = srcSb.toString();

                try {
                    /*
                     * Read.
                     */
                    in = new CharCountingStringReader( srcStr );

                    dstOut.setLength( 0 );

                    remaining = srcStr.length();
                    consumed = 0;
                    read = 0;
                    mod = 2;
                    while ( remaining > 0 && read != -1 ) {
                        switch ( mod ) {
                        case 0:
                            dstOut.append( (char)read );
                            --remaining;
                            ++consumed;
                            Assert.assertEquals( consumed, in.consumed );
                            break;
                        case 1:
                        case 2:
                            dstOut.append( tmpBuf, 0, read );
                            remaining -= read;
                            consumed += read;
                            Assert.assertEquals( consumed, in.consumed );
                            break;
                        }

                        mod = (mod + 1) % 3;

                        switch ( mod ) {
                        case 0:
                            read = in.read();
                            break;
                        case 1:
                            read = in.read( tmpBuf );
                            break;
                        case 2:
                            read = random.nextInt( 15 ) + 1;
                            read = in.read( tmpBuf, 0, read );
                            break;
                        }
                    }

                    Assert.assertEquals( 0, remaining );
                    Assert.assertEquals( n, consumed );
                    Assert.assertEquals( n, in.consumed );
                    Assert.assertEquals( n, in.counter );
                    Assert.assertEquals( in.consumed, in.getConsumed() );
                    Assert.assertEquals( in.counter, in.getCounter() );

                    dstStr = dstOut.toString();
                    Assert.assertEquals( srcStr.length(), dstStr.length() );
                    Assert.assertEquals( srcStr, dstStr );

                    in.close();
                    /*
                     * Skip.
                     */
                    in = new CharCountingStringReader( srcStr );
                    in.setCounter( n );

                    dstOut.setLength( 0 );

                    remaining = srcArr.length;
                    consumed = 0;
                    read = 0;
                    mod = 3;
                    int skipped = 0;
                    while ( remaining > 0 && read != -1 ) {
                        switch ( mod ) {
                        case 0:
                            dstOut.append( (char)read );
                            --remaining;
                            ++consumed;
                            Assert.assertEquals( consumed, in.consumed );
                            break;
                        case 1:
                        case 2:
                            dstOut.append( tmpBuf, 0, read );
                            remaining -= read;
                            consumed += read;
                            Assert.assertEquals( consumed, in.consumed );
                            break;
                        case 3:
                            remaining -= read;
                            consumed += read;
                            skipped += read;
                            Assert.assertEquals( consumed, in.consumed );
                            break;
                        }

                        mod = (mod + 1) % 4;

                        switch ( mod ) {
                        case 0:
                            read = in.read();
                            break;
                        case 1:
                            read = in.read( tmpBuf );
                            break;
                        case 2:
                            read = random.nextInt( 15 ) + 1;
                            read = in.read( tmpBuf, 0, read );
                            break;
                        case 3:
                            read = random.nextInt( 15 ) + 1;
                            read = (int)in.skip( read );
                            break;
                        }
                    }

                    Assert.assertEquals( 0, remaining );
                    Assert.assertEquals( n, consumed );
                    Assert.assertEquals( n, in.consumed );
                    Assert.assertEquals( 2 * n, in.counter );
                    Assert.assertEquals( in.consumed, in.getConsumed() );
                    Assert.assertEquals( in.counter, in.getCounter() );

                    dstStr = dstOut.toString();
                    Assert.assertEquals( srcStr.length(), dstStr.length() + skipped );

                    in.close();
                } catch (IOException e) {
                    Assert.fail( "Exception not expected!" );
                    e.printStackTrace();
                }
            }
        }
    }

}
