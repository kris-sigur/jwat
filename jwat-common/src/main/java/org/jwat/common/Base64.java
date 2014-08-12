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

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * Base64 encoder/decoder implementation based on the specifications in
 * rfc2045/rfc3548.
 *
 * @author nicl
 */
public class Base64 {

    /** Ascii table used to encode. */
    private static String encodeTab = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    /** Table used to decode. */
    public static byte[] decodeTab = new byte[ 256 ];

    /** Populate decode table. */
    static {
        Arrays.fill( decodeTab, (byte)0xff );
        for ( int i=0; i<encodeTab.length(); ++i ) {
            decodeTab[ encodeTab.charAt( i ) ] = (byte)i;
        }
    }

    /**
     * Static class.
     */
    protected Base64() {
    }

    /**
     * Decodes an encoded string.
     * @param in encoded string.
     * @param bStrict if true checks for the correct number of trailing "="
     * @return decoded string or null
     */
    public static String decodeToString(String in, boolean bStrict) {
        if (in == null) {
            return null;
        }
        if (in.length() == 0) {
            return "";
        }

        StringBuffer out = new StringBuffer( 256 );

        boolean b = true;
        int idx = 0;
        char cin;
        int mod = 0;
        int cIdx;
        int cout = 0;

        /*
         * Loop.
         */

        while ( b ) {
            if ( idx < in.length() ) {
                cin = in.charAt( idx );
                if ( cin == '=' ) {
                    b = false;
                }
                else {
                    ++idx;
                    cIdx = decodeTab[ cin ];
                    if ( cIdx != -1 ) {
                        switch ( mod ) {
                            case 0:
                                cout = cIdx << 2;
                                break;
                            case 1:
                                cout |= (cIdx >> 4);
                                out.append( (char)cout );
                                cout = (cIdx << 4) & 255;
                                break;
                            case 2:
                                cout |= (cIdx >> 2);
                                out.append( (char)cout );
                                cout = (cIdx << 6) & 255;
                                break;
                            case 3:
                                cout |= cIdx;
                                out.append( (char)cout );
                                break;
                        }
                        mod = ( mod + 1 ) % 4;
                    }
                    else {
                        return null;
                    }
                }
            }
            else {
                b = false;
            }
        }

        /*
         * Padding.
         */

        if ( mod == 1 ) {
            // In state 1 only 6 bits of the next 24 bit has been decoded.
            return null;
        }
        if ( bStrict ) {
            while ( mod != 0 && idx < in.length() && in.charAt( idx ) == '=' ) {
                ++idx;
                mod = ( mod + 1 ) % 4;
            }
            if (mod != 0 || idx < in.length() ) {
                return null;
            }
        }
        return out.toString();
    }

    /**
     * Decodes an encoded string.
     * @param in encoded string.
     * @param bStrict if true checks for the correct number of trailing "="
     * @return decoded byte array or null
     */
    public static byte[] decodeToArray(String in, boolean bStrict) {
        if (in == null) {
            return null;
        }
        if (in.length() == 0) {
            return new byte[0];
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        boolean b = true;
        int idx = 0;
        char cin;
        int mod = 0;
        int cIdx;
        int cout = 0;

        /*
         * Loop.
         */

        while ( b ) {
            if ( idx < in.length() ) {
                cin = in.charAt( idx );
                if ( cin == '=' ) {
                    b = false;
                }
                else {
                    ++idx;
                    cIdx = decodeTab[ cin ];
                    if ( cIdx != -1 ) {
                        switch ( mod ) {
                            case 0:
                                cout = cIdx << 2;
                                break;
                            case 1:
                                cout |= (cIdx >> 4);
                                out.write( cout );
                                cout = (cIdx << 4) & 255;
                                break;
                            case 2:
                                cout |= (cIdx >> 2);
                                out.write( cout );
                                cout = (cIdx << 6) & 255;
                                break;
                            case 3:
                                cout |= cIdx;
                                out.write( cout );
                                break;
                        }
                        mod = ( mod + 1 ) % 4;
                    }
                    else {
                        return null;
                    }
                }
            }
            else {
                b = false;
            }
        }

        /*
         * Padding.
         */

        if ( mod == 1 ) {
            // In state 1 only 6 bits of the next 24 bit has been decoded.
            return null;
        }
        if ( bStrict ) {
            while ( mod != 0 && idx < in.length() && in.charAt( idx ) == '=' ) {
                ++idx;
                mod = ( mod + 1 ) % 4;
            }
            if (mod != 0 || idx < in.length() ) {
                return null;
            }
        }
        return out.toByteArray();
    }

    /**
     * Encodes a string.
     * @param in unencoded text string.
     * @return encoded string or null
     */
    public static String encodeString(String in) {
        if (in == null) {
            return null;
        }
        if (in.length() == 0) {
            return "";
        }

        StringBuffer out = new StringBuffer( 256 );

        boolean b = true;
        int idx = 0;
        char cin;
        int cout = 0;
        int mod = 0;

        /*
         * Loop.
         */

        while ( b ) {
            if ( idx < in.length() ) {
                cin = in.charAt( idx++ );
                if ( cin < 256 ) {
                    switch ( mod ) {
                    case 0:
                        cout = ( cin >> 2 ) & 63;
                        out.append( encodeTab.charAt( cout ) );
                        cout = ( cin << 4 ) & 63;
                        break;
                    case 1:
                        cout |= ( cin >> 4 ) & 63;
                        out.append( encodeTab.charAt( cout ) );
                        cout = ( cin << 2 ) & 63;
                        break;
                    case 2:
                        cout |= ( cin >> 6 ) & 63;
                        out.append( encodeTab.charAt( cout ) );
                        cout = cin & 63;
                        out.append( encodeTab.charAt( cout ) );
                        break;
                    }
                    mod = ( mod + 1 ) % 3;
                }
                else {
                    return null;
                }
            }
            else {
                b = false;
            }
        }

        /*
         * Padding.
         */

        switch( mod ) {
            case 0:
                break;
            case 1:
                out.append( encodeTab.charAt( cout ) );
                out.append( "==" );
                break;
            case 2:
                out.append( encodeTab.charAt( cout ) );
                out.append( "=" );
                break;
        }

        return out.toString();
    }

    /**
     * Encodes a byte array.
     * @param in byte array.
     * @return encoded string or null
     */
    public static String encodeArray(byte[] in) {
        if (in == null) {
            return null;
        }
        if (in.length == 0) {
            return "";
        }

        StringBuffer out = new StringBuffer( 256 );

        boolean b = true;
        int idx = 0;
        int cin;
        int cout = 0;
        int mod = 0;

        /*
         * Loop.
         */

        while ( b ) {
            if ( idx < in.length ) {
                cin = in[ idx++ ] & 255;
                switch ( mod ) {
                    case 0:
                        cout = ( cin >> 2 ) & 63;
                        out.append( encodeTab.charAt( cout ) );
                        cout = ( cin << 4 ) & 63;
                        break;
                    case 1:
                        cout |= ( cin >> 4 ) & 63;
                        out.append( encodeTab.charAt( cout ) );
                        cout = ( cin << 2 ) & 63;
                        break;
                    case 2:
                        cout |= ( cin >> 6 ) & 63;
                        out.append( encodeTab.charAt( cout ) );
                        cout = cin & 63;
                        out.append( encodeTab.charAt( cout ) );
                        break;
                }
                mod = ( mod + 1 ) % 3;
            }
            else {
                b = false;
            }
        }

        /*
         * Padding.
         */

        switch( mod ) {
            case 0:
                break;
            case 1:
                out.append( encodeTab.charAt( cout ) );
                out.append( "==" );
                break;
            case 2:
                out.append( encodeTab.charAt( cout ) );
                out.append( "=" );
                break;
        }

        return out.toString();
    }

}
