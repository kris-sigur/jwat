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

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestUri {

    private boolean debug = false;

    @Test
    public void test_uri_absolute() {
        /*
         * Valid absolute URIs.
         */
        Object[][] valid_uri_cases = {
                {"scheme://userinfo@host:42/path?query#fragment", true, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", "42", "/path", "query", "fragment"},
                    new Object[] {"scheme", "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", 42, "/path", "query", "fragment"}
                },
                {"SCHEME://USERINFO@HOST:42/PATH?QUERY#FRAGMENT", true, false,
                    new Object[] {"//USERINFO@HOST:42/PATH", "//USERINFO@HOST:42/PATH?QUERY", "USERINFO@HOST:42", "USERINFO", "HOST", "42", "/PATH", "QUERY", "FRAGMENT"},
                    new Object[] {"SCHEME", "//USERINFO@HOST:42/PATH?QUERY", "USERINFO@HOST:42", "USERINFO", "HOST", 42, "/PATH", "QUERY", "FRAGMENT"}
                },
                // Fragment
                {"scheme://userinfo@host:42/path?query#", true, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", "42", "/path", "query", ""},
                    new Object[] {"scheme", "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", 42, "/path", "query", ""}
                },
                {"scheme://userinfo@host:42/path?query", true, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", "42", "/path", "query", null},
                    new Object[] {"scheme", "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", 42, "/path", "query", null}
                },
                // Query
                {"scheme://userinfo@host:42/path?#fragment", true, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path?", "userinfo@host:42", "userinfo", "host", "42", "/path", "", "fragment"},
                    new Object[] {"scheme", "//userinfo@host:42/path?", "userinfo@host:42", "userinfo", "host", 42, "/path", "", "fragment"}
                },
                {"scheme://userinfo@host:42/path#fragment", true, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path", "userinfo@host:42", "userinfo", "host", "42", "/path", null, "fragment"},
                    new Object[] {"scheme", "//userinfo@host:42/path", "userinfo@host:42", "userinfo", "host", 42, "/path", null, "fragment"}
                },
                // Fragment/Query
                {"scheme://userinfo@host:42/path", true, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path", "userinfo@host:42", "userinfo", "host", "42", "/path", null, null},
                    new Object[] {"scheme", "//userinfo@host:42/path", "userinfo@host:42", "userinfo", "host", 42, "/path", null, null}
                },
                // Path
                {"scheme://userinfo@host:42/?query#fragment", true, false,
                    new Object[] {"//userinfo@host:42/", "//userinfo@host:42/?query", "userinfo@host:42", "userinfo", "host", "42", "/", "query", "fragment"},
                    new Object[] {"scheme", "//userinfo@host:42/?query", "userinfo@host:42", "userinfo", "host", 42, "/", "query", "fragment"}
                },
                {"scheme://userinfo@host:42?query#fragment", true, false,
                    new Object[] {"//userinfo@host:42", "//userinfo@host:42?query", "userinfo@host:42", "userinfo", "host", "42", "", "query", "fragment"},
                    new Object[] {"scheme", "//userinfo@host:42?query", "userinfo@host:42", "userinfo", "host", 42, "", "query", "fragment"}
                },
                // Hierpart
                {"scheme:/path?query#fragment", true, false,
                    new Object[] {"/path", "/path?query", null, null, null, null, "/path", "query", "fragment"},
                    new Object[] {"scheme", "/path?query", null, null, null, -1, "/path", "query", "fragment"}
                },
                {"scheme:/path?query", true, false,
                    new Object[] {"/path", "/path?query", null, null, null, null, "/path", "query", null},
                    new Object[] {"scheme", "/path?query", null, null, null, -1, "/path", "query", null}
                },
                {"scheme:/path#fragment", true, false,
                    new Object[] {"/path", "/path", null, null, null, null, "/path", null, "fragment"},
                    new Object[] {"scheme", "/path", null, null, null, -1, "/path", null, "fragment"}
                },
                {"scheme:/path", true, false,
                    new Object[] {"/path", "/path", null, null, null, null, "/path", null, null},
                    new Object[] {"scheme", "/path", null, null, null, -1, "/path", null, null}
                },
                // Opaque
                {"scheme:path?query#fragment", true, true,
                    new Object[] {"path", "path?query", null, null, null, null, "path", "query", "fragment"},
                    new Object[] {"scheme", "path?query", null, null, null, -1, "path", "query", "fragment"}
                },
                {"scheme:path?query", true, true,
                    new Object[] {"path", "path?query", null, null, null, null, "path", "query", null},
                    new Object[] {"scheme", "path?query", null, null, null, -1, "path", "query", null}
                },
                {"scheme:path#fragment", true, true,
                    new Object[] {"path", "path", null, null, null, null, "path", null, "fragment"},
                    new Object[] {"scheme", "path", null, null, null, -1, "path", null, "fragment"}
                },
                {"scheme:path", true, true,
                    new Object[] {"path", "path", null, null, null, null, "path", null, null},
                    new Object[] {"scheme", "path", null, null, null, -1, "path", null, null}
                },
                {"scheme:", true, false,
                    new Object[] {"", "", null, null, null, null, "", null, null},
                    new Object[] {"scheme", "", null, null, null, -1, "", null, null}
                },
                // Userinfo
                {"scheme://@host:42/path", true, false,
                    new Object[] {"//@host:42/path", "//@host:42/path", "@host:42", "", "host", "42", "/path", null, null},
                    new Object[] {"scheme", "//@host:42/path", "@host:42", "", "host", 42, "/path", null, null}
                },
                {"scheme://host:42/path", true, false,
                    new Object[] {"//host:42/path", "//host:42/path", "host:42", null, "host", "42", "/path", null, null},
                    new Object[] {"scheme", "//host:42/path", "host:42", null, "host", 42, "/path", null, null}
                },
                // ipv6
                {"scheme://userinfo@[2001:db8::7]:42/path?query#fragment", true, false,
                    new Object[] {"//userinfo@[2001:db8::7]:42/path", "//userinfo@[2001:db8::7]:42/path?query", "userinfo@[2001:db8::7]:42", "userinfo", "[2001:db8::7]", "42", "/path", "query", "fragment"},
                    new Object[] {"scheme", "//userinfo@[2001:db8::7]:42/path?query", "userinfo@[2001:db8::7]:42", "userinfo", "[2001:db8::7]", 42, "/path", "query", "fragment"}
                },
                {"scheme://userinfo@[2001:db8::7]/path?query#fragment", true, false,
                    new Object[] {"//userinfo@[2001:db8::7]/path", "//userinfo@[2001:db8::7]/path?query", "userinfo@[2001:db8::7]", "userinfo", "[2001:db8::7]", null, "/path", "query", "fragment"},
                    new Object[] {"scheme", "//userinfo@[2001:db8::7]/path?query", "userinfo@[2001:db8::7]", "userinfo", "[2001:db8::7]", -1, "/path", "query", "fragment"}
                },
                // host
                {"scheme://", true, false,
                    new Object[] {"//", "//", "", null, "", null, "", null, null},
                    new Object[] {"scheme", "//", "", null, "", -1, "", null, null}
                },
                // port, should decoded have empty :
                {"scheme-port://userinfo@host:/path?query#fragment", true, false,
                    new Object[] {"//userinfo@host:/path", "//userinfo@host:/path?query", "userinfo@host:", "userinfo", "host", "", "/path", "query", "fragment"},
                    new Object[] {"scheme-port", "//userinfo@host:/path?query", "userinfo@host:", "userinfo", "host", -1, "/path", "query", "fragment"}
                },
                {"scheme-port://userinfo@[2001:db8::7]:/path?query#fragment", true, false,
                    new Object[] {"//userinfo@[2001:db8::7]:/path", "//userinfo@[2001:db8::7]:/path?query", "userinfo@[2001:db8::7]:", "userinfo", "[2001:db8::7]", "", "/path", "query", "fragment"},
                    new Object[] {"scheme-port", "//userinfo@[2001:db8::7]:/path?query", "userinfo@[2001:db8::7]:", "userinfo", "[2001:db8::7]", -1, "/path", "query", "fragment"}
                },
                /*
                 * Relative.
                 */
                {"//userinfo@host:42/path?query#fragment", false, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", "42", "/path", "query", "fragment"},
                    new Object[] {null, "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", 42, "/path", "query", "fragment"}
                },
                // Fragment
                {"//userinfo@host:42/path?query#", false, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", "42", "/path", "query", ""},
                    new Object[] {null, "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", 42, "/path", "query", ""}
                },
                {"//userinfo@host:42/path?query", false, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", "42", "/path", "query", null},
                    new Object[] {null, "//userinfo@host:42/path?query", "userinfo@host:42", "userinfo", "host", 42, "/path", "query", null}
                },
                // Query
                {"//userinfo@host:42/path?#fragment", false, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path?", "userinfo@host:42", "userinfo", "host", "42", "/path", "", "fragment"},
                    new Object[] {null, "//userinfo@host:42/path?", "userinfo@host:42", "userinfo", "host", 42, "/path", "", "fragment"}
                },
                {"//userinfo@host:42/path#fragment", false, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path", "userinfo@host:42", "userinfo", "host", "42", "/path", null, "fragment"},
                    new Object[] {null, "//userinfo@host:42/path", "userinfo@host:42", "userinfo", "host", 42, "/path", null, "fragment"}
                },
                // Fragment/Query
                {"//userinfo@host:42/path", false, false,
                    new Object[] {"//userinfo@host:42/path", "//userinfo@host:42/path", "userinfo@host:42", "userinfo", "host", "42", "/path", null, null},
                    new Object[] {null, "//userinfo@host:42/path", "userinfo@host:42", "userinfo", "host", 42, "/path", null, null}
                },
                // Path
                {"//userinfo@host:42/?query#fragment", false, false,
                    new Object[] {"//userinfo@host:42/", "//userinfo@host:42/?query", "userinfo@host:42", "userinfo", "host", "42", "/", "query", "fragment"},
                    new Object[] {null, "//userinfo@host:42/?query", "userinfo@host:42", "userinfo", "host", 42, "/", "query", "fragment"}
                },
                {"//userinfo@host:42?query#fragment", false, false,
                    new Object[] {"//userinfo@host:42", "//userinfo@host:42?query", "userinfo@host:42", "userinfo", "host", "42", "", "query", "fragment"},
                    new Object[] {null, "//userinfo@host:42?query", "userinfo@host:42", "userinfo", "host", 42, "", "query", "fragment"}
                },
                // Hierpart
                {"/path?query#fragment", false, false,
                    new Object[] {"/path", "/path?query", null, null, null, null, "/path", "query", "fragment"},
                    new Object[] {null, "/path?query", null, null, null, -1, "/path", "query", "fragment"}
                },
                {"/path?query", false, false,
                    new Object[] {"/path", "/path?query", null, null, null, null, "/path", "query", null},
                    new Object[] {null, "/path?query", null, null, null, -1, "/path", "query", null}
                },
                {"/path#fragment", false, false,
                    new Object[] {"/path", "/path", null, null, null, null, "/path", null, "fragment"},
                    new Object[] {null, "/path", null, null, null, -1, "/path", null, "fragment"}
                },
                {"/path", false, false,
                    new Object[] {"/path", "/path", null, null, null, null, "/path", null, null},
                    new Object[] {null, "/path", null, null, null, -1, "/path", null, null}
                },
                // Opaque
                {"path?query#fragment", false, false,
                    new Object[] {"path", "path?query", null, null, null, null, "path", "query", "fragment"},
                    new Object[] {null, "path?query", null, null, null, -1, "path", "query", "fragment"}
                },
                {"path?query", false, false,
                    new Object[] {"path", "path?query", null, null, null, null, "path", "query", null},
                    new Object[] {null, "path?query", null, null, null, -1, "path", "query", null}
                },
                {"path#fragment", false, false,
                    new Object[] {"path", "path", null, null, null, null, "path", null, "fragment"},
                    new Object[] {null, "path", null, null, null, -1, "path", null, "fragment"}
                },
                {"path", false, false,
                    new Object[] {"path", "path", null, null, null, null, "path", null, null},
                    new Object[] {null, "path", null, null, null, -1, "path", null, null}
                },
                {"", false, false,
                    new Object[] {"", "", null, null, null, null, "", null, null},
                    new Object[] {null, "", null, null, null, -1, "", null, null}
                },
                // Userinfo
                {"//@host:42/path", false, false,
                    new Object[] {"//@host:42/path", "//@host:42/path", "@host:42", "", "host", "42", "/path", null, null},
                    new Object[] {null, "//@host:42/path", "@host:42", "", "host", 42, "/path", null, null}
                },
                {"//host:42/path", false, false,
                    new Object[] {"//host:42/path", "//host:42/path", "host:42", null, "host", "42", "/path", null, null},
                    new Object[] {null, "//host:42/path", "host:42", null, "host", 42, "/path", null, null}
                },
                // ipv6
                {"//userinfo@[2001:db8::7]:42/path?query#fragment", false, false,
                    new Object[] {"//userinfo@[2001:db8::7]:42/path", "//userinfo@[2001:db8::7]:42/path?query", "userinfo@[2001:db8::7]:42", "userinfo", "[2001:db8::7]", "42", "/path", "query", "fragment"},
                    new Object[] {null, "//userinfo@[2001:db8::7]:42/path?query", "userinfo@[2001:db8::7]:42", "userinfo", "[2001:db8::7]", 42, "/path", "query", "fragment"}
                },
                {"//userinfo@[2001:db8::7]/path?query#fragment", false, false,
                    new Object[] {"//userinfo@[2001:db8::7]/path", "//userinfo@[2001:db8::7]/path?query", "userinfo@[2001:db8::7]", "userinfo", "[2001:db8::7]", null, "/path", "query", "fragment"},
                    new Object[] {null, "//userinfo@[2001:db8::7]/path?query", "userinfo@[2001:db8::7]", "userinfo", "[2001:db8::7]", -1, "/path", "query", "fragment"}
                },
                // host
                {"//", false, false,
                    new Object[] {"//", "//", "", null, "", null, "", null, null},
                    new Object[] {null, "//", "", null, "", -1, "", null, null}
                },
                /*
                // port, should decoded have empty :
                {"//userinfo@host:/path?query#fragment", false, false,
                    new Object[] {"//userinfo@host:/path", "//userinfo@host:/path?query", "userinfo@host:", "userinfo", "host", "", "/path", "query", "fragment"},
                    new Object[] {null, "//userinfo@host:/path?query", "userinfo@host:", "userinfo", "host", -1, "/path", "query", "fragment"}
                },
                {"//userinfo@[2001:db8::7]:/path?query#fragment", false, false,
                    new Object[] {"//userinfo@[2001:db8::7]:/path", "//userinfo@[2001:db8::7]:/path?query", "userinfo@[2001:db8::7]:", "userinfo", "[2001:db8::7]", "", "/path", "query", "fragment"},
                    new Object[] {null, "//userinfo@[2001:db8::7]:/path?query", "userinfo@[2001:db8::7]:", "userinfo", "[2001:db8::7]", -1, "/path", "query", "fragment"}
                },
                */
        };
        Uri[] uris = new Uri[valid_uri_cases.length];
        URI jdkuri = null;
        Uri uri1 = null;
        Uri uri2 = null;
        for (int i=0; i<valid_uri_cases.length; ++i) {
            String str = (String)valid_uri_cases[i][0];
            boolean bAbsolute = (Boolean)valid_uri_cases[i][1];
            boolean bOpaque = (Boolean)valid_uri_cases[i][2];
            Object[] expectedraw = (Object[])valid_uri_cases[i][3];
            Object[] expected = (Object[])valid_uri_cases[i][4];
            jdkuri = null;
            uri1 = null;
            uri2 = null;
            try {
                jdkuri = URI.create(str);
                if (jdkuri != null) {
                    // debug
                    if (debug) {
                        System.out.println("      Scheme: " + jdkuri.getScheme());
                        System.out.println("  SchemeSpec: " + jdkuri.getSchemeSpecificPart());
                        System.out.println("   Authority: " + jdkuri.getAuthority());
                        System.out.println("    Userinfo: " + jdkuri.getUserInfo());
                        System.out.println("        Host: " + jdkuri.getHost());
                        System.out.println("        Port: " + jdkuri.getPort());
                        System.out.println("        Path: " + jdkuri.getPath());
                        System.out.println("       Query: " + jdkuri.getQuery());
                        System.out.println("    Fragment: " + jdkuri.getFragment());
                    }
                }
                // debug
                if (debug) {
                    System.out.println(jdkuri);
                }
            } catch (IllegalArgumentException e) {
                // debug
                if (debug) {
                    System.out.println("#### " + str);
                }
            }
            try {
                uri1 = Uri.create(str);
                Assert.assertEquals(UriProfile.RFC3986, uri1.uriProfile);
                check_expected_uri(uri1, jdkuri, bAbsolute, bOpaque, expectedraw, expected);
                // debug
                if (debug) {
                    System.out.println(uri1.toString());
                }
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception!");
            }
            try {
                uri2 = new Uri(str);
                Assert.assertEquals(UriProfile.RFC3986, uri2.uriProfile);
                check_expected_uri(uri2, jdkuri, bAbsolute, bOpaque, expectedraw, expected);
                // debug
                if (debug) {
                    System.out.println(uri1.toString());
                }
            } catch (URISyntaxException e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception!");
            }
            // debug
            if (debug) {
                System.out.println(str);
            }
            uris[i] = uri1;
            Assert.assertEquals(str, uri1.toString());
            Assert.assertEquals(str, uri2.toString());
            Assert.assertEquals(uri1.toString(), uri2.toString());
            Assert.assertTrue(uri1.equals(uri1));
            Assert.assertTrue(uri2.equals(uri2));
            Assert.assertTrue(uri1.equals(uri2));
            Assert.assertTrue(uri2.equals(uri1));
            Assert.assertEquals(uri1.hashCode(), uri2.hashCode());
            Assert.assertEquals(0, uri1.compareTo(uri1));
            Assert.assertEquals(0, uri2.compareTo(uri2));
            Assert.assertEquals(0, uri1.compareTo(uri2));
            Assert.assertEquals(0, uri2.compareTo(uri1));
            Assert.assertFalse(uri1.equals(null));
            Assert.assertFalse(uri2.equals(null));
            Assert.assertFalse(uri1.equals(jdkuri));
            Assert.assertFalse(uri2.equals(jdkuri));
            Assert.assertEquals(1, uri1.compareTo(null));
            Assert.assertEquals(1, uri2.compareTo(null));
            /*
             * Relative disabled.
             */
            if (bAbsolute) {
                try {
                    uri1 = Uri.create(str, UriProfile.RFC3986_ABS_16BIT);
                    Assert.assertEquals(UriProfile.RFC3986_ABS_16BIT, uri1.uriProfile);
                    check_expected_uri(uri1, jdkuri, bAbsolute, bOpaque, expectedraw, expected);
                    // debug
                    //System.out.println(uri1.toString());
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                    Assert.fail("Unexpected exception!");
                }
                try {
                    uri2 = new Uri(str, UriProfile.RFC3986_ABS_16BIT);
                    Assert.assertEquals(UriProfile.RFC3986_ABS_16BIT, uri2.uriProfile);
                    check_expected_uri(uri2, jdkuri, bAbsolute, bOpaque, expectedraw, expected);
                    // debug
                    //System.out.println(uri1.toString());
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    Assert.fail("Unexpected exception!");
                }
                Assert.assertEquals(str, uri1.toString());
                Assert.assertEquals(str, uri2.toString());
                Assert.assertEquals(uri1.toString(), uri2.toString());
                Assert.assertTrue(uri1.equals(uri1));
                Assert.assertTrue(uri2.equals(uri2));
                Assert.assertTrue(uri1.equals(uri2));
                Assert.assertTrue(uri2.equals(uri1));
                Assert.assertEquals(uri1.hashCode(), uri2.hashCode());
                Assert.assertEquals(0, uri1.compareTo(uri1));
                Assert.assertEquals(0, uri2.compareTo(uri2));
                Assert.assertEquals(0, uri1.compareTo(uri2));
                Assert.assertEquals(0, uri2.compareTo(uri1));
                Assert.assertFalse(uri1.equals(null));
                Assert.assertFalse(uri2.equals(null));
                Assert.assertFalse(uri1.equals(jdkuri));
                Assert.assertFalse(uri2.equals(jdkuri));
                Assert.assertEquals(1, uri1.compareTo(null));
                Assert.assertEquals(1, uri2.compareTo(null));
            } else {
                try {
                    Uri.create(str, UriProfile.RFC3986_ABS_16BIT);
                    Assert.fail("Exception expected!");
                } catch (IllegalArgumentException e) {
                }
                try {
                    new Uri(str, UriProfile.RFC3986_ABS_16BIT);
                    Assert.fail("Exception expected!");
                } catch (URISyntaxException e) {
                }
            }
        }
        Arrays.sort(uris);
        /*
        // debug
        for (int i=0; i<uris.length; ++i) {
            //System.out.println("> " + uris[i].toString());
        }
        */
        for (int i=0; i<uris.length; ++i) {
            // debug
            //System.out.println(">> " + uris[i].toString());
            for (int j=0; j<uris.length; ++j) {
                if (i < j) {
                    Assert.assertFalse(uris[i].equals(uris[j]));
                    Assert.assertThat(uris[i].compareTo(uris[j]), is(lessThan(0)));
                    Assert.assertThat(uris[j].compareTo(uris[i]), is(greaterThan(0)));
                } else if (i > j) {
                    Assert.assertFalse(uris[i].equals(uris[j]));
                    Assert.assertThat(uris[i].compareTo(uris[j]), is(greaterThan(0)));
                    Assert.assertThat(uris[j].compareTo(uris[i]), is(lessThan(0)));
                } else {
                    Assert.assertTrue(uris[i].equals(uris[j]));
                    Assert.assertEquals(0, uris[i].compareTo(uris[j]));
                }
            }
        }
        String[] invalid_uri_cases = {
                "\u1234a:",
                "4a:",
                "éy:",
                "aä:",
                ":",
                "://userinfo@host:42/path?query#fragment",
                "scheme://userinfo@[2001:db8::7/path",
                "scheme://[2001:db8::7/path",
                "scheme://userinfo@[2001:db8::7]42/path?query#fragment",
                "scheme://userinfo@[2001:db8::7]:-1/path?query#fragment",
                "scheme://userinfo@[2001:db8::7]:65536/path?query#fragment",
                "scheme://userinfo@[2001:db8::7]:port/path?query#fragment",
                "scheme://userinfo@host:-1/path?query#fragment",
                "scheme://userinfo@host:65536/path?query#fragment",
                "scheme://userinfo@host:port/path?query#fragment",
        };
        for (int i=0; i<invalid_uri_cases.length; ++i) {
            String str = invalid_uri_cases[i];
            try {
                uri1 = Uri.create(str);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }
            try {
                uri2 = new Uri(str);
                Assert.fail("Exception expected!");
            } catch (URISyntaxException e) {
            }

        }
        uri1 = new Uri();
        Assert.assertNotNull(uri1);
        uri2 = new Uri();
        Assert.assertNotNull(uri2);
    }

    @Test
    public void test_uri() {
        String[] invalidUris = {
                "filedesc://horsy|\\\\hello",
        };
        for (int i=0; i<invalidUris.length; ++i) {
            try {
                new Uri(invalidUris[i]);
                Assert.fail("Exception expected!");
            } catch (URISyntaxException e) {
            }
            try {
                new URI(invalidUris[i]);
                Assert.fail("Exception expected!");
            } catch (URISyntaxException e) {
            }
        }
        Object[][] valid_uri_cases = {
                // rfc3986
                {"ftp://ftp.is.co.za/rfc/rfc1808.txt", true, false},
                {"http://www.ietf.org/rfc/rfc2396.txt", true, false},
                {"ldap://[2001:db8::7]/c=GB?objectClass?one", true, false},
                {"mailto:John.Doe@example.com", true, true},
                {"news:comp.infosystems.www.servers.unix", true, true},
                {"tel:+1-816-555-1212", true, true},
                {"telnet://192.0.2.16:80/", true, false},
                {"urn:oasis:names:specification:docbook:dtd:xml:4.1.2", true, true},
                {"foo://example.com:8042/over/there?name=ferret#nose", true, false},
                {"urn:example:animal:ferret:nose", true, true},
                // misc
                {"http://", true, false},
                {"last-modified:", true, false},
                {"http://www.a1ie.com", true, false},
                {"https://sbforge.org/jenkins/view/Active%20jobs/", true, false},
                {"http://code.google.com/p/acre/source/browse/trunk/webapp/WEB-INF/?r=452#WEB-INF%2Flib-src", true, false},
                {"urn:uuid:173a3db1-9ba4-496e-9eaf-6e550a62fd88", true, true},
                {"https://services.brics.dk/java/courseadmin/login/login?returnURL=https%3A%2F%2Fservices.brics.dk%2Fjava%2Fcourseadmin%2FdPersp%2Fwebboard%2F", true, false},
                {"http://healthtrawl.com/store/insurance/rate/quotes/automobile_insurance_rate.htm?yt=qs%3d06oENya4ZG1YS6vOLJwpLiFdjG9wRAvs4Nk7NsEKCkSCHmnJLHtuz7tt7ykteKWgNq1iJVLZ6mGhgFhh7h6-hm0nsNEjrJjFhg5frIpv1on3rp_mUtDZSO5BWZTURTx32nsYdBndmO0eLnsyL6de67bGMA7W6jpj3FUzi5Jybt0SrblzNmsi0s-Aw00FrVmCq0SiUKQ6JiXsGQ4-uoQMimRuU.%2cYT0xO0w9QXV0b21vYmlsZSBJbnN1cmFuY2UgUmF0ZTtSPTg7Uz1UOnIjLSNLWjtrPTQ.&slt=1&slr=8&lpt=2", true, false},
                // TODO utf-8
                //{"http://news.ainfekka.com/?feed=rss2&tag=%d8%a8%d9%88%d8%b9%d9%8a%d8%b4%d8%a9-%d8%b3%d8%a7%d8%b9%d8%af", true, false},
                {"./this:that", false, false},
        };
        URI jdkuri;
        Uri uri;
        for (int i=0; i<valid_uri_cases.length; ++i) {
            String str = (String)valid_uri_cases[i][0];
            boolean bAbsolute = (Boolean)valid_uri_cases[i][1];
            boolean bOpaque = (Boolean)valid_uri_cases[i][2];
            Object[] expectedraw = null;
            Object[] expected = null;
            jdkuri = null;
            try {
                try {
                    jdkuri = new URI(str);
                    // debug
                    if (debug) {
                        System.out.println(jdkuri);
                        System.out.println("      Scheme: " + jdkuri.getScheme());
                        System.out.println("  SchemeSpec: " + jdkuri.getSchemeSpecificPart());
                        System.out.println("   Authority: " + jdkuri.getAuthority());
                        System.out.println("    Userinfo: " + jdkuri.getUserInfo());
                        System.out.println("        Host: " + jdkuri.getHost());
                        System.out.println("        Port: " + jdkuri.getPort());
                        System.out.println("        Path: " + jdkuri.getPath());
                        System.out.println("       Query: " + jdkuri.getQuery());
                        System.out.println("    Fragment: " + jdkuri.getFragment());
                    }
                } catch (URISyntaxException e) {
                    // debug
                    if (debug) {
                        e.printStackTrace();
                    }
                }
                uri = Uri.create(str);
                Assert.assertEquals(UriProfile.RFC3986, uri.uriProfile);
                // debug
                if (debug) {
                    System.out.println("--------");
                }
                if (uri != null) {
                    // debug
                    if (debug) {
                        System.out.println("    Scheme: " + uri.scheme);
                        System.out.println("SchemeSpec: " + uri.schemeSpecificPart);
                        System.out.println(" Authority: " + uri.authority);
                        System.out.println("  Userinfo: " + uri.userinfo);
                        System.out.println("      Host: " + uri.host);
                        System.out.println("      Port: " + uri.port);
                        System.out.println("      Path: " + uri.path);
                        System.out.println("     Query: " + uri.query);
                        System.out.println("  Fragment: " + uri.fragment);
                        System.out.println(uri);
                    }
                }
                check_expected_uri(uri, jdkuri, bAbsolute, bOpaque, expectedraw, expected);
                // debug
                if (debug) {
                    System.out.println("--------");
                }
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception!");
            }
        }
        String[] unicodepct_uris = {
                // %uXXXX
                "http://www.a1ie.com/news/index_0_7.html?tags=%u4E8B%u4EF6%u8425%u9500#hello_world",
        };
        for (int i=0; i<unicodepct_uris.length; ++i) {
            String str = unicodepct_uris[i];
            try {
                uri = Uri.create(str, UriProfile.RFC3986);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }
            try {
                uri = new Uri(str, UriProfile.RFC3986);
                Assert.fail("Exception expected!");
            } catch (URISyntaxException e) {
            }
            try {
                uri = Uri.create(str, UriProfile.RFC3986_ABS_16BIT);
                Assert.assertEquals(UriProfile.RFC3986_ABS_16BIT, uri.uriProfile);
                uri = new Uri(str, UriProfile.RFC3986_ABS_16BIT);
                Assert.assertEquals(UriProfile.RFC3986_ABS_16BIT, uri.uriProfile);
            } catch (URISyntaxException e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception!");
            }
            try {
                uri = Uri.create(str, UriProfile.RFC3986_ABS_16BIT_LAX);
                Assert.assertEquals(UriProfile.RFC3986_ABS_16BIT_LAX, uri.uriProfile);
                uri = new Uri(str, UriProfile.RFC3986_ABS_16BIT_LAX);
                Assert.assertEquals(UriProfile.RFC3986_ABS_16BIT_LAX, uri.uriProfile);
            } catch (URISyntaxException e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception!");
            }
        }
        String[] relaxed_uris = {
                // [
                "http://www.kb.dk/GUIDResolver/Uid:dk:kb:doms:2006-09/99999/1/S/erez?erez=online_master_arkiv/webbilleder/NB/Bevaring/billeder_til_CMS/bibsal:002520-:002520small[1]-1.tif&tmp=1kolonneH&top=0.06395349&bottom=0.93604651&width=200&height=150",
                "http://www.kb.dk/erez4/erez?height=150&bottom=0.93604651&tmp=1kolonneH&width=200&top=0.06395349&src=online_master_arkiv/webbilleder/NB/Bevaring/billeder_til_CMS/bibsal:002520-:002520small[1]-1.tif",
                // ^
                "http://www.advfn.com/p.php?pid=staticchart&s=A^CXZ&p=8&t=37&vol=1",
        };
        for (int i=0; i<relaxed_uris.length; ++i) {
            String str = relaxed_uris[i];
            try {
                uri = Uri.create(str, UriProfile.RFC3986);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }
            try {
                uri = new Uri(str, UriProfile.RFC3986);
                Assert.fail("Exception expected!");
            } catch (URISyntaxException e) {
            }
            try {
                uri = Uri.create(str, UriProfile.RFC3986_ABS_16BIT);
                Assert.fail("Exception expected!");
            } catch (IllegalArgumentException e) {
            }
            try {
                uri = new Uri(str, UriProfile.RFC3986_ABS_16BIT);
                Assert.fail("Exception expected!");
            } catch (URISyntaxException e) {
            }
            try {
                uri = Uri.create(str, UriProfile.RFC3986_ABS_16BIT_LAX);
                Assert.assertEquals(UriProfile.RFC3986_ABS_16BIT_LAX, uri.uriProfile);
                uri = new Uri(str, UriProfile.RFC3986_ABS_16BIT_LAX);
                Assert.assertEquals(UriProfile.RFC3986_ABS_16BIT_LAX, uri.uriProfile);
            } catch (URISyntaxException e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception!");
            }
        }
    }

    /*
    // Useful for mass dumping and testing of all processed URIs.
    try {
        RandomAccessFile raf = new RandomAccessFile("uris.txt", "rw");
        raf.seek(raf.length());
        raf.write(str.getBytes("ISO8859-1"));
        raf.write("\r\n".getBytes());
        raf.close();
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }
    */

    /*
    public void test_uris_list() {
        RandomAccessFile raf;
        String line;
        int valid = 0;
        int invalid = 0;
        try {
            raf = new RandomAccessFile("uris.txt", "r");
            while ((line = raf.readLine()) != null) {
                try {
                    new Uri(line);
                    ++valid;
                } catch (URISyntaxException e) {
                    //System.out.println(line);
                    ++invalid;
                }
            }
            raf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception!");
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception!");
        }
        System.out.println("valid: " + valid);
        System.out.println("invalid: " + invalid);
    }
    */

    public void check_expected_uri(Uri uri, URI jdkuri, boolean bAbsolute, boolean bOpaque, Object[] expectedraw, Object[] expected) {
        // booleans
        Assert.assertEquals(bAbsolute, uri.bAbsolute);
        Assert.assertEquals(bOpaque, uri.bOpaque);
        Assert.assertEquals(uri.bAbsolute, uri.isAbsolute());
        Assert.assertEquals(uri.bOpaque, uri.isOpaque());
        // raw data
        if (expectedraw != null) {
            Assert.assertEquals(expectedraw[0], uri.hierPartRaw);
            Assert.assertEquals(expectedraw[1], uri.schemeSpecificPartRaw);
            Assert.assertEquals(expectedraw[2], uri.authorityRaw);
            Assert.assertEquals(expectedraw[3], uri.userinfoRaw);
            Assert.assertEquals(expectedraw[4], uri.hostRaw);
            Assert.assertEquals(expectedraw[5], uri.portRaw);
            Assert.assertEquals(expectedraw[6], uri.pathRaw);
            Assert.assertEquals(expectedraw[7], uri.queryRaw);
            Assert.assertEquals(expectedraw[8], uri.fragmentRaw);
        }
        // decoded data
        if (expected != null) {
            Assert.assertEquals(expected[0], uri.scheme);
            Assert.assertEquals(expected[1], uri.schemeSpecificPart);
            Assert.assertEquals(expected[2], uri.authority);
            Assert.assertEquals(expected[3], uri.userinfo);
            Assert.assertEquals(expected[4], uri.host);
            Assert.assertEquals(expected[5], uri.port);
            Assert.assertEquals(expected[6], uri.path);
            Assert.assertEquals(expected[7], uri.query);
            Assert.assertEquals(expected[8], uri.fragment);
        }
        // Raw methods
        Assert.assertEquals(uri.schemeSpecificPartRaw, uri.getRawSchemeSpecificPart());
        Assert.assertEquals(uri.authorityRaw, uri.getRawAuthority());
        Assert.assertEquals(uri.userinfoRaw, uri.getRawUserInfo());
        Assert.assertEquals(uri.pathRaw, uri.getRawPath());
        Assert.assertEquals(uri.queryRaw, uri.getRawQuery());
        Assert.assertEquals(uri.fragmentRaw, uri.getRawFragment());
        // decoded methods
        Assert.assertEquals(uri.scheme, uri.getScheme());
        Assert.assertEquals(uri.schemeSpecificPart, uri.getSchemeSpecificPart());
        Assert.assertEquals(uri.authority, uri.getAuthority());
        Assert.assertEquals(uri.userinfo, uri.getUserInfo());
        Assert.assertEquals(uri.host, uri.getHost());
        Assert.assertEquals(uri.port, uri.getPort());
        Assert.assertEquals(uri.path, uri.getPath());
        Assert.assertEquals(uri.query, uri.getQuery());
        Assert.assertEquals(uri.fragment, uri.getFragment());
        // URI
        if (jdkuri != null) {
            Assert.assertEquals(jdkuri.getScheme(), uri.getScheme());
            Assert.assertEquals(jdkuri.getSchemeSpecificPart(), uri.getSchemeSpecificPart());
            Assert.assertEquals(jdkuri.getAuthority(), uri.getAuthority());
            Assert.assertEquals(jdkuri.getUserInfo(), uri.getUserInfo());
            Assert.assertEquals(jdkuri.getHost(), uri.getHost());
            Assert.assertEquals(jdkuri.getPort(), uri.getPort());
            if (jdkuri.getPath() != null) {
                Assert.assertEquals(jdkuri.getPath(), uri.getPath());
            }
            if (jdkuri.getQuery() != null) {
                Assert.assertEquals(jdkuri.getQuery(), uri.getQuery());
            }
            Assert.assertEquals(jdkuri.getFragment(), uri.getFragment());
        }
    }

}
