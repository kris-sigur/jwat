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

import org.jwat.common.Uri;

/**
 * Simple wrapper for a (non) validated WARC ConcurrentTo header.
 *
 * @author nicl
 */
public class WarcConcurrentTo {

    /** Warc-Concurrent-To string representation. */
    public String warcConcurrentToStr;

    /** Warc-Concurrent-To <code>Uri</code> object. */
    public Uri warcConcurrentToUri;

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof WarcConcurrentTo)) {
            return false;
        }
        WarcConcurrentTo concurrentToObj = (WarcConcurrentTo)obj;
        if (warcConcurrentToStr != null) {
            if (!warcConcurrentToStr.equals(concurrentToObj.warcConcurrentToStr)) {
                return false;
            }
        } else if (concurrentToObj.warcConcurrentToStr != null) {
            return false;
        }
        if (warcConcurrentToUri != null) {
            if (!warcConcurrentToUri.equals(concurrentToObj.warcConcurrentToUri)) {
                return false;
            }
        } else if (concurrentToObj.warcConcurrentToUri != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        if (warcConcurrentToStr != null) {
            hashCode ^= warcConcurrentToStr.hashCode();
        }
        if (warcConcurrentToUri != null) {
            hashCode ^= warcConcurrentToUri.hashCode();
        }
        return hashCode;
    }

}
