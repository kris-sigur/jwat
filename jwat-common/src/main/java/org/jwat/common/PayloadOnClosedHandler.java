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

/**
 * Callback handler used by the payload implementation to notify the initiating
 * caller that the payload has been closed and all payload data have been read
 * or skipped.
 *
 * @author nicl
 */
public interface PayloadOnClosedHandler {

    /**
     * Method called when the associated payload has been fully read or
     * skipped.
     * @throws IOException i/o exception while closing payload
     */
    void payloadClosed() throws IOException;

}
