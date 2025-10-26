/*
 * Copyright 2025, Rob Syme (rob.syme@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package robsyme.plugin.ipld

import groovy.transform.CompileStatic
import groovy.transform.Immutable

/**
 * Represents an IPLD link to another content-addressed object.
 *
 * In IPLD/DAG-CBOR, links are represented as: {"/": "cid"}
 */
@Immutable
@CompileStatic
class Link {
    /**
     * The CID being linked to
     */
    String cid

    /**
     * Create a link from a CID
     */
    static Link of(String cid) {
        return new Link(cid)
    }

    /**
     * Convert to IPLD link format for serialization
     * Returns a map with "/" key pointing to the CID
     */
    Map<String, String> toMap() {
        return ['/' : cid]
    }

    /**
     * Create a Link from the IPLD map format
     */
    static Link fromMap(Map<String, String> map) {
        def cid = map['/']
        if (!cid) {
            throw new IllegalArgumentException("Invalid IPLD link: missing '/' key")
        }
        return new Link(cid)
    }

    @Override
    String toString() {
        return "Link(${cid})"
    }
}
