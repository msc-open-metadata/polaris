/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.extension.opendic.persistence;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Set;

public interface IUnameCache {

    /**
     * @param tableIdentifier full id of table. Example: {@code "SYSTEM.function" }
     * @param uname           name of the function. Example: {@code "foo"}
     */
    void addUnameEntry(TableIdentifier tableIdentifier, String uname);

    /**
     * @param tableIdentifier full name of table. Example: {@code "SYSTEM.function" }
     * @param unames          set of function names. Example: {@code Set.of("foo", "bar")}
     */
    void addUnameEntries(TableIdentifier tableIdentifier, Set<String> unames);


    /**
     * @param tableIdentifier full name of table. Example: {@code "SYSTEM.function" }
     */
    void addTable(TableIdentifier tableIdentifier);


    /**
     * @param tableIdentifier full name of table. Example: {@code "SYSTEM.function" }
     * @param uname           name of the function. Example: {@code "foo"}
     */
    boolean deleteUnameEntry(TableIdentifier tableIdentifier, String uname);

    /**
     * @param tableIdentifier full name of table. Example: {@code "SYSTEM.function" }
     */
    boolean deleteUnameTableEntry(TableIdentifier tableIdentifier);

    /**
     * @param tableIdentifier full name of table. Example: {@code "SYSTEM.function" }
     * @param uname           name of the function. Example: {@code "foo"}
     */
    void checkUnameDoesNotExist(TableIdentifier tableIdentifier, String uname);


}
