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

import com.google.common.base.Preconditions;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Naive in-memory cache for uname duplication checking. Needs atleast 2 improvements for better scaling:
 * 1. Cache needs to be consistent between Polaris instances. State in icebergRepository does not work. Use something like a redis cache.
 * 2. Write to iceberg tables and the cache need to be executed as a single atomic transaction.
 */
public record UnameCacheInMemory(Map<String, Set<String>> unameCacheMap) implements IUnameCache {

    public UnameCacheInMemory() {
        this(new HashMap<>());
    }

    public UnameCacheInMemory(Map<String, Set<String>> unameCacheMap) {
        this.unameCacheMap = unameCacheMap;
    }

    @Override
    public void addUnameEntry(TableIdentifier tableIdentifier, String uname) {
        String fullTableName = tableIdentifier.toString();
        if (!unameCacheMap.containsKey(fullTableName)) {
            throw new NotFoundException("Table %s does not exist in cache", fullTableName);
        }
        Set<String> unames = unameCacheMap.get(fullTableName);
        if (unames.contains(uname)) {
            throw new AlreadyExistsException("Uname %s already exists in %s cache", uname, fullTableName);
        }
        unames.add(uname);
        unameCacheMap.put(fullTableName, unames);
    }

    @Override
    public void addUnameEntries(TableIdentifier tableIdentifier, Set<String> unames) {
        addUnameEntries(tableIdentifier.toString(), unames);
    }

    private void addUnameEntries(String fullTableName, Set<String> unames) {
        if (!unameCacheMap.containsKey(fullTableName)) {
            throw new NotFoundException("Table %s does not exist in cache", fullTableName);
        }

        Set<String> existingUnames = unameCacheMap.get(fullTableName);
        for (String uname : unames) {
            if (existingUnames.contains(uname)) {
                throw new AlreadyExistsException("Uname %s already exists in %s cache", uname, fullTableName);
            }
        }
        existingUnames.addAll(unames);

        unameCacheMap.put(fullTableName, existingUnames);
    }

    @Override
    public void addTable(TableIdentifier tableIdentifier) {
        addTable(tableIdentifier.toString());
    }

    private void addTable(String fullTableName) {
        if (unameCacheMap.containsKey(fullTableName)) {
            throw new AlreadyExistsException("Table %s already exists in cache", fullTableName);
        }
        unameCacheMap.put(fullTableName, new HashSet<>());
    }

    @Override
    public boolean deleteUnameEntry(TableIdentifier tableIdentifier, String uname) {
        var fullTableName = tableIdentifier.toString();
        if (!unameCacheMap.containsKey(fullTableName)) {
            throw new NotFoundException("Table %s does not exist in cache", fullTableName);
        }
        Set<String> unames = unameCacheMap.get(fullTableName);
        boolean removed = unames.remove(uname);
        if (!removed) {
            throw new NotFoundException("Uname %s does not exist in %s cache", uname, fullTableName);
        }
        unameCacheMap.put(fullTableName, unames);
        return true;
    }

    @Override
    public boolean deleteUnameTableEntry(TableIdentifier tableIdentifier) {
        var fullTableName = tableIdentifier.toString();
        if (!unameCacheMap.containsKey(fullTableName)) {
            throw new NotFoundException("Table %s does not exist in cache", fullTableName);
        }
        unameCacheMap.remove(fullTableName);
        return true;
    }


    /**
     * Throws if {@code uname} Exists in {@code tableIdentifier}
     *
     * @param tableIdentifier full name of table. Example: {@code "SYSTEM.function" }
     * @param uname           name of the function. Example: {@code "foo"}
     */
    @Override
    public void checkUnameDoesNotExist(TableIdentifier tableIdentifier, String uname) {
        var fullTableName = tableIdentifier.toString();
        Preconditions.checkArgument(unameCacheMap.containsKey(fullTableName), "Table %s does not exist in cache", fullTableName);
        Set<String> unames = unameCacheMap.get(fullTableName);
        if (unames.contains(uname)) {
            throw new AlreadyExistsException("Uname %s already exists in %s cache", uname, fullTableName);
        }
    }
}

