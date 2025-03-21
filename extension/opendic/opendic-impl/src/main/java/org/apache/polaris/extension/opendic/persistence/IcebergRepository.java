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

import jakarta.inject.Inject;
import org.apache.iceberg.catalog.Catalog;
import org.apache.polaris.extension.opendic.entity.SchemaRegistryService;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntity;

import java.util.List;

public class IcebergRepository implements IRepositoryBase {
    Catalog catalog;

    @Inject
    SchemaRegistryService schemaRegistry;

    private IcebergRepository() {
        catalog = IcebergConfig.createRESTCatalog("polaris", IcebergConfig.readDockerSecret("engineer-id"), IcebergConfig.readDockerSecret("engineer-secret"));
    }

    public static IcebergRepository createIcebergRepository() {
        return new IcebergRepository();
    }


    @Override
    public void initializeTable(String entityTypeName) {

    }

    @Override
    public void saveEntity(UserDefinedEntity entity) {

    }

    @Override
    public List<UserDefinedEntity> listEntities(String entityTypeName) {
        return List.of();
    }

    @Override
    public List<String> listEntityTypes() {
        return List.of();
    }
}
