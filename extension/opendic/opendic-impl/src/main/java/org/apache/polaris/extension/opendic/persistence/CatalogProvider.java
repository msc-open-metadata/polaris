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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.iceberg.rest.RESTCatalog;

@ApplicationScoped
public class CatalogProvider {
    private static final String DEFAULT_CLIENT_ID_PATH = "engineer_client_id";
    private static final String DEFAULT_CLIENT_SECRET_PATH = "engineer_client_secret";
    private final RESTCatalog catalog;
    private final IcebergCatalogConfig.RESTCatalogType catalogType;

    @Inject
    public CatalogProvider() {
        String clientId = IcebergCatalogConfig.readDockerSecret(DEFAULT_CLIENT_ID_PATH);
        String clientSecret = IcebergCatalogConfig.readDockerSecret(DEFAULT_CLIENT_SECRET_PATH);
        this.catalogType = IcebergCatalogConfig.RESTCatalogType.ADLS;
        this.catalog = IcebergCatalogConfig.createRESTCatalog(
                this.catalogType,
                clientId != null ? clientId : DEFAULT_CLIENT_ID_PATH,
                clientSecret != null ? clientSecret : DEFAULT_CLIENT_SECRET_PATH
        );
    }

    public CatalogProvider(IcebergCatalogConfig.RESTCatalogType catalogType) {
        String clientId = IcebergCatalogConfig.readDockerSecret(DEFAULT_CLIENT_ID_PATH);
        String clientSecret = IcebergCatalogConfig.readDockerSecret(DEFAULT_CLIENT_SECRET_PATH);
        this.catalogType = catalogType;
        this.catalog = IcebergCatalogConfig.createRESTCatalog(
                catalogType,
                clientId != null ? clientId : DEFAULT_CLIENT_ID_PATH,
                clientSecret != null ? clientSecret : DEFAULT_CLIENT_SECRET_PATH
        );
    }

    public RESTCatalog getCatalog() {
        return catalog;
    }
    public IcebergCatalogConfig.RESTCatalogType getCatalogType() {
        return catalogType;
    }
}
