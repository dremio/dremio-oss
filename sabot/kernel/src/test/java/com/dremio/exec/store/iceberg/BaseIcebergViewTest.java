/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.exec.store.iceberg;

import java.net.URI;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.nessie.NessieExtCatalog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedViews;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClientImpl;
import com.google.common.collect.ImmutableMap;

@ExtendWith(OlderNessieServersExtension.class)
public class BaseIcebergViewTest {

  @TempDir protected static Path temp;

  protected static String warehouseLocation;
  @NessieBaseUri
  protected static URI nessieBaseUri;
  @NessieAPI
  protected static NessieApiV1 nessieApi;
  protected static NessieClient nessieClient;
  protected static Configuration fileSystemConfig;
  protected static FileSystemPlugin fsPlugin;
  protected static final String userName = "test-user";
  protected static IcebergNessieVersionedViews icebergNessieVersionedViews;

  protected static NessieExtCatalog nessieExtCatalog;

  @BeforeAll
  public static void setup() throws Exception {
    warehouseLocation = temp.toUri().toString();
    nessieClient = new NessieClientImpl(nessieApi);
    fileSystemConfig = new Configuration();
    fsPlugin = BaseTestQuery.getMockedFileSystemPlugin();
    icebergNessieVersionedViews =
        new IcebergNessieVersionedViews(
            warehouseLocation, nessieClient, fileSystemConfig, fsPlugin, userName);

    initCatalog(nessieBaseUri.resolve("v1"), "main");
  }

  private static void initCatalog(URI x, String ref) {
    nessieExtCatalog = new NessieExtCatalog();
    nessieExtCatalog.setConf(fileSystemConfig);
    nessieExtCatalog.initialize(
        "nessie",
        ImmutableMap.of(
            CatalogProperties.URI,
            x.toString(),
            "ref",
            ref,
            "auth-type",
            "NONE",
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation));
  }

  protected ResolvedVersionContext getVersion(String branchName) {
    return nessieClient.resolveVersionContext(VersionContext.ofBranch(branchName));
  }

  protected void createBranch(String branchName, VersionContext versionContext) {
    nessieClient.createBranch(branchName, versionContext);
  }

  protected void createNamespacesIfMissing(String branchName, ContentKey key) {
    createNamespacesIfMissing(branchName, key.getNamespace());
  }

  protected void createNamespacesIfMissing(String branchName, Namespace ns) {
    if (ns.isEmpty()) {
      return;
    }

    if (ns.getElements().size() > 1) {
      createNamespacesIfMissing(branchName, ns.getParent());
    }

    try {
      // Modern Nessie servers require namespaces to exist before tables can be created in them.
      // The getContent() here is not very strict in that it does not check the type of content
      // if it exists, but table creation will fail later if it uses a non-namespace object as
      // a namespace.
      if (nessieApi.getContent().refName(branchName).key(ns.toContentKey()).get().isEmpty()) {
        nessieApi.createNamespace().refName(branchName).namespace(ns).create();
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
