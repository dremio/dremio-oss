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
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.jaxrs.ext.NessieUri;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedViews;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClientImpl;
import com.google.common.collect.ImmutableMap;

@ExtendWith(DatabaseAdapterExtension.class)
@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
public class BaseIcebergViewTest {
  @NessieDbAdapter(storeWorker = TableCommitMetaStoreWorker.class)
  static DatabaseAdapter databaseAdapter;

  @RegisterExtension
  static NessieJaxRsExtension server = buildNessieJaxRsExtension();

  private static NessieJaxRsExtension buildNessieJaxRsExtension() {
    // Prevents CDIExtension to load beans through JNDI causing
    // an exception as Weld hasn't initialized a JNDI context
    System.setProperty("com.sun.jersey.server.impl.cdi.lookupExtensionInBeanManager", "true");
    // Use a dynamically allocated port, not a static default (80/443) or statically
    // configured port.
    System.setProperty(TestProperties.CONTAINER_PORT, "0");

    return new NessieJaxRsExtension(() -> databaseAdapter);
  }

  @TempDir protected static Path temp;

  protected static String warehouseLocation;
  protected static NessieApiV1 nessieApi;
  protected static NessieClient nessieClient;
  protected static Configuration fileSystemConfig;
  protected static FileSystemPlugin fsPlugin;
  protected static IcebergNessieVersionedViews icebergNessieVersionedViews;

  protected static NessieExtCatalog nessieExtCatalog;

  @BeforeAll
  public static void setup(@NessieUri URI x) throws Exception {
    warehouseLocation = temp.toUri().toString();
    nessieApi = HttpClientBuilder.builder().withUri(x.toString()).build(NessieApiV1.class);
    nessieClient = new NessieClientImpl(nessieApi);
    fileSystemConfig = new Configuration();
    fsPlugin = BaseTestQuery.getMockedFileSystemPlugin();
    icebergNessieVersionedViews =
        new IcebergNessieVersionedViews(
            warehouseLocation, nessieClient, fileSystemConfig, fsPlugin);

    initCatalog(x, "main");
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
}
