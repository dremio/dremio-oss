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

import static com.dremio.plugins.NessieClientOptions.BYPASS_CONTENT_CACHE;
import static com.dremio.plugins.NessieClientOptions.NESSIE_CONTENT_CACHE_SIZE_ITEMS;
import static com.dremio.plugins.NessieClientOptions.NESSIE_CONTENT_CACHE_TTL_MINUTES;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedViews;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadata;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadataParser;
import com.dremio.options.OptionManager;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClientImpl;
import java.net.URI;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

@ExtendWith(OlderNessieServersExtension.class)
@NessieServerProperty(name = "nessie.test.storage.kind", value = "PERSIST")
public class BaseIcebergViewTest {

  @TempDir protected static Path temp;

  protected static String warehouseLocation;
  @NessieBaseUri protected static URI nessieBaseUri;
  @NessieAPI protected static NessieApiV2 nessieApi;
  protected static NessieClient nessieClient;
  protected static Configuration fileSystemConfig;
  protected static FileSystemPlugin fsPlugin;
  protected static FileIO fileIO;
  protected static final String userName = "test-user";
  protected static IcebergNessieVersionedViews icebergNessieVersionedViews;

  @BeforeAll
  public static void setup() throws Exception {
    warehouseLocation = temp.toUri().toString();
    fileSystemConfig = new Configuration();
    fsPlugin = BaseTestQuery.getMockedFileSystemPlugin();
    fileIO = fsPlugin.createIcebergFileIO(fsPlugin.getSystemUserFS(), null, null, null, null);

    SabotContext sabotContext = mock(SabotContext.class);
    when(fsPlugin.getContext()).thenReturn(sabotContext);

    OptionManager optionManager = mock(OptionManager.class);
    doReturn(NESSIE_CONTENT_CACHE_SIZE_ITEMS.getDefault().getNumVal())
        .when(optionManager)
        .getOption(NESSIE_CONTENT_CACHE_SIZE_ITEMS);
    doReturn(NESSIE_CONTENT_CACHE_TTL_MINUTES.getDefault().getNumVal())
        .when(optionManager)
        .getOption(NESSIE_CONTENT_CACHE_TTL_MINUTES);
    doReturn(BYPASS_CONTENT_CACHE.getDefault().getBoolVal())
        .when(optionManager)
        .getOption(BYPASS_CONTENT_CACHE);
    when(sabotContext.getOptionManager()).thenReturn(optionManager);

    nessieClient = new NessieClientImpl(nessieApi, fsPlugin.getContext().getOptionManager());

    icebergNessieVersionedViews =
        new IcebergNessieVersionedViews(
            warehouseLocation,
            nessieClient,
            fileSystemConfig,
            fsPlugin,
            userName,
            BaseIcebergViewTest::viewMetadataLoader);
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

  private static ViewVersionMetadata viewMetadataLoader(String metadataLocation) {
    return ViewVersionMetadataParser.read(fileIO.newInputFile(metadataLocation));
  }
}
