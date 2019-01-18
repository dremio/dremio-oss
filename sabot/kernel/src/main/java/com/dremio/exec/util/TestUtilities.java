/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Set;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;

/**
 * This class contains utility methods to speed up tests. Some of the production code currently calls this method
 * when the production code is executed as part of the test runs. That's the reason why this code has to be in
 * production module.
 */
public class TestUtilities {

  public static final String DFS_TEST_PLUGIN_NAME = "dfs_test";

  /**
   * Create and removes a temporary folder
   *
   * @return absolute path to temporary folder
   */
  public static String createTempDir() {
    final File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    return tmpDir.getAbsolutePath();
  }

  public static void addDefaultTestPlugins(CatalogService catalog, final String tmpDirPath) {
    CatalogServiceImpl catalogImpl = (CatalogServiceImpl) catalog;

    // add dfs.
    {
      SourceConfig c = new SourceConfig();
      InternalFileConf conf = new InternalFileConf();
      conf.connection = "file:///";
      conf.path = "/";
      c.setConnectionConf(conf);
      c.setName("dfs");
      c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE);
      catalogImpl.getSystemUserCatalog().createSource(c);
    }

    addClasspathSource(catalog);

    // add dfs_test
    {
      SourceConfig c = new SourceConfig();
      InternalFileConf conf = new InternalFileConf();
      conf.connection = "file:///";
      conf.path = tmpDirPath;
      conf.mutability = SchemaMutability.ALL;
      c.setConnectionConf(conf);
      c.setName("dfs_test");
      c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE);
      catalogImpl.getSystemUserCatalog().createSource(c);
    }

    // add dfs_root
    {
      SourceConfig c = new SourceConfig();
      InternalFileConf conf = new InternalFileConf();
      conf.connection = "file:///";
      conf.path = "/";
      c.setConnectionConf(conf);
      c.setName("dfs_root");
      c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE);
      catalogImpl.getSystemUserCatalog().createSource(c);
    }

    // add dacfs
    {
      SourceConfig c = new SourceConfig();
      InternalFileConf conf = new InternalFileConf();
      conf.connection = "file:///";
      conf.path = "/";
      c.setConnectionConf(conf);
      c.setName("dacfs");
      c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE);
      catalogImpl.getSystemUserCatalog().createSource(c);
    }

  }

  public static void addClasspathSource(CatalogService catalog) {
    // add cp.
    CatalogServiceImpl catalogImpl = (CatalogServiceImpl) catalog;
    catalogImpl.getSystemUserCatalog().createSource(cp());
  }

  private static SourceConfig cp() {
    SourceConfig c = new SourceConfig();
    InternalFileConf conf = new InternalFileConf();
    conf.connection = "classpath:///";
    conf.path = "/";
    conf.isInternal = false;
    c.setName("cp");
    c.setConnectionConf(conf);
    c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE);
    return c;
  }

  public static void addClasspathSourceIf(CatalogService catalog) {
    try {
      catalog.createSourceIfMissingWithThrow(cp());
    } catch (ConcurrentModificationException e) {
      // no-op since signature was change to throw
    }
  }

  /**
   * Clear all data except the identified values. Note that this delete all stores except those
   * identified and the namespace stores (namespace/splits). For namespace, it does selective delete
   * based on the list of items that should be maintained in savedPaths.
   *
   * @param catalogService
   *          CatalogService
   * @param kvstore
   *          KVStoreProvider
   * @param savedStores
   *          List of kvstores that should be maintained (in addition to namespace).
   * @param savedPaths
   *          List of root entities in namespace that should be maintained in addition to a standard
   *          set of internal entities.
   * @throws NamespaceException
   * @throws IOException
   */
  public static void clear(CatalogService catalogService, KVStoreProvider kvstore, List<String> savedStores, List<String> savedPaths) throws NamespaceException, IOException {
    {
      List<String> list = new ArrayList<>();
      list.add(NamespaceServiceImpl.DAC_NAMESPACE);
      list.add(NamespaceServiceImpl.DATASET_SPLITS);
      list.add(CatalogServiceImpl.CATALOG_SOURCE_DATA_NAMESPACE);
      list.add("wlmqueue");
      list.add("rulesmanager");
      list.add("wlmqueuecontainerversion");
      list.add("sys.options");
      if(savedStores != null) {
        list.addAll(savedStores);
      }
      ((LocalKVStoreProvider) kvstore).deleteEverything(list.toArray(new String[0]));
    }

    final NamespaceService namespace = new NamespaceServiceImpl(kvstore);

    List<String> list = new ArrayList<>();
    list.add("__jobResultsStore");
    list.add("__home");
    list.add("__accelerator");
    list.add("__datasetDownload");
    list.add("__support");
    list.add("$scratch");
    list.add("sys");
    list.add("INFORMATION_SCHEMA");
    if(savedPaths != null) {
      list.addAll(savedPaths);
    }

    final Set<String> rootsToSaveSet = ImmutableSet.copyOf(list);

    for(HomeConfig home : namespace.getHomeSpaces()) {
      String name = "@" + home.getOwner();
      if(rootsToSaveSet.contains(name)) {
        continue;
      }

      namespace.deleteHome(new NamespaceKey("@" + home.getOwner()), home.getTag());
    }

    for(SpaceConfig space : namespace.getSpaces()) {
      if(rootsToSaveSet.contains(space.getName())) {
        continue;
      }

      namespace.deleteSpace(new NamespaceKey(space.getName()), space.getTag());
    }

    ((CatalogServiceImpl) catalogService).deleteExcept(rootsToSaveSet);

  }

  public static void updateDfsTestTmpSchemaLocation(final CatalogServiceImpl catalog, final String tmpDirPath) throws ExecutionSetupException {
    final ManagedStoragePlugin msp = catalog.getManagedSource(DFS_TEST_PLUGIN_NAME);
    final FileSystemPlugin plugin = (FileSystemPlugin) catalog.getSource(DFS_TEST_PLUGIN_NAME);
    SourceConfig newConfig = msp.getId().getClonedConfig();
    InternalFileConf conf = (InternalFileConf) plugin.getConfig();
    conf.path = tmpDirPath;
    conf.mutability = SchemaMutability.ALL;
    newConfig.setConfig(conf.toBytesString());
    catalog.getSystemUserCatalog().updateSource(newConfig);
  }
}
