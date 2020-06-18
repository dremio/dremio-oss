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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.dremio.common.utils.PathUtils;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.LegacySourceType;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;

/**
 * Helper methods for working with the Namespace Service.
 */
public final class NamespaceTestUtils {
  private static final long REFRESH_PERIOD_MS = TimeUnit.HOURS.toMillis(24);
  private static final long GRACE_PERIOD_MS = TimeUnit.HOURS.toMillis(48);

  private NamespaceTestUtils() {
  }

  public static SourceConfig addSource(NamespaceService ns, String name) throws Exception {
    return addSourceWithRefreshAndGracePeriod(ns, name, REFRESH_PERIOD_MS, GRACE_PERIOD_MS);
  }

  public static SourceConfig addSourceWithRefreshAndGracePeriod(NamespaceService ns, String name, long refreshPeriod,
                                                                long gracePeriod) throws Exception {
    final SourceConfig src = new SourceConfig()
      .setName(name)
      .setCtime(100L)
      .setLegacySourceTypeEnum(LegacySourceType.NAS)
      .setAccelerationRefreshPeriod(refreshPeriod)
      .setAccelerationGracePeriod(gracePeriod);
    ns.addOrUpdateSource(new NamespaceKey(name), src);
    return src;
  }

  public static void addSpace(NamespaceService ns, String name) throws Exception {
    final SpaceConfig space = new SpaceConfig();
    space.setName(name);
    ns.addOrUpdateSpace(new NamespaceKey(name), space);
  }

  public static void addFolder(NamespaceService ns, String name) throws Exception {
    final FolderConfig folder = new FolderConfig();
    final NamespaceKey folderPath = new NamespaceKey(PathUtils.parseFullPath(name));
    folder.setName(folderPath.getName());
    folder.setFullPathList(folderPath.getPathComponents());
    ns.addOrUpdateFolder(folderPath, folder);
  }

  public static void addDS(NamespaceService ns, String name) throws Exception {
    final NamespaceKey dsPath = new NamespaceKey(PathUtils.parseFullPath(name));
    final DatasetConfig ds = new DatasetConfig();
    final VirtualDataset vds = new VirtualDataset();
    vds.setVersion(DatasetVersion.newVersion());
    ds.setType(DatasetType.VIRTUAL_DATASET);
    ds.setVirtualDataset(vds);
    ds.setFullPathList(dsPath.getPathComponents());
    ds.setName(dsPath.getName());
    ns.addOrUpdateDataset(dsPath, ds);
  }

  public static void addDS(NamespaceService ns, String name, String sql) throws Exception {
    final NamespaceKey dsPath = new NamespaceKey(PathUtils.parseFullPath(name));
    final DatasetConfig ds = new DatasetConfig();
    final VirtualDataset vds = new VirtualDataset();
    vds.setVersion(DatasetVersion.newVersion());
    vds.setSql(sql);
    ds.setType(DatasetType.VIRTUAL_DATASET);
    ds.setVirtualDataset(vds);
    ds.setFullPathList(dsPath.getPathComponents());
    ds.setName(dsPath.getName());
    ns.addOrUpdateDataset(dsPath, ds);
  }

  public static void addFile(NamespaceService ns, List<String> path) throws Exception {
    NamespaceKey filePath = new NamespaceKey(path);
    final boolean isHome = path.get(0).startsWith("@");
    final DatasetConfig ds = new DatasetConfig()
        .setType(isHome ? DatasetType.PHYSICAL_DATASET_HOME_FILE : DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
        .setPhysicalDataset(new PhysicalDataset()
            .setFormatSettings(new FileConfig()));
    ns.addOrUpdateDataset(filePath, ds);
  }

  public static void addHome(NamespaceService ns, String name) throws Exception {
    final HomeConfig homeConfig = new HomeConfig();
    homeConfig.setOwner(name);
    ns.addOrUpdateHome(new NamespaceKey("@" + name), homeConfig);
  }

  public static void addPhysicalDS(NamespaceService ns, String filePath) throws Exception {
    addPhysicalDS(ns, filePath, null);
  }

  public static void addPhysicalDS(NamespaceService ns, String filePath, byte[] datasetSchema) throws Exception {
    addPhysicalDS(ns, filePath, PHYSICAL_DATASET, datasetSchema);
  }

  public static void addPhysicalDS(NamespaceService ns, String filePath, DatasetType type, byte[] datasetSchema) throws Exception {
    NamespaceKey datasetPath = new NamespaceKey(PathUtils.parseFullPath(filePath));
    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setName(datasetPath.getName());
    datasetConfig.setType(type);

    final PhysicalDataset physicalDataset = new PhysicalDataset();
    if (datasetSchema != null) {
      datasetConfig.setRecordSchema(io.protostuff.ByteString.copyFrom(datasetSchema));
    }
    datasetConfig.setSchemaVersion(DatasetHelper.CURRENT_VERSION);
    datasetConfig.setPhysicalDataset(physicalDataset);
    ns.tryCreatePhysicalDataset(datasetPath, datasetConfig);
  }

  public static Map<String, NameSpaceContainer> listFolder(NamespaceService ns, String parent) throws Exception {
    Map<String, NameSpaceContainer> children = new HashMap<>();
    for (NameSpaceContainer container : ns.list(new NamespaceKey(PathUtils.parseFullPath(parent)))) {
      children.put(PathUtils.constructFullPath(container.getFullPathList()), container);
    }
    return children;
  }

  public static Map<String, NameSpaceContainer> listHome(NamespaceService ns, String parent) throws Exception {
    Map<String, NameSpaceContainer> children = new HashMap<>();
    for (NameSpaceContainer container : ns.list(new NamespaceKey(parent))) {
      children.put(PathUtils.constructFullPath(container.getFullPathList()), container);
    }
    return children;
  }
}
