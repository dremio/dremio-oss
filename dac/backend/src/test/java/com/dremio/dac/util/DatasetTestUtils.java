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
package com.dremio.dac.util;

import static java.util.Arrays.asList;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.file.FilePath;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import java.util.Collections;
import org.apache.calcite.util.Pair;

public class DatasetTestUtils {

  public static Pair<String, String> createDS(
      DatasetVersionMutator service,
      String path,
      String name,
      String table,
      String version,
      Pair<String, String> idVersionPair)
      throws NamespaceException, DatasetNotFoundException {
    return createDS(service, path, name, table, new DatasetVersion(version), idVersionPair);
  }

  public static Pair<String, String> createDS(
      DatasetVersionMutator service,
      String path,
      String name,
      String table,
      String version,
      String previousVersion,
      Pair<String, String> idVersionPair)
      throws NamespaceException, DatasetNotFoundException {
    return createDS(
        service,
        path,
        name,
        table,
        new DatasetVersion(version),
        new DatasetVersion(previousVersion),
        idVersionPair);
  }

  public static Pair<String, String> createDS(
      DatasetVersionMutator service,
      String path,
      String name,
      String table,
      DatasetVersion version,
      Pair<String, String> idVersionPair)
      throws NamespaceException, DatasetNotFoundException {
    return createDS(service, path, name, table, version, null, idVersionPair);
  }

  public static Pair<String, String> createDS(
      DatasetVersionMutator service,
      String path,
      String name,
      String table,
      DatasetVersion version,
      DatasetVersion previousVersion,
      Pair<String, String> idVersionPair)
      throws NamespaceException, DatasetNotFoundException {
    DatasetPath path1 = new DatasetPath(path);
    VirtualDatasetUI ds1 = new VirtualDatasetUI();
    ds1.setFullPathList(path1.toPathList());
    ds1.setContextList(path1.toParentPathList());
    ds1.setVersion(version);
    ds1.setSavedTag(idVersionPair == null ? null : idVersionPair.getValue());
    ds1.setName(name);
    ds1.setState(new VirtualDatasetState().setFrom(new FromTable(path1.toPathString()).wrap()));
    ds1.getState().setColumnsList(asList(new Column("foo", new ExpColumnReference("bar").wrap())));
    ds1.getState().setContextList(path1.toParentPathList());
    ds1.setSql("select * from " + table);
    ds1.setId(idVersionPair == null ? null : idVersionPair.getKey());
    ViewFieldType type = new ViewFieldType("hello", "REAL");
    ds1.setSqlFieldsList(Collections.singletonList(type));
    if (previousVersion != null) {
      ds1.setPreviousVersion(
          new NameDatasetRef(path).setDatasetVersion(previousVersion.getVersion()));
    }
    service.put(ds1);
    service.putVersion(ds1);
    VirtualDatasetUI dsOut = service.get(path1);
    return Pair.of(dsOut.getId(), dsOut.getSavedTag());
  }

  public static String createPhysicalDS(NamespaceService ns, String path, DatasetType datasetType)
      throws NamespaceException {
    DatasetConfig datasetConfig = new DatasetConfig();
    PhysicalDatasetPath physicalDatasetPath = new PhysicalDatasetPath(path);
    datasetConfig.setType(datasetType);
    datasetConfig.setFullPathList(physicalDatasetPath.toPathList());
    datasetConfig.setName(physicalDatasetPath.getLeaf().getName());
    datasetConfig.setCreatedAt(System.currentTimeMillis());
    datasetConfig.setTag(null);
    datasetConfig.setOwner("test_user");
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    ns.addOrUpdateDataset(physicalDatasetPath.toNamespaceKey(), datasetConfig);
    return datasetConfig.getTag();
  }

  public static String createPhysicalDSInHome(
      NamespaceService ns, String path, DatasetType datasetType) throws NamespaceException {
    DatasetConfig datasetConfig = new DatasetConfig();
    FilePath filePath = new FilePath(path);
    datasetConfig.setType(datasetType);
    datasetConfig.setFullPathList(filePath.toPathList());
    datasetConfig.setName(filePath.getFileName().toString());
    datasetConfig.setCreatedAt(System.currentTimeMillis());
    datasetConfig.setTag(null);
    datasetConfig.setOwner("test_user");
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    ns.addOrUpdateDataset(filePath.toNamespaceKey(), datasetConfig);
    return datasetConfig.getTag();
  }
}
