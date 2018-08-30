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
package com.dremio.service.accelerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.client.Entity;

import org.junit.Assert;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationStateApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutContainerApiDescriptor;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Util class for acceleration tests
 */
public abstract class AccelerationTestUtil extends BaseTestServer {

  private static AtomicInteger queryNumber = new AtomicInteger(0);

  public static final String HOME_NAME = HomeName.getUserHomePath(SampleDataPopulator.DEFAULT_USER_NAME).getName();
  public static final String TEST_SOURCE = "src";
  public static final String TEST_SPACE = "accel_test";
  public static final String TEST_FOLDER = "test_folder";
  public static final String WORKING_PATH = Paths.get("").toAbsolutePath().toString();
  public static final String[] STRUCTURE = {"src", "test", "resources", "acceleration"};
  public static final String PATH_SEPARATOR = System.getProperty("file.separator");
  public static final String EMPLOYEES_FILE = "employees.json";
  public static final String EMPLOYEES_WITH_NULL_FILE = "employees_with_null.json";
  public static final String EMPLOYEES_VIRTUAL_DATASET_NAME = "ds_emp";
  public static final String RENAMED_EMPLOYEES_VIRTUAL_DATASET_NAME = "renamed_ds_emp";


  // Available Datasets
  public static final DatasetPath EMPLOYEES = new DatasetPath(Arrays.asList(TEST_SOURCE, EMPLOYEES_FILE));
  public static final DatasetPath EMPLOYEES_VIRTUAL = new DatasetPath(Arrays.asList(TEST_SPACE, EMPLOYEES_VIRTUAL_DATASET_NAME));

  public static final FolderPath TEST_FOLDER_PATH = new FolderPath(Arrays.asList(TEST_SPACE, TEST_FOLDER));
  public static final DatasetPath EMPLOYEES_UNDER_FOLDER = new DatasetPath(Arrays.asList(TEST_SPACE, TEST_FOLDER, EMPLOYEES_VIRTUAL_DATASET_NAME));

  public void addCPSource() throws Exception {
    addCPSource(false);
  }

  public void addCPSource(boolean createFolder) throws Exception {

    getSourceService().registerSourceWithRuntime(InternalFileConf.create(TEST_SOURCE, new URI("classpath:///acceleration/"), SchemaMutability.ALL, CatalogService.DEFAULT_METADATA_POLICY));

    final NamespaceService nsService = getNamespaceService();
    final SpaceConfig config = new SpaceConfig().setName(TEST_SPACE);

    if(!nsService.exists(new SpacePath(config.getName()).toNamespaceKey())) {
      nsService.addOrUpdateSpace(new SpacePath(config.getName()).toNamespaceKey(), config);
    }

    if (createFolder) {
      final FolderConfig folderConfig = new FolderConfig().setName(TEST_FOLDER);
      nsService.addOrUpdateFolder(TEST_FOLDER_PATH.toNamespaceKey(), folderConfig);
    }
  }

  public void moveDataset(DatasetPath dataset1, DatasetPath dataset2) throws NamespaceException {
    getNamespaceService().renameDataset(dataset1.toNamespaceKey(), dataset2.toNamespaceKey());
  }

  public void deleteSource() {
    NamespaceKey key = new SourcePath(TEST_SOURCE).toNamespaceKey();
    SourceConfig config;
    try {
      config = getNamespaceService().getSource(key);
      if (config != null) {
        getNamespaceService().deleteSource(key, config.getVersion());
      }
    } catch (NamespaceException e) {

    }
  }

  public void deleteSpace() {
    NamespaceKey key = new SpacePath(TEST_SPACE).toNamespaceKey();
    SpaceConfig config;
    try {
      config = getNamespaceService().getSpace(key);
      if (config != null) {
        getNamespaceService().deleteSpace(key, config.getVersion());
      }
    }catch (NamespaceException e) {
    }
  }

  public void deleteFolder() {
    NamespaceKey key = TEST_FOLDER_PATH.toNamespaceKey();
    FolderConfig config;
    try {
      config = getNamespaceService().getFolder(key);
      if (config != null) {
        getNamespaceService().deleteFolder(key, config.getVersion());
      }
    }catch (NamespaceException e) {
    }
  }

  public void addEmployeesJson() throws Exception {
    addJson(EMPLOYEES, new DatasetPath(Arrays.asList(TEST_SPACE, EMPLOYEES_VIRTUAL_DATASET_NAME)));
  }

  public void renameDataset(DatasetPath ds, DatasetPath dsNew) {
    NamespaceKey key = ds.toNamespaceKey();
    DatasetConfig config;
    try {
      config = getNamespaceService().getDataset(key);
      if (config != null) {
        getNamespaceService().renameDataset(key, dsNew.toNamespaceKey());
      }
    }catch (NamespaceException e) {
    }
  }

  public void deleteDataset(DatasetPath ds) {
    NamespaceKey key = ds.toNamespaceKey();
    DatasetConfig config;
    try {
      config = getNamespaceService().getDataset(key);
      if (config != null) {
        getNamespaceService().deleteDataset(key, config.getVersion());
      }
    }catch (NamespaceException e) {
    }
  }

  public void addJson(DatasetPath path, DatasetPath vdsPath) throws Exception {
    final DatasetConfig dataset = new DatasetConfig()
        .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
        .setFullPathList(path.toPathList())
        .setName(path.getLeaf().getName())
        .setCreatedAt(System.currentTimeMillis())
        .setVersion(null)
        .setOwner(DEFAULT_USERNAME)
        .setPhysicalDataset(new PhysicalDataset()
            .setFormatSettings(new FileConfig().setType(FileType.JSON))
            );
    final NamespaceService nsService = getNamespaceService();
    nsService.addOrUpdateDataset(path.toNamespaceKey(), dataset);
    createDatasetFromParentAndSave(vdsPath, path.toPathString());
  }

  protected JobsService getJobsService() {
    final JobsService service = l(JobsService.class);
    return Preconditions.checkNotNull(service, "jobs service is required");
  }

  protected NamespaceService getNamespaceService() {
    final NamespaceService service = newNamespaceService();
    return Preconditions.checkNotNull(service, "ns service is required");
  }

  protected SourceService getSourceService() {
    final SourceService service = newSourceService();
    return Preconditions.checkNotNull(service, "source service is required");
  }

  protected AccelerationApiDescriptor createNewAcceleration(DatasetPath path) {
    return expectSuccess(
        getBuilder(getAPIv2().path("/accelerations")).buildPost(Entity.entity(path.toPathList(), JSON)),
        AccelerationApiDescriptor.class
        );
  }

  protected AccelerationApiDescriptor pollAcceleration(AccelerationId id) {
    return expectSuccess(getBuilder(getAPIv2().path(String.format("/accelerations/%s", id.getId()))).buildGet(), AccelerationApiDescriptor.class);
  }

  protected AccelerationApiDescriptor waitForLayoutGeneration(final AccelerationId id) throws Exception {
    for (int i=0; i< 100; i++) {
      final AccelerationApiDescriptor descriptor = pollAcceleration(id);
      final LayoutContainerApiDescriptor container = Optional.fromNullable(descriptor.getRawLayouts()).or(new LayoutContainerApiDescriptor());
      if (descriptor.getState() != AccelerationStateApiDescriptor.NEW && !AccelerationUtils.selfOrEmpty(container.getLayoutList()).isEmpty()) {
        return descriptor;
      }
      Thread.sleep(500);
    }
    Assert.fail(String.format("unable to find layouts[%s]", id.getId()));
    return null;
  }

  protected AccelerationApiDescriptor createAcceleration(DatasetPath dataset) throws Exception {
    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(dataset);
    final AccelerationApiDescriptor existingApiDescriptor = pollAcceleration(newApiDescriptor.getId());

    assertEquals(newApiDescriptor.getId(), existingApiDescriptor.getId());
    assertEquals(newApiDescriptor.getType(), existingApiDescriptor.getType());

    final AccelerationApiDescriptor finalApiDescriptor = waitForLayoutGeneration(newApiDescriptor.getId());
    assertEquals(newApiDescriptor.getId(), finalApiDescriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, finalApiDescriptor.getState());
    assertFalse("aggregation layout generation failed", finalApiDescriptor.getAggregationLayouts().getLayoutList().isEmpty());
    assertFalse("raw layout generation failed", finalApiDescriptor.getRawLayouts().getLayoutList().isEmpty());
    assertFalse("dataset schema is required", finalApiDescriptor.getContext().getDatasetSchema().getFieldList().isEmpty());
    return newApiDescriptor;
  }

}
