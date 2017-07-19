/*
 * Copyright (C) 2017 Dremio Corporation
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

import static org.junit.Assert.assertFalse;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;
import javax.ws.rs.client.Entity;

import org.junit.Assert;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationInfoApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationStateApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutContainerApiDescriptor;
import com.dremio.dac.proto.model.source.ClassPathConfig;
import com.dremio.dac.resource.ApiIntentMessageMapper;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.errors.AccelerationNotFoundException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationState;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.TimePeriod;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Util class for acceleration tests
 */
public abstract class AccelerationTestUtil extends BaseTestServer {

  public static final ApiIntentMessageMapper MAPPER = new ApiIntentMessageMapper();
  private static AtomicInteger queryNumber = new AtomicInteger(0);

  public static final String HOME_NAME = HomeName.getUserHomePath(SampleDataPopulator.DEFAULT_USER_NAME).getName();
  public static final String TEST_SOURCE = "src";
  public static final String TEST_SPACE = "accel_test";
  public static final String WORKING_PATH = Paths.get("").toAbsolutePath().toString();
  public static final String[] STRUCTURE = {"src", "test", "resources", "acceleration"};
  public static final String PATH_SEPARATOR = System.getProperty("file.separator");
  public static final String EMPLOYEES_FILE = "employees.json";
  public static final String EMPLOYEES_WITH_NULL_FILE = "employees_with_null.json";

  // Available Datasets
  public static final DatasetPath EMPLOYEES = new DatasetPath(Arrays.asList(TEST_SOURCE, "employees.json"));
  public static final DatasetPath EMPLOYEES_VIRTUAL = new DatasetPath(Arrays.asList(TEST_SPACE, "ds_emp"));

  public void addCPSource() throws Exception {
    final ClassPathConfig nas = new ClassPathConfig();
    nas.setPath("/acceleration");
    SourceUI source = new SourceUI();
    source.setName(TEST_SOURCE);
    source.setCtime(System.currentTimeMillis());
    source.setConfig(nas);
    getSourceService().registerSourceWithRuntime(source);

    final NamespaceService nsService = getNamespaceService();
    final SpaceConfig config = new SpaceConfig().setName(TEST_SPACE);
    nsService.addOrUpdateSpace(new SpacePath(config.getName()).toNamespaceKey(), config);
  }

  public void addEmployeesJson() throws Exception {
    addJson(EMPLOYEES, EMPLOYEES_VIRTUAL);
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

  public byte[] readTestJson(String datasetFile) throws Exception {
    // create new data
    ArrayList<String> filePath = new ArrayList<>(STRUCTURE.length + 1);
    for (int i = 0; i < STRUCTURE.length; i++) {
      filePath.add(STRUCTURE[i]);
    }
    filePath.add(datasetFile);

    return Files.readAllBytes(Paths.get(WORKING_PATH, filePath.toArray(new String[filePath.size()])));
  }

  protected static AccelerationService getAccelerationService() {
    final AccelerationService service = l(AccelerationService.class);
    return Preconditions.checkNotNull(service, "acceleration service is required");
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

  private DatasetConfig clone(DatasetConfig config) {
    Serializer<DatasetConfig> serializer = ProtostuffSerializer.of(DatasetConfig.getSchema());
    return serializer.deserialize(serializer.serialize(config));
  }

  protected void setTtl(DatasetPath path, long newTtl) throws Exception {
    DatasetConfig config = getNamespaceService().getDataset(path.toNamespaceKey());
    DatasetConfig newConfig = clone(config);
    newConfig.getPhysicalDataset().getAccelerationSettings().setAccelerationTTL(new TimePeriod(newTtl, TimePeriod.TimeUnit.MINUTES));
    getNamespaceService().addOrUpdateDataset(path.toNamespaceKey(), newConfig);
  }

  protected void accelerateQuery(String query, String testSpace) throws Exception {
    String datasetName = "query" + queryNumber.getAndIncrement();
    final List<String> path = Arrays.asList(testSpace, datasetName);
    final DatasetPath datasetPath = new DatasetPath(path);
    createDatasetFromSQLAndSave(datasetPath, query, Collections.<String>emptyList());
    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(datasetPath);

    final AccelerationApiDescriptor descriptor = waitForLayoutGeneration(newApiDescriptor.getId());
    assertFalse(descriptor.getRawLayouts().getLayoutList().isEmpty());
    descriptor.getRawLayouts().setEnabled(true);

    AccelerationApiDescriptor newApiDescriptor2 = saveAcceleration(descriptor.getId(), descriptor);

    waitForMaterialization(descriptor.getId(), true);
  }

  protected AccelerationApiDescriptor createNewAcceleration(DatasetPath path) {
    return expectSuccess(
      getBuilder(getAPIv2().path("/accelerations")).buildPost(Entity.entity(path.toPathList(), JSON)),
      AccelerationApiDescriptor.class
    );
  }

  protected AccelerationApiDescriptor saveAcceleration(AccelerationId id, AccelerationApiDescriptor descriptor) {
    return expectSuccess(getBuilder(
      getAPIv2().path(String.format("/accelerations/%s", id.getId()))).buildPut(Entity.entity(MAPPER.toIntentMessage(descriptor), JSON)),
      AccelerationApiDescriptor.class);
  }

  protected GenericErrorMessage saveAccelerationFailure(AccelerationId id, AccelerationApiDescriptor descriptor) {
    return expectError(FamilyExpectation.SERVER_ERROR, getBuilder(
      getAPIv2().path(String.format("/accelerations/%s", id.getId()))).buildPut(Entity.entity(MAPPER.toIntentMessage(descriptor), JSON)),
      GenericErrorMessage.class);
  }

  protected AccelerationApiDescriptor pollAcceleration(AccelerationId id) {
    return expectSuccess(getBuilder(getAPIv2().path(String.format("/accelerations/%s", id.getId()))).buildGet(), AccelerationApiDescriptor.class);
  }

  protected AccelerationInfoApiDescriptor getAccelerationForDataset(DatasetPath path) {
    return expectSuccess(getBuilder(getAPIv2().path(String.format("/accelerations/dataset/%s", "src.\"names.csv\""))).buildGet(), AccelerationInfoApiDescriptor.class);
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

  public List<Materialization> getMaterializations(AccelerationId id) throws AccelerationNotFoundException {
    final String message = String.format("unable to find acceleration[id: %s]", id.getId());
    final Acceleration acc = getOrFailChecked(getAccelerationService().getAccelerationById(id), message);
    Iterator<Layout> layouts = AccelerationUtils.getAllLayouts(acc).iterator();
    if (!layouts.hasNext()) {
      return Lists.newArrayList();
    }

    List<Materialization> materializations = Lists.newArrayList();
    while (layouts.hasNext()) {
      materializations.addAll(Lists.newArrayList(getAccelerationService().getMaterializations(layouts.next().getId()).iterator()));
    }
    return materializations;
  }

  public void waitForMaterialization(final AccelerationId id, final boolean eachLayoutMustMaterialize) throws Exception {

    for (int i = 0; i < 120; i++) {
      Thread.sleep(500);
      final List<Materialization> materializations = getMaterializations(id);
      if (materializations.isEmpty()) {
        continue;
      }

      final boolean anyFailed = Iterables.any(materializations, new Predicate<Materialization>() {
        @Override
        public boolean apply(@Nullable final Materialization input) {
          return input.getState() == MaterializationState.FAILED;
        }
      });

      if (anyFailed) {
        Assert.fail("materialization failed");
      }

      final boolean allDone = Iterables.all(materializations, new Predicate<Materialization>() {
        @Override
        public boolean apply(@Nullable final Materialization input) {
          return input.getState() == MaterializationState.DONE;
        }
      });

      if (eachLayoutMustMaterialize && allDone) {
        return;
      }

      final boolean anyDone = Iterables.any(materializations, new Predicate<Materialization>() {
        @Override
        public boolean apply(@Nullable final Materialization input) {
          return input.getState() == MaterializationState.DONE;
        }
      });

      if (!eachLayoutMustMaterialize && anyDone) {
        return;
      }
    }

    Assert.fail("Timed out waiting for materializations");
  }

  private static <T> T getOrFailChecked(final Optional<T> entry, final String message) throws AccelerationNotFoundException {
    if (entry.isPresent()) {
      return entry.get();
    }

    throw new AccelerationNotFoundException(message);
  }
}
