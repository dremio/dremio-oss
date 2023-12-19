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
package com.dremio.dac.explore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.SecurityContext;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.physicaldataset.proto.AccelerationSettingsDescriptor;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.test.DremioTest;

/**
 * Tests for acceleration settings.
 */
public class TestAccelerationSettingsFromAPI extends DremioTest {
  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.LENIENT);

  @Mock private DatasetVersionMutator datasetService;
  @Mock private JobsService jobsService;
  @Mock private SecurityContext securityContext;
  @Mock private ReflectionServiceHelper reflectionServiceHelper;
  @Mock private NamespaceService namespaceService;
  @Mock private CollaborationHelper collaborationService;
  @Mock private BufferAllocatorFactory bufferAllocatorFactory;
  @Mock private ReflectionSettings reflectionSettings;
  @Mock private Catalog catalog;
  @Mock private DataplanePlugin dataplanePlugin;
  @Mock private DremioTable dremioTable;
  @Mock private DatasetPath datasetPath;

  private List<String> path = Arrays.asList("versioned", "table");

  private AccelerationSettings accelerationSettings =
      new AccelerationSettings()
          .setMethod(RefreshMethod.FULL)
          .setRefreshPeriod(TimeUnit.HOURS.toMillis(1))
          .setGracePeriod(TimeUnit.HOURS.toMillis(2))
          .setNeverExpire(false)
          .setNeverRefresh(false);

  private AccelerationSettingsDescriptor accelerationSettingsDescriptor =
      new AccelerationSettingsDescriptor()
          .setMethod(RefreshMethod.FULL)
          .setAccelerationRefreshPeriod(TimeUnit.HOURS.toMillis(1))
          .setAccelerationGracePeriod(TimeUnit.HOURS.toMillis(2))
          .setAccelerationNeverExpire(false)
          .setAccelerationNeverRefresh(false);

  @InjectMocks private DatasetResource datasetResource;

  @Before
  public void setup() {
    when(datasetService.getCatalog()).thenReturn(catalog);
    when(catalog.getSource(anyString())).thenReturn(dataplanePlugin);
    when(datasetPath.toPathList()).thenReturn(path);
    when(datasetPath.toPathString()).thenReturn(String.join(".", path));
    when(datasetPath.toNamespaceKey()).thenReturn(new NamespaceKey(path));
  }

  @Test
  public void getAccelerationSettingsWithoutVersionType() throws Exception {
    assertThatThrownBy(() -> datasetResource.getAccelerationSettings(null, "main"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Missing a valid versionType/versionValue");
  }

  @Test
  public void getAccelerationSettingsWithoutVersionValue() throws Exception {
    assertThatThrownBy(() -> datasetResource.getAccelerationSettings("BRANCH", null))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Missing a valid versionType/versionValue");
  }

  @Test
  public void getAccelerationSettingsNotFoundTable() throws Exception {
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
        .thenReturn(null);

    assertThatThrownBy(() -> datasetResource.getAccelerationSettings("BRANCH", "main"))
        .isInstanceOf(DatasetNotFoundException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void getAccelerationSettingsForView() throws Exception {
    final DatasetConfig datasetConfig = new DatasetConfig().setType(DatasetType.VIRTUAL_DATASET);
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
      .thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class)))
      .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    assertThatThrownBy(() -> datasetResource.getAccelerationSettings("BRANCH", "main"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("only to physical dataset");
  }

  @Test
  public void getAccelerationSettings() throws Exception {
    final DatasetConfig datasetConfig = new DatasetConfig().setType(DatasetType.PHYSICAL_DATASET);
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
        .thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class)))
      .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final AccelerationSettingsDescriptor descriptor =
        datasetResource.getAccelerationSettings("BRANCH", "main");

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getMethod()).isEqualTo(RefreshMethod.FULL);
    assertThat(descriptor.getAccelerationNeverRefresh()).isFalse();
    assertThat(descriptor.getAccelerationRefreshPeriod()).isEqualTo(TimeUnit.HOURS.toMillis(1));
    assertThat(descriptor.getAccelerationNeverExpire()).isFalse();
    assertThat(descriptor.getAccelerationGracePeriod()).isEqualTo(TimeUnit.HOURS.toMillis(2));
  }

  @Test
  public void updateAccelerationSettingsNullDescriptor() throws Exception {
    assertThatThrownBy(() -> datasetResource.updateAccelerationSettings(null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("descriptor is required");
  }

  @Test
  public void updateAccelerationSettingsNonRefreshPeriod() throws Exception {
    assertThatThrownBy(
            () ->
                datasetResource.updateAccelerationSettings(
                    new AccelerationSettingsDescriptor(), null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("refreshPeriod is required");
  }

  @Test
  public void updateAccelerationSettingsNonGracePeriod() throws Exception {
    assertThatThrownBy(
            () ->
                datasetResource.updateAccelerationSettings(
                    new AccelerationSettingsDescriptor()
                        .setAccelerationRefreshPeriod(TimeUnit.HOURS.toMillis(1)),
                    null,
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("gracePeriod is required");
  }

  @Test
  public void updateAccelerationSettingsInvalidPeriod() throws Exception {
    assertThatThrownBy(
            () ->
                datasetResource.updateAccelerationSettings(
                    new AccelerationSettingsDescriptor()
                        .setAccelerationRefreshPeriod(TimeUnit.HOURS.toMillis(2))
                        .setAccelerationGracePeriod(TimeUnit.HOURS.toMillis(1)),
                    null,
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("refreshPeriod must be less than gracePeriod");
  }

  @Test
  public void updateAccelerationSettingsWithoutVersionValue() throws Exception {
    assertThatThrownBy(
            () ->
                datasetResource.updateAccelerationSettings(
                    accelerationSettingsDescriptor, "BRANCH", null))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Missing a valid versionType/versionValue");
  }

  @Test
  public void updateAccelerationSettingsWithoutVersionType() throws Exception {
    assertThatThrownBy(
      () ->
        datasetResource.updateAccelerationSettings(
          accelerationSettingsDescriptor, null, "main"))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Missing a valid versionType/versionValue");
  }

  @Test
  public void updateAccelerationSettingsNotFoundTable() throws Exception {
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
      .thenReturn(null);

    assertThatThrownBy(
            () ->
                datasetResource.updateAccelerationSettings(
                    accelerationSettingsDescriptor, "BRANCH", "main"))
        .isInstanceOf(DatasetNotFoundException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void updateAccelerationSettingsForView() throws Exception {
    final DatasetConfig datasetConfig = new DatasetConfig().setType(DatasetType.VIRTUAL_DATASET);
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
      .thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class)))
      .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    assertThatThrownBy(
            () ->
                datasetResource.updateAccelerationSettings(
                    accelerationSettingsDescriptor, "BRANCH", "main"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("only to physical dataset");
  }

  @Test
  public void updateAccelerationSettings() throws Exception {
    final DatasetConfig datasetConfig = new DatasetConfig().setType(DatasetType.PHYSICAL_DATASET);
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
        .thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class)))
      .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    datasetResource.updateAccelerationSettings(accelerationSettingsDescriptor, "BRANCH", "main");
  }

  @Test
  public void getAccelerationSettingsRefreshMethodAutoNotApplicable() throws Exception {
    final DatasetConfig datasetConfig = new DatasetConfig().setType(DatasetType.PHYSICAL_DATASET);
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    accelerationSettings.setMethod(RefreshMethod.AUTO);

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);
    when(reflectionServiceHelper.isIncrementalRefreshBySnapshotEnabled(any(DatasetConfig.class))).thenReturn(false);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
      .thenReturn(accelerationSettings);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
      .thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class)))
      .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final AccelerationSettingsDescriptor descriptor =
      datasetResource.getAccelerationSettings("BRANCH", "main");

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getMethod()).isEqualTo(RefreshMethod.FULL);
    assertThat(descriptor.getRefreshField()).isNull();
    assertThat(descriptor.getAccelerationNeverRefresh()).isFalse();
    assertThat(descriptor.getAccelerationRefreshPeriod()).isEqualTo(TimeUnit.HOURS.toMillis(1));
    assertThat(descriptor.getAccelerationNeverExpire()).isFalse();
    assertThat(descriptor.getAccelerationGracePeriod()).isEqualTo(TimeUnit.HOURS.toMillis(2));
  }

  @Test
  public void getAccelerationSettingsRefreshMethodAuto() throws Exception {
    final DatasetConfig datasetConfig = new DatasetConfig().setType(DatasetType.PHYSICAL_DATASET);
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    accelerationSettings.setMethod(RefreshMethod.INCREMENTAL);
    accelerationSettings.setRefreshField("abc");

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);
    when(reflectionServiceHelper.isIncrementalRefreshBySnapshotEnabled(any(DatasetConfig.class))).thenReturn(true);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
      .thenReturn(accelerationSettings);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
      .thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class)))
      .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final AccelerationSettingsDescriptor descriptor =
      datasetResource.getAccelerationSettings("BRANCH", "main");

    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getMethod()).isEqualTo(RefreshMethod.AUTO);
    assertThat(descriptor.getRefreshField()).isNull();
    assertThat(descriptor.getAccelerationNeverRefresh()).isFalse();
    assertThat(descriptor.getAccelerationRefreshPeriod()).isEqualTo(TimeUnit.HOURS.toMillis(1));
    assertThat(descriptor.getAccelerationNeverExpire()).isFalse();
    assertThat(descriptor.getAccelerationGracePeriod()).isEqualTo(TimeUnit.HOURS.toMillis(2));
  }

  @Test
  public void updateAccelerationSettingsRefreshMethodAuto() throws Exception {
    final DatasetConfig datasetConfig = new DatasetConfig().setType(DatasetType.PHYSICAL_DATASET);
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    accelerationSettings.setMethod(RefreshMethod.INCREMENTAL);
    accelerationSettings.setRefreshField("abc");

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);
    when(reflectionServiceHelper.isIncrementalRefreshBySnapshotEnabled(any(DatasetConfig.class))).thenReturn(true);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
      .thenReturn(accelerationSettings);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
      .thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class)))
      .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    accelerationSettingsDescriptor.setMethod(RefreshMethod.AUTO);

    datasetResource.updateAccelerationSettings(accelerationSettingsDescriptor, "BRANCH", "main");
    assertThat(accelerationSettings.getMethod()).isEqualByComparingTo(RefreshMethod.AUTO);
    assertThat(accelerationSettings.getRefreshField()).isNull();
  }

  @Test
  public void updateAccelerationSettingsRefreshMethodAutoNotApplicable() throws Exception {
    final DatasetConfig datasetConfig = new DatasetConfig().setType(DatasetType.PHYSICAL_DATASET);
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    accelerationSettings.setMethod(RefreshMethod.INCREMENTAL);

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);
    when(reflectionServiceHelper.isIncrementalRefreshBySnapshotEnabled(any(DatasetConfig.class))).thenReturn(false);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
      .thenReturn(accelerationSettings);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
      .thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class)))
      .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    accelerationSettingsDescriptor.setMethod(RefreshMethod.AUTO);

    datasetResource.updateAccelerationSettings(accelerationSettingsDescriptor, "BRANCH", "main");
    assertThat(accelerationSettings.getMethod()).isEqualByComparingTo(RefreshMethod.AUTO);
    assertThat(accelerationSettings.getRefreshField()).isNull();
  }

  @Test
  public void updateAccelerationSettingsRefreshMethodAutoInvalidRefreshField() {
    final DatasetConfig datasetConfig = new DatasetConfig().setType(DatasetType.PHYSICAL_DATASET);
    final ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofBranch("main", "xyz");

    when(catalog.resolveVersionContext(anyString(), any(VersionContext.class))).thenReturn(resolvedVersionContext);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);
    when(reflectionServiceHelper.isIncrementalRefreshBySnapshotEnabled(any(DatasetConfig.class))).thenReturn(true);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
      .thenReturn(accelerationSettings);
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
      .thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class)))
      .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    accelerationSettingsDescriptor.setMethod(RefreshMethod.AUTO);
    accelerationSettingsDescriptor.setRefreshField("abc");

    assertThatThrownBy(
      () ->
        datasetResource.updateAccelerationSettings(
          accelerationSettingsDescriptor, "BRANCH", "main"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Leave refresh field empty for 'AUTO' refresh method.");
  }
}
