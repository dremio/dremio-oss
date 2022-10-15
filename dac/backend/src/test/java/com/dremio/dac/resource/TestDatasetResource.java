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
package com.dremio.dac.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.ValidationErrorMessage;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.physicaldataset.proto.AccelerationSettingsDescriptor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Tests {@link com.dremio.dac.explore.DatasetResource} API
 */
public class TestDatasetResource extends BaseTestServer {
  private static final String SOURCE_NAME = "mysrc";
  private static final long DEFAULT_REFRESH_PERIOD = TimeUnit.HOURS.toMillis(4);
  private static final long DEFAULT_GRACE_PERIOD = TimeUnit.HOURS.toMillis(12);
  private static final DatasetPath DATASET_PATH = new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds1"));

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  protected NamespaceService getNamespaceService() {
    final NamespaceService service = newNamespaceService();
    return Preconditions.checkNotNull(service, "ns service is required");
  }

  protected SourceService getSourceService() {
    final SourceService service = newSourceService();
    return Preconditions.checkNotNull(service, "source service is required");
  }

  public void addPhysicalDataset(final DatasetPath path, final DatasetType type) throws Exception {
    NamespaceKey datasetPath = path.toNamespaceKey();
    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setName(datasetPath.getName());
    datasetConfig.setType(type);
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    getNamespaceService().tryCreatePhysicalDataset(datasetPath, datasetConfig);
  }

  @Before
  public void setup() throws Exception {
    final NASConf nas = new NASConf();
    nas.path = folder.getRoot().getPath();
    SourceUI source = new SourceUI();
    source.setName(SOURCE_NAME);
    source.setCtime(System.currentTimeMillis());
    source.setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD);
    source.setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD);
    // Please note: if this source is ever refreshed, the physical dataset added below will disappear
    source.setMetadataPolicy(UIMetadataPolicy.of(CatalogService.NEVER_REFRESH_POLICY));
    source.setConfig(nas);
    getSourceService().registerSourceWithRuntime(source);
    addPhysicalDataset(DATASET_PATH, DatasetType.PHYSICAL_DATASET);
  }

  @After
  public void clear() throws Exception {
    deleteSource(SOURCE_NAME);
  }

  @Test
  public void testAccelerationSettings() throws Exception {
    final String endpoint = String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString());
    {
      final AccelerationSettingsDescriptor descriptor = expectSuccess(
          getBuilder(getAPIv2().path(endpoint)).buildGet(),
          AccelerationSettingsDescriptor.class
      );

      assertEquals((Long) DEFAULT_REFRESH_PERIOD, descriptor.getAccelerationRefreshPeriod());
      assertEquals((Long) DEFAULT_GRACE_PERIOD, descriptor.getAccelerationGracePeriod());
    }
  }

  @Test
  public void testAccelerationSettingsNeverRefreshOrExpire() throws Exception {
    final String endpoint = String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString());
    {
      final AccelerationSettingsDescriptor descriptor = expectSuccess(
        getBuilder(getAPIv2().path(endpoint)).buildGet(),
        AccelerationSettingsDescriptor.class
      );

      assertEquals((Long) DEFAULT_REFRESH_PERIOD, descriptor.getAccelerationRefreshPeriod());
      assertEquals((Long) DEFAULT_GRACE_PERIOD, descriptor.getAccelerationGracePeriod());

      descriptor.setAccelerationNeverExpire(true);
      descriptor.setAccelerationNeverRefresh(false);

      expectSuccess(
        getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
          .buildPut(Entity.entity(descriptor, JSON)));

      final AccelerationSettingsDescriptor middleDescriptor = expectSuccess(
        getBuilder(getAPIv2().path(endpoint)).buildGet(),
        AccelerationSettingsDescriptor.class
      );

      assertTrue(middleDescriptor.getAccelerationNeverExpire());
      assertFalse(middleDescriptor.getAccelerationNeverRefresh());

      middleDescriptor.setAccelerationNeverExpire(false);
      middleDescriptor.setAccelerationNeverRefresh(true);

      expectSuccess(
        getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
          .buildPut(Entity.entity(middleDescriptor, JSON)));


      final AccelerationSettingsDescriptor descriptorMod = expectSuccess(
        getBuilder(getAPIv2().path(endpoint)).buildGet(),
        AccelerationSettingsDescriptor.class
      );

      assertFalse(descriptorMod.getAccelerationNeverExpire());
      assertTrue(descriptorMod.getAccelerationNeverRefresh());
    }
  }

  @Test
  public void testAccelerationSettingsRefreshLessthanExpire() throws Exception {
    final String endpoint = String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString());

    final AccelerationSettingsDescriptor goodDescriptor = expectSuccess(
      getBuilder(getAPIv2().path(endpoint)).buildGet(),
      AccelerationSettingsDescriptor.class
    );

    goodDescriptor.setAccelerationRefreshPeriod(1L);
    goodDescriptor.setAccelerationGracePeriod(2L);
    goodDescriptor.setAccelerationNeverExpire(false);
    goodDescriptor.setAccelerationNeverRefresh(false);


    expectSuccess(
      getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
        .buildPut(Entity.entity(goodDescriptor, JSON)));

    final AccelerationSettingsDescriptor badDescriptor = expectSuccess(
      getBuilder(getAPIv2().path(endpoint)).buildGet(),
      AccelerationSettingsDescriptor.class
    );

    badDescriptor.setAccelerationRefreshPeriod(2L); //this is > than expiration
    badDescriptor.setAccelerationGracePeriod(1L);
    badDescriptor.setAccelerationNeverExpire(false);
    badDescriptor.setAccelerationNeverRefresh(false);

    expectError(FamilyExpectation.CLIENT_ERROR,
      getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
        .buildPut(Entity.entity(badDescriptor, JSON)), ValidationErrorMessage.class);

  }

  @Test
  public void testUpdateSettingsInFullMode() throws Exception {
    {
      final AccelerationSettingsDescriptor descriptor = new AccelerationSettingsDescriptor()
        .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
        .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
        .setMethod(RefreshMethod.FULL);

      expectSuccess(
          getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));

      final AccelerationSettingsDescriptor newDescriptor = expectSuccess(
          getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString()))).buildGet(),
          AccelerationSettingsDescriptor.class
      );

      assertNotNull(newDescriptor);
      assertEquals(descriptor.getAccelerationRefreshPeriod(), newDescriptor.getAccelerationRefreshPeriod());
      assertEquals(descriptor.getAccelerationGracePeriod(), newDescriptor.getAccelerationGracePeriod());
      assertEquals(descriptor.getMethod(), newDescriptor.getMethod());
      assertEquals(descriptor.getRefreshField(), newDescriptor.getRefreshField());
    }
  }

  @Test
  public void testUpdateSettingsInIncrementalMode() throws Exception {
    {
      final AccelerationSettingsDescriptor descriptor = new AccelerationSettingsDescriptor()
        .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
        .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
        .setMethod(RefreshMethod.INCREMENTAL)
        .setRefreshField("test-field");

      expectSuccess(
          getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));

      final AccelerationSettingsDescriptor newDescriptor = expectSuccess(
          getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString()))).buildGet(),
          AccelerationSettingsDescriptor.class
      );

      assertNotNull(newDescriptor);
      assertEquals(descriptor.getAccelerationRefreshPeriod(), newDescriptor.getAccelerationRefreshPeriod());
      assertEquals(descriptor.getAccelerationGracePeriod(), newDescriptor.getAccelerationGracePeriod());
      assertEquals(descriptor.getMethod(), newDescriptor.getMethod());
      assertEquals(descriptor.getRefreshField(), newDescriptor.getRefreshField());
    }
  }

  @Test
  public void testValidation() throws Exception {
    {
      final AccelerationSettingsDescriptor descriptor = new AccelerationSettingsDescriptor()
        .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
        .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
        .setMethod(RefreshMethod.INCREMENTAL);

      expectStatus(Response.Status.BAD_REQUEST,
          getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));
    }

    {
      final AccelerationSettingsDescriptor descriptor = new AccelerationSettingsDescriptor()
        .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
        .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
        .setMethod(RefreshMethod.FULL)
        .setRefreshField("some-field");

      expectStatus(Response.Status.BAD_REQUEST,
          getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));
    }

    {
      final DatasetPath path2 = new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds2"));
      addPhysicalDataset(path2, DatasetType.PHYSICAL_DATASET_SOURCE_FILE);

      final AccelerationSettingsDescriptor descriptor = new AccelerationSettingsDescriptor()
        .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
        .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
        .setMethod(RefreshMethod.INCREMENTAL)
        .setRefreshField("some-field");

      expectStatus(Response.Status.BAD_REQUEST,
          getBuilder(getAPIv2().path(String.format("/dataset/%s/acceleration/settings", path2.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));
    }

    {
      expectStatus(Response.Status.BAD_REQUEST,
        getBuilder(getAPIv2().path(String.format("/dataset/%s/moveTo/%s", DATASET_PATH.toPathString(), DATASET_PATH.toPathString())))
          .build("POST"));
    }

    {
      expectStatus(Response.Status.BAD_REQUEST,
        getBuilder(getAPIv2().path(String.format("/dataset/%s/rename/", DATASET_PATH.toPathString()))
          .queryParam("renameTo", DATASET_PATH.getLeaf().toString()))
          .build("POST"));
    }

  }
}
