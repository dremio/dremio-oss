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
package com.dremio.provision.service;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.nodes.NodeProvider;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.edition.EditionProvider;
import com.dremio.options.OptionManager;
import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterConfig;
import com.dremio.provision.ClusterCreateRequest;
import com.dremio.provision.ClusterDesiredState;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterId;
import com.dremio.provision.ClusterModifyRequest;
import com.dremio.provision.ClusterSpec;
import com.dremio.provision.ClusterState;
import com.dremio.provision.ClusterType;
import com.dremio.provision.DistroType;
import com.dremio.provision.DynamicConfig;
import com.dremio.provision.ImmutableClusterModifyRequest;
import com.dremio.provision.Property;
import com.dremio.provision.PropertyType;
import com.dremio.provision.YarnPropsApi;
import com.dremio.provision.resource.ProvisioningResource;
import com.dremio.service.DirectProvider;
import com.dremio.service.SingletonRegistry;
import com.dremio.test.DremioTest;

/**
 * To test ProvisionResource APIs
 */
public class TestAPI extends DremioTest {

  private static ProvisioningResource resource;
  private static final LegacyKVStoreProvider kvstore =
    new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false).asLegacy();
  private static SingletonRegistry registry = new SingletonRegistry();
  private static ProvisioningServiceImpl service;
  private static final long defaultShutdownInterval = TimeUnit.MINUTES.toMillis(5);

  @BeforeClass
  public static void before() throws Exception {
    registry.bind(LegacyKVStoreProvider.class, kvstore);
    registry.start();
    service = Mockito.spy(new ProvisioningServiceImpl(
      DremioConfig.create(),
      registry.provider(LegacyKVStoreProvider.class),
      Mockito.mock(NodeProvider.class),
      DremioTest.CLASSPATH_SCAN_RESULT,
      DirectProvider.wrap(Mockito.mock(OptionManager.class)),
      DirectProvider.wrap(Mockito.mock(EditionProvider.class))));
    service.start();
    resource = new ProvisioningResource(service);
  }

  @AfterClass
  public static void after() throws Exception {
    kvstore.close();
    registry.close();
    service.close();
  }

  @Test
  public void testModifyService() throws Exception {
    ProvisioningServiceDelegate provServiceDelegate = Mockito.mock(ProvisioningServiceDelegate.class);
    service.getConcreteServices().put(ClusterType.YARN, provServiceDelegate);

    final ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setName("DremioDaemon");
    clusterConfig.setClusterType(ClusterType.YARN);
    clusterConfig.setClusterSpec(new ClusterSpec()
      .setMemoryMBOffHeap(4096)
      .setMemoryMBOnHeap(4096)
      .setContainerCount(2)
      .setVirtualCoreCount(2));
    List<Property> propertyList = new ArrayList<>();
    propertyList.add(new Property(FS_DEFAULT_NAME_KEY, "hdfs://name-node:8020"));
    propertyList.add(new Property("yarn.resourcemanager.hostname", "resource-manager"));
    propertyList.add(new Property(DremioConfig.LOCAL_WRITE_PATH_STRING, "/data/mydata"));
    propertyList.add(new Property(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs"));
    clusterConfig.setSubPropertyList(propertyList);

    doReturn(new ClusterEnriched()).when(provServiceDelegate).startCluster(any(Cluster.class));
    doReturn(new ClusterEnriched()).when(provServiceDelegate).startCluster(any(Cluster.class));
    doNothing().when(provServiceDelegate).stopCluster(any(Cluster.class));

    LegacyKVStore<ClusterId, Cluster> store =
      registry.provider(LegacyKVStoreProvider.class).get().getStore(ProvisioningServiceImpl.ProvisioningStoreCreator.class);

    ClusterId clusterId = new ClusterId(UUID.randomUUID().toString());
    Cluster cluster = new Cluster();
    cluster.setId(clusterId);
    cluster.setState(ClusterState.RUNNING);
    cluster.setDesiredState(ClusterState.RUNNING);
    cluster.setClusterConfig(clusterConfig);

    store.put(clusterId, cluster);

    Iterable<Map.Entry<ClusterId, Cluster>> entries = store.find();
    assertTrue(entries.iterator().hasNext());
    int count = 0;
    for (Map.Entry<ClusterId, Cluster> entry : entries) {
      Cluster clusterEntry = entry.getValue();
      clusterId = clusterEntry.getId();
      count++;
      assertNotNull(clusterEntry.getClusterConfig().getTag());
    }
    assertEquals(1, count);
    service.modifyCluster(clusterId, ClusterState.STOPPED, clusterConfig);
    Cluster clusterModified = store.get(clusterId);
    assertNotNull(clusterModified);
    assertEquals(ClusterState.STOPPED, clusterModified.getDesiredState());
    clusterConfig.setTag("4");
    try {
      service.modifyCluster(clusterId, null, clusterConfig);
      fail("Should be version mismatch");
    } catch (ConcurrentModificationException e) {
      // OK
    }
  }

  @Test
  public void testVersionMismatch() throws Exception {
    Cluster storedCluster = clusterCreateHelper();
    // test version mismatch
    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setTag("11")
        .setShutdownInterval(defaultShutdownInterval)
        .setId(storedCluster.getId().getId())
        .build();

    ClusterConfig config = resource.toClusterConfig(request);
    try {
      final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
      service.toAction(storedCluster, modifiedCluster);
      fail("Version mismatch");
    } catch (ConcurrentModificationException e) {
      // OK
    }
  }

  @Test
  public void testVersionNotMismatch() throws Exception {
    String [] versions = new String [] {"127","128","129"};
    for (String version : versions) {
      Cluster storedCluster = clusterCreateHelper();
      storedCluster.getClusterConfig().setTag(version);
      // test version mismatch
      ClusterModifyRequest request = ClusterModifyRequest.builder()
          .setId(storedCluster.getId().getId())
          .setTag(version)
          .setShutdownInterval(defaultShutdownInterval)
          .build();

      ClusterConfig config = resource.toClusterConfig(request);
      try {
        final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
        service.toAction(storedCluster, modifiedCluster);
      } catch (ConcurrentModificationException e) {
        fail("Version mismatch - request Version: " + request.getTag() +
          ", storedVersion: " + storedCluster.getClusterConfig().getTag());
      }
    }
  }

  @Test
  public void testVersionNull() throws Exception {
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.getClusterConfig().setTag("12");
    // test version not set
    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setShutdownInterval(defaultShutdownInterval)
        .build();

    ClusterConfig config = resource.toClusterConfig(request);
    try {
      final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
      service.toAction(storedCluster, modifiedCluster);
      fail();
    } catch (NullPointerException e) {
      // OK
    }
  }

  /**
   * This test verifies that when the cluster is STARTING we return ACTION.NONE
   * causing the query to wait until the cluster is RUNNING.
   * @throws Exception
   */
  @Test
  public void testStartingCluster() throws Exception {
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.STARTING);

    ClusterModifyRequest request = ClusterModifyRequest.builder()
      .setTag("12")
      .setId(storedCluster.getId().getId())
      .setShutdownInterval(defaultShutdownInterval)
      .build();

    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
    modifiedCluster.setState(ClusterState.RUNNING);

    assertEquals(ProvisioningServiceImpl.Action.NONE, service.toAction(storedCluster, modifiedCluster));

  }
  @Test
  public void testPropertyType() throws Exception {
    ProvisioningServiceDelegate provServiceDelegate = Mockito.mock(ProvisioningServiceDelegate.class);
    service.getConcreteServices().put(ClusterType.YARN, provServiceDelegate);

    Cluster storedCluster = clusterCreateHelper();
    storedCluster.getClusterConfig().setTag(null);

    doReturn(new ClusterEnriched()).when(provServiceDelegate).startCluster(any(Cluster.class));
    doReturn(new ClusterEnriched()).when(provServiceDelegate).startCluster(any(Cluster.class));
    doNothing().when(provServiceDelegate).stopCluster(any(Cluster.class));

    LegacyKVStore<ClusterId, Cluster> store =
      registry.provider(LegacyKVStoreProvider.class).get().getStore(ProvisioningServiceImpl.ProvisioningStoreCreator.class);

    List<Property> properties = storedCluster.getClusterConfig().getSubPropertyList();
    properties.add(new Property("abc", "bcd").setType(PropertyType.JAVA_PROP));
    properties.add(new Property("-Xxyz", "").setType(PropertyType.SYSTEM_PROP));
    properties.add(new Property("JAVA_HOME", "/abc/bcd").setType(PropertyType.ENV_VAR));

    ClusterId clusterId = storedCluster.getId();
    store.put(clusterId, storedCluster);


    Cluster cluster = store.get(clusterId);
    assertNotNull(cluster);
    List<Property> props = cluster.getClusterConfig().getSubPropertyList();
    for (Property prop : props) {
      if ("abc".equals(prop.getKey())) {
        assertTrue(PropertyType.JAVA_PROP.equals(prop.getType()));
      }
      if ("-Xxyz".equals(prop.getKey())) {
        assertTrue(PropertyType.SYSTEM_PROP.equals(prop.getType()));
      }
      if ("JAVA_HOME".equals(prop.getKey())) {
        assertTrue(PropertyType.ENV_VAR.equals(prop.getType()));
      }
    }
  }

  @Test
  public void testDeletedState() throws Exception {
    Cluster storedCluster = clusterCreateHelper();

    ImmutableClusterModifyRequest.Builder request = ClusterModifyRequest.builder();
    // test DELETED state
    storedCluster.setDesiredState(ClusterState.DELETED);
    request.setId(storedCluster.getId().getId());
    request.setTag("12");
    request.setShutdownInterval(defaultShutdownInterval);
    ClusterConfig config = resource.toClusterConfig(request.build());

    try {
      final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
      service.toAction(storedCluster, modifiedCluster);
      fail("Should not modify cluster that is subject for deletion");
    } catch (IllegalStateException e) {
      // OK
    }
  }

  @Test
  public void testStoppingState() throws Exception {
    Cluster storedCluster = clusterCreateHelper();

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .build();

    // test STOPPING state
    storedCluster.setDesiredState(null);
    storedCluster.setState(ClusterState.STOPPING);
    ClusterConfig config = resource.toClusterConfig(request);

    try {
      final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
      service.toAction(storedCluster, modifiedCluster);
      fail("Should not modify cluster that is Stopping");
    } catch (IllegalStateException e) {
      // OK
    }
  }

  @Test
  public void testActionNone() throws Exception {
    // test action NONE - resize to the same number
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.RUNNING);

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .setYarnProps(YarnPropsApi.builder()
            .setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList())
            .setVirtualCoreCount(storedCluster.getClusterConfig().getClusterSpec().getVirtualCoreCount())
            .setMemoryMB(8192)
            .build())

        .build();

    ClusterConfig config = resource.toClusterConfig(request);

    final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster);
    assertEquals(ProvisioningServiceImpl.Action.NONE, action);
  }

  @Test
  public void testResize() throws Exception {
    // test action RESIZE
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.RUNNING);

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(3).build())
        .setYarnProps(YarnPropsApi.builder()
            .setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList())
            .setVirtualCoreCount(storedCluster.getClusterConfig().getClusterSpec().getVirtualCoreCount())
            .setMemoryMB(8192)
            .build())
        .build();
    ClusterConfig config = resource.toClusterConfig(request);

    final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster);
    assertEquals(ProvisioningServiceImpl.Action.RESIZE, action);
  }

  @Test
  public void testStart() throws Exception {
    // test action START
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.STOPPED);

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .setDesiredState(ClusterDesiredState.RUNNING)
        .setYarnProps(YarnPropsApi.builder()
            .setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList())
            .setVirtualCoreCount(storedCluster.getClusterConfig().getClusterSpec().getVirtualCoreCount())
            .setMemoryMB(8192)
            .build())
        .build();
    ClusterConfig config = resource.toClusterConfig(request);

    final Cluster modifiedCluster2 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster2);
    assertEquals(ProvisioningServiceImpl.Action.START, action);
  }

  @Test
  public void testStop() throws Exception {
    // test action STOP
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.RUNNING);

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .setDesiredState(ClusterDesiredState.STOPPED)
        .setYarnProps(YarnPropsApi.builder()
            .setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList())
            .setVirtualCoreCount(storedCluster.getClusterConfig().getClusterSpec().getVirtualCoreCount())
            .setMemoryMB(8192)
            .build())
        .build();

    ClusterConfig config = resource.toClusterConfig(request);

    final Cluster modifiedCluster3 = service.toCluster(config, ClusterState.STOPPED, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster3);
    assertEquals(ProvisioningServiceImpl.Action.STOP, action);
  }

  @Test
  public void testRestartCoresChanges() throws Exception {
    // test action RESTART
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.RUNNING);

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .setDesiredState(ClusterDesiredState.RUNNING)
        .setYarnProps(YarnPropsApi.builder()
            .setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList())
            .setVirtualCoreCount(4)
            .setMemoryMB(8192)
            .build())
        .build();
    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster4 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster4);
    assertTrue(4096 == modifiedCluster4.getClusterConfig().getClusterSpec().getMemoryMBOnHeap());
    assertTrue((8192-4096) == modifiedCluster4.getClusterConfig().getClusterSpec().getMemoryMBOffHeap());
    assertEquals(ProvisioningServiceImpl.Action.RESTART, action);
  }

  @Test
  public void testRestartMemoryChanges() throws Exception {
    // test action RESTART
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.RUNNING);

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .setDesiredState(ClusterDesiredState.RUNNING)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .setYarnProps(YarnPropsApi.builder()
            .setMemoryMB(9126)
            .setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList())
            .build())
        .build();
    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster4 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster4);
    assertTrue(4096 == modifiedCluster4.getClusterConfig().getClusterSpec().getMemoryMBOnHeap());
    assertTrue((9126-4096) == modifiedCluster4.getClusterConfig().getClusterSpec().getMemoryMBOffHeap());
    assertEquals(ProvisioningServiceImpl.Action.RESTART, action);
  }

  @Test
  public void testRestartMemoryOnHeapChanges() throws Exception {
    // test action RESTART
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.RUNNING);

    List<Property> newList = new ArrayList<>(storedCluster.getClusterConfig().getSubPropertyList());
    newList.add(new Property(ProvisioningService.YARN_HEAP_SIZE_MB_PROPERTY, "2096"));

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .setDesiredState(ClusterDesiredState.RUNNING)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .setYarnProps(YarnPropsApi.builder()
            .setSubPropertyList(newList)
            .setVirtualCoreCount(storedCluster.getClusterConfig().getClusterSpec().getVirtualCoreCount())
            .setMemoryMB(8192)
            .build())
        .build();


    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster4 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster4);
    assertTrue(2096 == modifiedCluster4.getClusterConfig().getClusterSpec().getMemoryMBOnHeap());
    assertTrue((8192-2096) == modifiedCluster4.getClusterConfig().getClusterSpec().getMemoryMBOffHeap());
    assertEquals(ProvisioningServiceImpl.Action.RESTART, action);
  }

  @Test
  public void testRestartMemoryOnAndOffHeapChanges() throws Exception {
    // test action RESTART
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.RUNNING);
    List<Property> newList = new ArrayList<>(storedCluster.getClusterConfig().getSubPropertyList());
    newList.add(new Property(ProvisioningService.YARN_HEAP_SIZE_MB_PROPERTY, "2096"));

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .setDesiredState(ClusterDesiredState.RUNNING)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .setYarnProps(YarnPropsApi.builder()
            .setMemoryMB(9126)
            .setSubPropertyList(newList).build())
        .build();
    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster4 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster4);
    assertTrue(2096 == modifiedCluster4.getClusterConfig().getClusterSpec().getMemoryMBOnHeap());
    assertTrue((9126-2096) == modifiedCluster4.getClusterConfig().getClusterSpec().getMemoryMBOffHeap());
    assertEquals(ProvisioningServiceImpl.Action.RESTART, action);
  }

  @Test
  public void testMemoryLimitDefault() throws Exception {
    ClusterCreateRequest clusterCreateRequest = ClusterCreateRequest.builder()
        .setYarnProps(YarnPropsApi.builder()
            .setVirtualCoreCount(2)
            .setDistroType(DistroType.MAPR).setIsSecure(true)
            .setMemoryMB(32767)
            .build())
        .setClusterType(ClusterType.YARN)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .build();

    ClusterConfig clusterConfig = resource.getClusterConfig(clusterCreateRequest);
    assertEquals( 4096, clusterConfig.getClusterSpec().getMemoryMBOnHeap().intValue());
    assertEquals( (32767-4096), clusterConfig.getClusterSpec().getMemoryMBOffHeap().intValue());
  }

  @Test
  public void testMemoryLimitLargeSystem() throws Exception {
    ClusterCreateRequest clusterCreateRequest = ClusterCreateRequest.builder()
        .setYarnProps(YarnPropsApi.builder()
            .setMemoryMB(32768)
            .setVirtualCoreCount(2)
            .setDistroType(DistroType.MAPR)
            .setIsSecure(true)
            .build())
        .setClusterType(ClusterType.YARN)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .build();


    ClusterConfig clusterConfig = resource.getClusterConfig(clusterCreateRequest);
    assertEquals( 8192, clusterConfig.getClusterSpec().getMemoryMBOnHeap().intValue());
    assertEquals( (32768-8192), clusterConfig.getClusterSpec().getMemoryMBOffHeap().intValue());
  }

  @Test
  public void testMemoryLimitOverWrite() throws Exception {
    List<Property> newList = new ArrayList<>();
    newList.add(new Property(ProvisioningService.YARN_HEAP_SIZE_MB_PROPERTY, "2096"));

    ClusterCreateRequest clusterCreateRequest = ClusterCreateRequest.builder()
        .setYarnProps(YarnPropsApi.builder()
            .setMemoryMB(32768)
            .setVirtualCoreCount(2)
            .setDistroType(DistroType.MAPR)
            .setIsSecure(true)
            .setSubPropertyList(newList)
            .build()
            )
        .setClusterType(ClusterType.YARN)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .build();

    ClusterConfig clusterConfig = resource.getClusterConfig(clusterCreateRequest);
    assertEquals( 2096, clusterConfig.getClusterSpec().getMemoryMBOnHeap().intValue());
    assertEquals( (32768-2096), clusterConfig.getClusterSpec().getMemoryMBOffHeap().intValue());
  }

  @Test
  public void testUpdateFromDefaultToLargeSystem() throws Exception {
    ClusterCreateRequest clusterCreateRequest = ClusterCreateRequest.builder()
        .setYarnProps(YarnPropsApi.builder()
            .setMemoryMB(32767)
            .setVirtualCoreCount(2)
            .setDistroType(DistroType.MAPR)
            .setIsSecure(true)
            .build()

            )
        .setClusterType(ClusterType.YARN)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .build();

    ClusterConfig defaultConfig = resource.getClusterConfig(clusterCreateRequest);

    final Cluster initStoredCluster = clusterCreateHelper();
    final Cluster defaultStoredCluster = service.toCluster(defaultConfig, ClusterState.RUNNING, initStoredCluster);

    assertEquals(4096, defaultStoredCluster.getClusterConfig().getClusterSpec().getMemoryMBOnHeap().intValue());
    assertEquals( (32767-4096), defaultStoredCluster.getClusterConfig().getClusterSpec().getMemoryMBOffHeap().intValue());

    ClusterModifyRequest clusterModifyRequest = ClusterModifyRequest.builder()
        .setShutdownInterval(defaultShutdownInterval)
        .setYarnProps(YarnPropsApi.builder()
            .setMemoryMB(32768)
            .build())
        .build();

    ClusterConfig largeSystemConfig = resource.toClusterConfig(clusterModifyRequest);

    final Cluster largeSystemStoredCluster = service.toCluster(largeSystemConfig, ClusterState.RUNNING, defaultStoredCluster);

    assertEquals(8192, largeSystemStoredCluster.getClusterConfig().getClusterSpec().getMemoryMBOnHeap().intValue());
    assertEquals( (32768-8192), largeSystemStoredCluster.getClusterConfig().getClusterSpec().getMemoryMBOffHeap().intValue());
  }

  @Test
  public void testUpdateFromLargeSystemToDefault() throws Exception {
    ClusterCreateRequest clusterCreateRequest = ClusterCreateRequest.builder()
        .setClusterType(ClusterType.YARN)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .setYarnProps(YarnPropsApi.builder()
            .setMemoryMB(32768)
            .setVirtualCoreCount(2)
            .setDistroType(DistroType.MAPR)
            .setIsSecure(true)
            .build())
        .build();
    ClusterConfig defaultConfig = resource.getClusterConfig(clusterCreateRequest);

    final Cluster initStoredCluster = clusterCreateHelper();
    final Cluster defaultStoredCluster = service.toCluster(defaultConfig, ClusterState.RUNNING, initStoredCluster);

    assertEquals(8192, defaultStoredCluster.getClusterConfig().getClusterSpec().getMemoryMBOnHeap().intValue());
    assertEquals( (32768-8192), defaultStoredCluster.getClusterConfig().getClusterSpec().getMemoryMBOffHeap().intValue());

    ClusterModifyRequest clusterModifyRequest = ClusterModifyRequest.builder()
        .setShutdownInterval(defaultShutdownInterval)
        .setYarnProps(YarnPropsApi.builder()
            .setMemoryMB(32767)
            .build())
        .build();

    ClusterConfig largeSystemConfig = resource.toClusterConfig(clusterModifyRequest);

    final Cluster largeSystemStoredCluster = service.toCluster(largeSystemConfig, ClusterState.RUNNING, defaultStoredCluster);

    assertEquals(4096, largeSystemStoredCluster.getClusterConfig().getClusterSpec().getMemoryMBOnHeap().intValue());
    assertEquals( (32767-4096), largeSystemStoredCluster.getClusterConfig().getClusterSpec().getMemoryMBOffHeap().intValue());
  }

  @Test
  public void testRestartProperties() throws Exception {
    // test action RESTART
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.RUNNING);

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .setDesiredState(ClusterDesiredState.RUNNING)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .setYarnProps(YarnPropsApi.builder()
            .setVirtualCoreCount(2)
            .setSubPropertyList(new ArrayList<Property>())
            .build())
        .build();
    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster5 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster5);
    assertEquals(ProvisioningServiceImpl.Action.RESTART, action);
  }

  @Test
  public void testRestartNoChangeProperties() throws Exception {
    // test action RESTART
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.RUNNING);

    ClusterModifyRequest request = ClusterModifyRequest.builder()
        .setId(storedCluster.getId().getId())
        .setTag("12")
        .setShutdownInterval(defaultShutdownInterval)
        .setDesiredState(ClusterDesiredState.RUNNING)
        .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
        .setYarnProps(YarnPropsApi.builder()
            .setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList())
            .setVirtualCoreCount(storedCluster.getClusterConfig().getClusterSpec().getVirtualCoreCount())
            .setMemoryMB(8192)
            .build())
        .build();
    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster5 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster5);
    assertEquals(ProvisioningServiceImpl.Action.NONE, action);
  }

  @Test
  public void testDelete() throws Exception {
    // test action DELETE
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.setState(ClusterState.RUNNING);

    ClusterModifyRequest request = ClusterModifyRequest.builder()
      .setId(storedCluster.getId().getId())
      .setTag("12")
      .setShutdownInterval(defaultShutdownInterval)
      .setDesiredState(ClusterDesiredState.DELETED)
      .setDynamicConfig(DynamicConfig.builder().setContainerCount(2).build())
      .setYarnProps(YarnPropsApi.builder()
          .setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList())
          .setVirtualCoreCount(storedCluster.getClusterConfig().getClusterSpec().getVirtualCoreCount())
          .setMemoryMB(8192)
          .build())
      .build();
    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster6 = service.toCluster(config, ClusterState.DELETED, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster6);
    assertEquals(ProvisioningServiceImpl.Action.DELETE, action);
  }

  private Cluster clusterCreateHelper() {
    ClusterId clusterId = new ClusterId(UUID.randomUUID().toString());
    final Cluster storedCluster = new Cluster();
    storedCluster.setState(ClusterState.RUNNING);
    storedCluster.setId(clusterId);
    ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setTag("12");
    clusterConfig.setClusterType(ClusterType.YARN);
    clusterConfig.setClusterSpec(new ClusterSpec().setContainerCount(2).setMemoryMBOffHeap(4096).setMemoryMBOnHeap(4096).setVirtualCoreCount(2));
    clusterConfig.setDistroType(DistroType.MAPR).setIsSecure(true);
    List<Property> propertyList = new ArrayList<>();
    propertyList.add(new Property(FS_DEFAULT_NAME_KEY, "hdfs://name-node:8020"));
    propertyList.add(new Property("yarn.resourcemanager.hostname", "resource-manager"));
    propertyList.add(new Property(DremioConfig.LOCAL_WRITE_PATH_STRING, "/data/mydata"));
    propertyList.add(new Property(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs"));
    clusterConfig.setSubPropertyList(propertyList);
    storedCluster.setClusterConfig(clusterConfig);

    return storedCluster;
  }

  @Test
  public void testCompareProps() throws Exception {
    List<Property> propertyList1 = new ArrayList<>();
    propertyList1.add(new Property(FS_DEFAULT_NAME_KEY, "hdfs://name-node:8020").setType(PropertyType.JAVA_PROP));
    propertyList1.add(new Property("yarn.resourcemanager.hostname", "resource-manager"));
    propertyList1.add(new Property(DremioConfig.LOCAL_WRITE_PATH_STRING, "/data/mydata"));
    propertyList1.add(new Property(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs"));

    List<Property> propertyList2 = new ArrayList<>();
    propertyList2.add(new Property(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs"));
    propertyList2.add(new Property(FS_DEFAULT_NAME_KEY, "hdfs://name-node:8020").setType(PropertyType.JAVA_PROP));
    propertyList2.add(new Property(DremioConfig.LOCAL_WRITE_PATH_STRING, "/data/mydata"));
    propertyList2.add(new Property("yarn.resourcemanager.hostname", "resource-manager"));

    assertTrue(service.equals(propertyList1, propertyList2));
    assertEquals(4, propertyList1.size());

  }
}
