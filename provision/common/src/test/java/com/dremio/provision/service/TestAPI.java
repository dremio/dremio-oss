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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.nodes.NodeProvider;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterConfig;
import com.dremio.provision.ClusterDesiredState;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterId;
import com.dremio.provision.ClusterModifyRequest;
import com.dremio.provision.ClusterSpec;
import com.dremio.provision.ClusterState;
import com.dremio.provision.ClusterType;
import com.dremio.provision.DistroType;
import com.dremio.provision.DynamicConfig;
import com.dremio.provision.Property;
import com.dremio.provision.PropertyType;
import com.dremio.provision.resource.ProvisioningResource;
import com.dremio.service.SingletonRegistry;
import com.dremio.test.DremioTest;

/**
 * To test ProvisionResource APIs
 */
public class TestAPI {

  private static ProvisioningResource resource;
  private static final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true,
    false);
  private static SingletonRegistry registry = new SingletonRegistry();
  private static ProvisioningServiceImpl service;

  @BeforeClass
  public static void before() throws Exception {
      registry.bind(KVStoreProvider.class, kvstore);
      kvstore.start();
      registry.start();
      service = Mockito.spy(new ProvisioningServiceImpl(registry.provider(KVStoreProvider.class), Mockito.mock(NodeProvider.class),
        DremioTest.CLASSPATH_SCAN_RESULT));
      service.start();
      resource = new ProvisioningResource(service, null);
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

    KVStore<ClusterId, Cluster> store =
      registry.provider(KVStoreProvider.class).get().getStore(ProvisioningServiceImpl.ProvisioningStoreCreator.class);

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
      assertTrue(0 == clusterEntry.getClusterConfig().getVersion());
      count++;
    }
    assertEquals(1, count);
    clusterConfig.setVersion(0L);
    service.modifyCluster(clusterId, ClusterState.STOPPED, clusterConfig);
    Cluster clusterModified = store.get(clusterId);
    assertNotNull(clusterModified);
    assertEquals(2, clusterModified.getClusterConfig().getVersion().intValue());
    assertEquals(ClusterState.STOPPED, clusterModified.getDesiredState());
    clusterConfig.setVersion(3L);
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
    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(11L);

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
    long [] versions = new long [] {127,128,129};
    for (long version : versions) {
      Cluster storedCluster = clusterCreateHelper();
      storedCluster.getClusterConfig().setVersion(version);
      // test version mismatch
      ClusterModifyRequest request = new ClusterModifyRequest();
      request.setId(storedCluster.getId().getId());
      request.setVersion(version);

      ClusterConfig config = resource.toClusterConfig(request);
      try {
        final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
        service.toAction(storedCluster, modifiedCluster);
      } catch (ConcurrentModificationException e) {
        fail("Version mismatch - request Version: " + request.getVersion() +
          ", storedVersion: " + storedCluster.getClusterConfig().getVersion());
      }
    }
  }

  @Test
  public void testVersionNull() throws Exception {
    Cluster storedCluster = clusterCreateHelper();
    storedCluster.getClusterConfig().setVersion(12L);
    // test version not set
    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());

    ClusterConfig config = resource.toClusterConfig(request);
    try {
      final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
      service.toAction(storedCluster, modifiedCluster);
      fail();
    } catch (NullPointerException e) {
      // OK
    }
  }

  @Test
  public void testPropertyType() throws Exception {
    ProvisioningServiceDelegate provServiceDelegate = Mockito.mock(ProvisioningServiceDelegate.class);
    service.getConcreteServices().put(ClusterType.YARN, provServiceDelegate);

    Cluster storedCluster = clusterCreateHelper();
    storedCluster.getClusterConfig().setVersion(null);

    doReturn(new ClusterEnriched()).when(provServiceDelegate).startCluster(any(Cluster.class));
    doReturn(new ClusterEnriched()).when(provServiceDelegate).startCluster(any(Cluster.class));
    doNothing().when(provServiceDelegate).stopCluster(any(Cluster.class));

    KVStore<ClusterId, Cluster> store =
      registry.provider(KVStoreProvider.class).get().getStore(ProvisioningServiceImpl.ProvisioningStoreCreator.class);

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

    ClusterModifyRequest request = new ClusterModifyRequest();
    // test DELETED state
    storedCluster.setDesiredState(ClusterState.DELETED);
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);
    ClusterConfig config = resource.toClusterConfig(request);

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

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

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

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.RUNNING);
    request.setDynamicConfig(new DynamicConfig(2));
    request.setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList());
    ClusterConfig config = resource.toClusterConfig(request);

    final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster);
    assertEquals(ProvisioningServiceImpl.Action.NONE, action);
  }

  @Test
  public void testResize() throws Exception {
    // test action RESIZE
    Cluster storedCluster = clusterCreateHelper();

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.RUNNING);
    request.setDynamicConfig(new DynamicConfig(3));
    request.setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList());
    ClusterConfig config = resource.toClusterConfig(request);

    final Cluster modifiedCluster = service.toCluster(config, null, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster);
    assertEquals(ProvisioningServiceImpl.Action.RESIZE, action);
  }

  @Test
  public void testStart() throws Exception {
    // test action START
    Cluster storedCluster = clusterCreateHelper();

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.STOPPED);
    request.setDynamicConfig(new DynamicConfig(2));
    request.setDesiredState(ClusterDesiredState.RUNNING);
    request.setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList());
    ClusterConfig config = resource.toClusterConfig(request);

    final Cluster modifiedCluster2 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster2);
    assertEquals(ProvisioningServiceImpl.Action.START, action);
  }

  @Test
  public void testStop() throws Exception {
    // test action STOP
    Cluster storedCluster = clusterCreateHelper();

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.RUNNING);
    request.setDynamicConfig(new DynamicConfig(2));
    request.setDesiredState(ClusterDesiredState.STOPPED);
    request.setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList());
    ClusterConfig config = resource.toClusterConfig(request);

    final Cluster modifiedCluster3 = service.toCluster(config, ClusterState.STOPPED, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster3);
    assertEquals(ProvisioningServiceImpl.Action.STOP, action);
  }

  @Test
  public void testRestartCoresChanges() throws Exception {
    // test action RESTART
    Cluster storedCluster = clusterCreateHelper();

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.RUNNING);
    request.setDynamicConfig(new DynamicConfig(2));
    request.setVirtualCoreCount(4);
    request.setDesiredState(ClusterDesiredState.RUNNING);
    request.setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList());
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

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.RUNNING);
    request.setDynamicConfig(new DynamicConfig(2));
    request.setMemoryMB(9126);
    request.setDesiredState(ClusterDesiredState.RUNNING);
    request.setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList());
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

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.RUNNING);
    request.setDynamicConfig(new DynamicConfig(2));
    //request.setMemoryMB(9126);
    request.setDesiredState(ClusterDesiredState.RUNNING);
    List<Property> newList = new ArrayList<>(storedCluster.getClusterConfig().getSubPropertyList());
    newList.add(new Property(DremioConfig.YARN_HEAP_SIZE, "2096"));
    request.setSubPropertyList(newList);
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

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.RUNNING);
    request.setDynamicConfig(new DynamicConfig(2));
    request.setMemoryMB(9126);
    request.setDesiredState(ClusterDesiredState.RUNNING);
    List<Property> newList = new ArrayList<>(storedCluster.getClusterConfig().getSubPropertyList());
    newList.add(new Property(DremioConfig.YARN_HEAP_SIZE, "2096"));
    request.setSubPropertyList(newList);
    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster4 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster4);
    assertTrue(2096 == modifiedCluster4.getClusterConfig().getClusterSpec().getMemoryMBOnHeap());
    assertTrue((9126-2096) == modifiedCluster4.getClusterConfig().getClusterSpec().getMemoryMBOffHeap());
    assertEquals(ProvisioningServiceImpl.Action.RESTART, action);
  }


  @Test
  public void testRestartProperties() throws Exception {
    // test action RESTART
    Cluster storedCluster = clusterCreateHelper();

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.RUNNING);
    request.setDynamicConfig(new DynamicConfig(2));
    request.setVirtualCoreCount(2);
    request.setDesiredState(ClusterDesiredState.RUNNING);
    request.setSubPropertyList(new ArrayList<Property>());
    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster5 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster5);
    assertEquals(ProvisioningServiceImpl.Action.RESTART, action);
  }

  @Test
  public void testRestartNoChangeProperties() throws Exception {
    // test action RESTART
    Cluster storedCluster = clusterCreateHelper();

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.RUNNING);
    request.setDynamicConfig(new DynamicConfig(2));
    request.setVirtualCoreCount(2);
    request.setDesiredState(ClusterDesiredState.RUNNING);
    request.setSubPropertyList(null);
    ClusterConfig config = resource.toClusterConfig(request);
    final Cluster modifiedCluster5 = service.toCluster(config, ClusterState.RUNNING, storedCluster);
    ProvisioningServiceImpl.Action action = service.toAction(storedCluster, modifiedCluster5);
    assertEquals(ProvisioningServiceImpl.Action.NONE, action);
  }

  @Test
  public void testDelete() throws Exception {
    // test action DELETE
    Cluster storedCluster = clusterCreateHelper();

    ClusterModifyRequest request = new ClusterModifyRequest();
    request.setId(storedCluster.getId().getId());
    request.setVersion(12L);

    storedCluster.setState(ClusterState.RUNNING);
    request.setDynamicConfig(new DynamicConfig(2));
    request.setVirtualCoreCount(2);
    request.setDesiredState(ClusterDesiredState.DELETED);
    request.setSubPropertyList(storedCluster.getClusterConfig().getSubPropertyList());
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
    clusterConfig.setVersion(12L);
    clusterConfig.setClusterType(ClusterType.YARN);
    clusterConfig.setClusterSpec(new ClusterSpec(2, 4096, 4096, 2));
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
