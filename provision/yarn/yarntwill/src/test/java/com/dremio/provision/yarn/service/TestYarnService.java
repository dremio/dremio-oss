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
package com.dremio.provision.yarn.service;

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_HOSTNAME;
import static org.apache.twill.api.Configs.Keys.HEAP_RESERVED_MIN_RATIO;
import static org.apache.twill.api.Configs.Keys.JAVA_RESERVED_MEMORY_MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.inject.Provider;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.internal.DefaultTwillRunResources;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.utils.Resources;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import com.dremio.common.nodes.NodeProvider;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterConfig;
import com.dremio.provision.ClusterCreateRequest;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterId;
import com.dremio.provision.ClusterSpec;
import com.dremio.provision.ClusterState;
import com.dremio.provision.ClusterType;
import com.dremio.provision.DistroType;
import com.dremio.provision.DynamicConfig;
import com.dremio.provision.Property;
import com.dremio.provision.resource.ProvisioningResource;
import com.dremio.provision.service.ProvisioningHandlingException;
import com.dremio.provision.service.ProvisioningService;
import com.dremio.provision.service.ProvisioningServiceImpl;
import com.dremio.provision.service.ProvisioningStateListener;
import com.dremio.provision.yarn.DacDaemonYarnApplication;
import com.dremio.provision.yarn.YarnController;
import com.dremio.service.SingletonRegistry;
import com.dremio.test.DremioTest;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Class to test YarnService
 */
public class TestYarnService {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestYarnService.class);

  @Rule
  public final TemporarySystemProperties properties = new TemporarySystemProperties();

  @Test
  public void testStartCluster() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);
    YarnService yarnService = new YarnService(new TestListener(), controller, Mockito.mock(NodeProvider.class));
    Cluster cluster = new Cluster();
    cluster.setState(ClusterState.CREATED);
    cluster.setId(new ClusterId(UUID.randomUUID().toString()));
    ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setClusterSpec(new ClusterSpec(2, 4096, 4096, 2));
    List<Property> propertyList = new ArrayList<>();
    propertyList.add(new Property(FS_DEFAULT_NAME_KEY, "hdfs://name-node:8020"));
    propertyList.add(new Property(RM_HOSTNAME, "resource-manager"));
    propertyList.add(new Property(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs"));
    clusterConfig.setSubPropertyList(propertyList);
    cluster.setClusterConfig(clusterConfig);
    YarnConfiguration yarnConfig = new YarnConfiguration();
    yarnService.updateYarnConfiguration(cluster, yarnConfig);

    assertNotNull(yarnConfig.get(FS_DEFAULT_NAME_KEY));
    assertNotNull(yarnConfig.get(RM_HOSTNAME));
    assertNotNull(yarnConfig.get(DremioConfig.DIST_WRITE_PATH_STRING));

    assertEquals("hdfs://name-node:8020", yarnConfig.get(FS_DEFAULT_NAME_KEY));
    assertEquals("resource-manager", yarnConfig.get(RM_HOSTNAME));
    assertEquals("pdfs:///data/mydata/pdfs", yarnConfig.get(DremioConfig.DIST_WRITE_PATH_STRING));

    TwillController twillController = Mockito.mock(TwillController.class);
    RunId runId = RunIds.generate();
    ResourceReport resourceReport = Mockito.mock(ResourceReport.class);

    when(controller.startCluster(any(YarnConfiguration.class), any(List.class)))
      .thenReturn(twillController);
    when(twillController.getRunId()).thenReturn(runId);
    when(twillController.getResourceReport()).thenReturn(resourceReport);

    ClusterEnriched clusterEnriched = yarnService.startCluster(cluster);
    assertNull(null,clusterEnriched.getRunTimeInfo());

    assertEquals(ClusterState.STARTING, cluster.getState());
    assertNotNull(cluster.getRunId());
    assertEquals(runId.getId(), cluster.getRunId().getId());
  }

  @Test
  public void testDistroDefaults() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);
    YarnService yarnService = new YarnService(new TestListener(), controller, Mockito.mock(NodeProvider.class));
    Cluster cluster = new Cluster();
    cluster.setState(ClusterState.CREATED);
    cluster.setId(new ClusterId(UUID.randomUUID().toString()));
    ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setClusterSpec(new ClusterSpec(2, 4096, 4096, 2));
    clusterConfig.setIsSecure(false);
    clusterConfig.setDistroType(DistroType.MAPR);
    List<Property> propertyList = new ArrayList<>();
    propertyList.add(new Property(FS_DEFAULT_NAME_KEY, "hdfs://name-node:8020"));
    propertyList.add(new Property(RM_HOSTNAME, "resource-manager"));
    propertyList.add(new Property(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs"));
    clusterConfig.setSubPropertyList(propertyList);
    cluster.setClusterConfig(clusterConfig);
    YarnConfiguration yarnConfig = new YarnConfiguration();
    yarnService.updateYarnConfiguration(cluster, yarnConfig);

    assertNotNull(yarnConfig.get(FS_DEFAULT_NAME_KEY));
    assertNotNull(yarnConfig.get(RM_HOSTNAME));
    assertNotNull(yarnConfig.get(DremioConfig.DIST_WRITE_PATH_STRING));

    assertEquals("hdfs://name-node:8020", yarnConfig.get(FS_DEFAULT_NAME_KEY));
    assertEquals("resource-manager", yarnConfig.get(RM_HOSTNAME));
    assertEquals("pdfs:///data/mydata/pdfs", yarnConfig.get(DremioConfig.DIST_WRITE_PATH_STRING));

    assertEquals("/opt/mapr/conf/mapr.login.conf", yarnConfig.get(YarnDefaultsConfigurator.JAVA_LOGIN));
    assertEquals("false", yarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT));
    assertEquals("Client_simple", yarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT_CONFIG));
    assertEquals("com.mapr.security.simplesasl.SimpleSaslProvider", yarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_PROVIDER));
    assertEquals("[\"maprfs:///var/mapr/local/${NM_HOST}/mapred/spill\"]", yarnConfig.get(YarnDefaultsConfigurator
      .SPILL_PATH));

    // MapR security ON
    Cluster myCluster = createCluster();
    myCluster.getClusterConfig().setDistroType(DistroType.MAPR).setIsSecure(true);
    YarnConfiguration myYarnConfig = new YarnConfiguration();
    yarnService.updateYarnConfiguration(myCluster, myYarnConfig);

    assertEquals("/opt/mapr/conf/mapr.login.conf", myYarnConfig.get(YarnDefaultsConfigurator.JAVA_LOGIN));
    assertEquals("false", myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT));
    assertEquals("Client", myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT_CONFIG));
    assertEquals("com.mapr.security.maprsasl.MaprSaslProvider", myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_PROVIDER));
    assertEquals("[\"maprfs:///var/mapr/local/${NM_HOST}/mapred/spill\"]", myYarnConfig.get(YarnDefaultsConfigurator
      .SPILL_PATH));

    // HDP security OFF

  }

  private static final String MAPR_IMPALA_RA_THROTTLE = "MAPR_IMPALA_RA_THROTTLE";
  private static final String MAPR_MAX_RA_STREAMS = "MAPR_MAX_RA_STREAMS";
  private static final String NETTY_MAX_DIRECT_MEMORY = "io.netty.maxDirectMemory";

  @Test
  public void testDistroMapRDefaults() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);
    YarnService yarnService = new YarnService(new TestListener(), controller, Mockito.mock(NodeProvider.class));

    properties.clear(MAPR_IMPALA_RA_THROTTLE);
    properties.clear(MAPR_MAX_RA_STREAMS);

    Cluster myCluster = createCluster();
    myCluster.getClusterConfig().setDistroType(DistroType.MAPR).setIsSecure(true);
    YarnConfiguration myYarnConfig = new YarnConfiguration();
    yarnService.updateYarnConfiguration(myCluster, myYarnConfig);

    assertEquals("/opt/mapr/conf/mapr.login.conf", myYarnConfig.get(YarnDefaultsConfigurator.JAVA_LOGIN));
    assertEquals("false", myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT));
    assertEquals("Client", myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT_CONFIG));
    assertEquals("com.mapr.security.maprsasl.MaprSaslProvider", myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_PROVIDER));
    assertEquals("[\"maprfs:///var/mapr/local/${NM_HOST}/mapred/spill\"]", myYarnConfig.get(YarnDefaultsConfigurator
      .SPILL_PATH));
    assertEquals("0", myYarnConfig.get(NETTY_MAX_DIRECT_MEMORY));
    assertNull(myYarnConfig.get(MAPR_IMPALA_RA_THROTTLE));
    assertNull(myYarnConfig.get(MAPR_MAX_RA_STREAMS));

    Cluster myClusterOff = createCluster();
    myClusterOff.getClusterConfig().setDistroType(DistroType.MAPR).setIsSecure(false);
    YarnConfiguration myYarnConfigOff = new YarnConfiguration();
    yarnService.updateYarnConfiguration(myClusterOff, myYarnConfigOff);

    assertEquals("/opt/mapr/conf/mapr.login.conf", myYarnConfigOff.get(YarnDefaultsConfigurator.JAVA_LOGIN));
    assertEquals("false", myYarnConfigOff.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT));
    assertEquals("Client_simple", myYarnConfigOff.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT_CONFIG));
    assertEquals("com.mapr.security.simplesasl.SimpleSaslProvider", myYarnConfigOff.get(YarnDefaultsConfigurator.ZK_SASL_PROVIDER));
    assertEquals("[\"maprfs:///var/mapr/local/${NM_HOST}/mapred/spill\"]", myYarnConfigOff.get(YarnDefaultsConfigurator
      .SPILL_PATH));
    assertEquals("0", myYarnConfigOff.get(NETTY_MAX_DIRECT_MEMORY));
    assertNull(myYarnConfigOff.get(MAPR_IMPALA_RA_THROTTLE));
    assertNull(myYarnConfigOff.get(MAPR_MAX_RA_STREAMS));
  }

  @Test
  public void testDistroMapRDefaultsWithMaprRAStreams() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);
    YarnService yarnService = new YarnService(new TestListener(), controller, Mockito.mock(NodeProvider.class));

    properties.set("MAPR_IMPALA_RA_THROTTLE", "");
    properties.set("MAPR_MAX_RA_STREAMS", "123");

    Cluster myCluster = createCluster();
    myCluster.getClusterConfig().setDistroType(DistroType.MAPR).setIsSecure(true);
    YarnConfiguration myYarnConfig = new YarnConfiguration();
    yarnService.updateYarnConfiguration(myCluster, myYarnConfig);

    assertEquals("/opt/mapr/conf/mapr.login.conf", myYarnConfig.get(YarnDefaultsConfigurator.JAVA_LOGIN));
    assertEquals("false", myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT));
    assertEquals("Client", myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT_CONFIG));
    assertEquals("com.mapr.security.maprsasl.MaprSaslProvider", myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_PROVIDER));
    assertEquals("[\"maprfs:///var/mapr/local/${NM_HOST}/mapred/spill\"]", myYarnConfig.get(YarnDefaultsConfigurator
      .SPILL_PATH));
    assertEquals("0", myYarnConfig.get(NETTY_MAX_DIRECT_MEMORY));
    assertEquals("", myYarnConfig.get(MAPR_IMPALA_RA_THROTTLE));
    assertEquals("123", myYarnConfig.get(MAPR_MAX_RA_STREAMS));

    Cluster myClusterOff = createCluster();
    myClusterOff.getClusterConfig().setDistroType(DistroType.MAPR).setIsSecure(false);
    YarnConfiguration myYarnConfigOff = new YarnConfiguration();
    yarnService.updateYarnConfiguration(myClusterOff, myYarnConfigOff);

    assertEquals("/opt/mapr/conf/mapr.login.conf", myYarnConfigOff.get(YarnDefaultsConfigurator.JAVA_LOGIN));
    assertEquals("false", myYarnConfigOff.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT));
    assertEquals("Client_simple", myYarnConfigOff.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT_CONFIG));
    assertEquals("com.mapr.security.simplesasl.SimpleSaslProvider", myYarnConfigOff.get(YarnDefaultsConfigurator.ZK_SASL_PROVIDER));
    assertEquals("[\"maprfs:///var/mapr/local/${NM_HOST}/mapred/spill\"]", myYarnConfigOff.get(YarnDefaultsConfigurator
      .SPILL_PATH));
    assertEquals("0", myYarnConfigOff.get(NETTY_MAX_DIRECT_MEMORY));
    assertEquals("", myYarnConfigOff.get(MAPR_IMPALA_RA_THROTTLE));
    assertEquals("123", myYarnConfigOff.get(MAPR_MAX_RA_STREAMS));

  }

  @Test
  public void testDistroHDPDefaults() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);
    YarnService yarnService = new YarnService(new TestListener(), controller, Mockito.mock(NodeProvider.class));

    Cluster myCluster = createCluster();
    myCluster.getClusterConfig().setDistroType(DistroType.HDP).setIsSecure(true);
    YarnConfiguration myYarnConfig = new YarnConfiguration();
    yarnService.updateYarnConfiguration(myCluster, myYarnConfig);

    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.JAVA_LOGIN));
    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT));
    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT_CONFIG));
    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_PROVIDER));
    assertEquals("[\"file:///tmp/dremio/spill\"]", myYarnConfig.get(YarnDefaultsConfigurator.SPILL_PATH));
    assertEquals("0", myYarnConfig.get(NETTY_MAX_DIRECT_MEMORY));

    Cluster myClusterOff = createCluster();
    myClusterOff.getClusterConfig().setDistroType(DistroType.MAPR).setIsSecure(false);
    YarnConfiguration myYarnConfigOff = new YarnConfiguration();
    yarnService.updateYarnConfiguration(myClusterOff, myYarnConfigOff);

    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.JAVA_LOGIN));
    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT));
    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT_CONFIG));
    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_PROVIDER));
    assertEquals("[\"file:///tmp/dremio/spill\"]", myYarnConfig.get(YarnDefaultsConfigurator.SPILL_PATH));
    assertEquals("0", myYarnConfigOff.get(NETTY_MAX_DIRECT_MEMORY));
  }

  @Test
  public void testDistroDefaultsOverwrite() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);
    YarnService yarnService = new YarnService(new TestListener(), controller, Mockito.mock(NodeProvider.class));

    Cluster myCluster = createCluster();
    List<Property> props = myCluster.getClusterConfig().getSubPropertyList();
    props.add(new Property(YarnDefaultsConfigurator.SPILL_PATH, "/abc/bcd"));
    props.add(new Property(YarnDefaultsConfigurator.JAVA_LOGIN, "/abc/bcd/login.conf"));
    myCluster.getClusterConfig().setDistroType(DistroType.HDP).setIsSecure(true);
    YarnConfiguration myYarnConfig = new YarnConfiguration();
    yarnService.updateYarnConfiguration(myCluster, myYarnConfig);

    assertEquals("/abc/bcd/login.conf", myYarnConfig.get(YarnDefaultsConfigurator.JAVA_LOGIN));
    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT));
    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_CLIENT_CONFIG));
    assertNull(myYarnConfig.get(YarnDefaultsConfigurator.ZK_SASL_PROVIDER));
    assertEquals("/abc/bcd", myYarnConfig.get(YarnDefaultsConfigurator.SPILL_PATH));
  }

    @Test
  public void testMemoryLimit() throws Exception {
    Provider provider = Mockito.mock(Provider.class);
    ProvisioningService service = new ProvisioningServiceImpl(DremioConfig.create(), provider, Mockito.mock(NodeProvider.class), null);

    ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setClusterSpec(new ClusterSpec(2, 4096, 2048, 2));

    try {
      service.createCluster(clusterConfig);
      fail("Should throw not enough memory exception");
    } catch(ProvisioningHandlingException phe) {
      // ok
    } catch (Exception e) {
      fail("Exception thrown should be of type: ProvisioningHandlingException");
    }
  }

  @Test
  public void testStartServiceFailure() throws Exception {
    assumeNonMaprProfile();
    try(
      final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    ) {
      SingletonRegistry registry = new SingletonRegistry();
      registry.bind(KVStoreProvider.class, kvstore);
      kvstore.start();
      registry.start();
      ProvisioningService service = new ProvisioningServiceImpl(DremioConfig.create(), registry.provider(KVStoreProvider.class), Mockito.mock(NodeProvider.class), DremioTest
        .CLASSPATH_SCAN_RESULT);
      service.start();
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
      propertyList.add(new Property(RM_HOSTNAME, "resource-manager"));
      propertyList.add(new Property(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs"));
      clusterConfig.setSubPropertyList(propertyList);

      try {
        service.createCluster(clusterConfig);
        fail("Should not be able to create cluster");
      } catch (Exception phe) {
        // ok
      }
      assertEquals(false, service.getClustersInfo().iterator().hasNext());
    }
  }

  @Test
  public void testMemorySplit() throws Exception {
    assumeNonMaprProfile();
    try (
      final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    ) {
      SingletonRegistry registry = new SingletonRegistry();
      registry.bind(KVStoreProvider.class, kvstore);
      kvstore.start();
      registry.start();
      ProvisioningService service = Mockito.spy(new ProvisioningServiceImpl(
          DremioConfig.create(),
          registry.provider(KVStoreProvider.class),
          Mockito.mock(NodeProvider.class),
          DremioTest.CLASSPATH_SCAN_RESULT));
      service.start();
      ProvisioningResource resource = new ProvisioningResource(service);
      ClusterCreateRequest createRequest = new ClusterCreateRequest();
      createRequest.setClusterType(ClusterType.YARN);
      createRequest.setDynamicConfig(new DynamicConfig(2));
      createRequest.setVirtualCoreCount(2);
      createRequest.setMemoryMB(8192);
      List<Property> props = new ArrayList<>();
      props.add(new Property(ProvisioningService.YARN_HEAP_SIZE_MB_PROPERTY, "2048"));
      props.add(new Property(FS_DEFAULT_NAME_KEY, "hdfs://name-node:8020"));
      props.add(new Property("yarn.resourcemanager.hostname", "resource-manager"));

      createRequest.setSubPropertyList(props);
      doReturn(new ClusterEnriched()).when(service).startCluster(any(ClusterId.class));
      try {
        resource.createCluster(createRequest);
      } catch (NullPointerException e) {
        // as we did not fill out ClusterEnriched it will lead to NPE
        // but it is not subject of the test here
      }
      KVStore<ClusterId, Cluster> store =
        registry.provider(KVStoreProvider.class).get().getStore(ProvisioningServiceImpl.ProvisioningStoreCreator.class);
      Iterable<Map.Entry<ClusterId, Cluster>> entries = store.find();
      assertTrue(entries.iterator().hasNext());
      int count = 0;
      for (Map.Entry<ClusterId, Cluster> entry : entries) {
        Cluster clusterEntry = entry.getValue();
        int offHeap = clusterEntry.getClusterConfig().getClusterSpec().getMemoryMBOffHeap();
        assertEquals(8192-2048, offHeap);
        int onHeap = clusterEntry.getClusterConfig().getClusterSpec().getMemoryMBOnHeap();
        assertEquals(2048, onHeap);
        count++;
      }
      assertEquals(1, count);
    }
  }

  @Test
  public void testUpdater() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);

    final TestListener listener = new TestListener();
    YarnService yarnService = new YarnService(listener, controller, Mockito.mock(NodeProvider.class));

    final Cluster cluster = createCluster();

    final TwillController twillController = Mockito.mock(TwillController.class);
    RunId runId = RunIds.generate();
    ResourceReport resourceReport = Mockito.mock(ResourceReport.class);

    when(controller.startCluster(any(YarnConfiguration.class), any(List.class)))
      .thenReturn(twillController);
    when(twillController.getRunId()).thenReturn(runId);
    when(twillController.getResourceReport()).thenReturn(resourceReport);

    final Future<? extends ServiceController> futureController1 = Executors.newSingleThreadExecutor()
      .submit(new Runnable() {
        @Override
        public void run() {

        }
      }, twillController
    );

    doAnswer(new Answer<Future<? extends ServiceController>>() {
      @Override
      public Future<? extends ServiceController> answer(InvocationOnMock invocationOnMock) throws Throwable {
        return futureController1;
      }
    }).when(twillController).terminate();


    final YarnService.OnRunningRunnable onRunning = yarnService.new OnRunningRunnable(cluster);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        try {
          Thread.sleep(100);
          onRunning.run();
          System.out.println("run");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return null;
      }
    }).when(twillController).onRunning(any(Runnable.class), any(Executor.class));

    final YarnService.OnTerminatingRunnable onTerminating =
      yarnService.new OnTerminatingRunnable(cluster, twillController);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        try {
          Thread.sleep(1000);
          onTerminating.run();
          System.out.println("terminate");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return null;
      }
    }).when(twillController).onTerminated(any(Runnable.class), any(Executor.class));

    cluster.setState(ClusterState.CREATED);
    ClusterEnriched clusterEnriched = yarnService.startCluster(cluster);
    assertEquals(ClusterState.STOPPED, clusterEnriched.getCluster().getState());
    assertNull(cluster.getRunId());
    assertTrue(yarnService.terminatingThreads.isEmpty());
  }

  @Test
  public void testFailedUpdater() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);

    final TestListener listener = new TestListener();
    YarnService yarnService = new YarnService(listener, controller, Mockito.mock(NodeProvider.class));

    final Cluster cluster = createCluster();
    cluster.setDesiredState(ClusterState.RUNNING);

    final TwillController twillController = Mockito.mock(TwillController.class);
    RunId runId = RunIds.generate();
    ResourceReport resourceReport = Mockito.mock(ResourceReport.class);

    when(controller.startCluster(any(YarnConfiguration.class), any(List.class)))
      .thenReturn(twillController);
    when(twillController.getRunId()).thenReturn(runId);
    when(twillController.getResourceReport()).thenReturn(resourceReport);

    final Future<? extends ServiceController> futureController1 = Executors.newSingleThreadExecutor()
      .submit(new Runnable() {
                @Override
                public void run() {

                }
              }, twillController
      );

    doAnswer(new Answer<Future<? extends ServiceController>>() {
      @Override
      public Future<? extends ServiceController> answer(InvocationOnMock invocationOnMock) throws Throwable {
        return futureController1;
      }
    }).when(twillController).terminate();


    final YarnService.OnRunningRunnable onRunning = yarnService.new OnRunningRunnable(cluster);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        try {
          Thread.sleep(100);
          onRunning.run();
          System.out.println("run");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return null;
      }
    }).when(twillController).onRunning(any(Runnable.class), any(Executor.class));

    final YarnService.OnTerminatingRunnable onTerminatingNoOP =
      yarnService.new OnTerminatingRunnable(cluster, twillController);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        try {
          Thread.sleep(1000);
          cluster.setState(ClusterState.FAILED);
          cluster.setError("My error");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return null;
      }
    }).when(twillController).onTerminated(any(Runnable.class), any(Executor.class));

    ClusterEnriched clusterEnriched = yarnService.startCluster(cluster);
    assertEquals(ClusterState.FAILED, clusterEnriched.getCluster().getState());
    assertEquals("My error", clusterEnriched.getCluster().getError());
    assertEquals(ClusterState.RUNNING, clusterEnriched.getCluster().getDesiredState());
  }

  @Test
  public void testProcessRestart() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);

    final TestListener listener = new TestListener();
    YarnService yarnService = new YarnService(listener, controller, Mockito.mock(NodeProvider.class));
    TwillRunnerService twillRunnerService = Mockito.mock(TwillRunnerService.class);

    final Cluster cluster = createCluster();
    cluster.setDesiredState(ClusterState.RUNNING);

    final TwillController twillController = Mockito.mock(TwillController.class);
    RunId runId = RunIds.generate();
    ResourceReport resourceReport = Mockito.mock(ResourceReport.class);

    when(controller.startCluster(any(YarnConfiguration.class), any(List.class)))
      .thenReturn(twillController);
    when(twillController.getRunId()).thenReturn(runId);
    when(twillController.getResourceReport()).thenReturn(resourceReport);
    when(controller.getTwillService(cluster.getId())).thenReturn(twillRunnerService);
    when(twillRunnerService.lookup(DacDaemonYarnApplication.YARN_APPLICATION_NAME_DEFAULT, runId)).thenReturn(twillController);


    final YarnService.OnRunningRunnable onRunning = yarnService.new OnRunningRunnable(cluster);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        try {
          Thread.sleep(100);
          onRunning.run();
          System.out.println("run");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return null;
      }
    }).when(twillController).onRunning(any(Runnable.class), any(Executor.class));

    ClusterEnriched clusterEnriched = yarnService.startCluster(cluster);

    assertEquals(ClusterState.RUNNING, clusterEnriched.getCluster().getState());
    assertEquals(1, yarnService.terminatingThreads.size());
    com.dremio.provision.RunId dRunId = yarnService.terminatingThreads.keySet().iterator().next();
    assertEquals(runId.getId(), dRunId.getId());

    // supposedly process stopped and restarted before any termination happened
    YarnService yarnServiceNew = new YarnService(listener, controller, Mockito.mock(NodeProvider.class));
    assertTrue(yarnServiceNew.terminatingThreads.isEmpty());

    final Future<? extends ServiceController> futureController1 = Executors.newSingleThreadExecutor()
      .submit(new Runnable() {
                @Override
                public void run() {
                  try {
                    // let the test run
                    Thread.sleep(1000);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }
              }, twillController
      );

    doAnswer(new Answer<Future<? extends ServiceController>>() {
      @Override
      public Future<? extends ServiceController> answer(InvocationOnMock invocationOnMock) throws Throwable {
        return futureController1;
      }
    }).when(twillController).terminate();

    final YarnService.OnTerminatingRunnable onTerminating =
      yarnService.new OnTerminatingRunnable(cluster, twillController);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        try {
          Thread.sleep(10);
          onTerminating.run();
          System.out.println("terminate");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return null;
      }
    }).when(twillController).onTerminated(any(Runnable.class), any(Executor.class));

    yarnServiceNew.stopCluster(cluster);
    assertEquals(1, yarnServiceNew.terminatingThreads.size());
    dRunId = yarnServiceNew.terminatingThreads.keySet().iterator().next();
    assertEquals(runId.getId(), dRunId.getId());
    assertEquals(ClusterState.STOPPED, cluster.getState());
  }

  @Test
  public void testStopCluster() throws Exception {
    assumeNonMaprProfile();

    Cluster myCluster = createCluster();
    myCluster.setState(ClusterState.RUNNING);

    YarnController controller = Mockito.mock(YarnController.class);
    YarnService yarnService = new YarnService(new TestListener(), controller, Mockito.mock(NodeProvider.class));

    TwillController twillController = Mockito.mock(TwillController.class);
    RunId runId = RunIds.generate();

    when(controller.startCluster(any(YarnConfiguration.class), eq(myCluster.getClusterConfig().getSubPropertyList())))
      .thenReturn(twillController);
    when(twillController.getRunId()).thenReturn(runId);

    myCluster.setRunId(new com.dremio.provision.RunId(runId.getId()));
    yarnService.stopCluster(myCluster);

    assertEquals(ClusterState.STOPPED, myCluster.getState());
  }

  @Test
  public void testMemoryOnOffHeapRatio() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);
    YarnService yarnService = new YarnService(new TestListener(), controller, Mockito.mock(NodeProvider.class));
    Cluster cluster = new Cluster();
    cluster.setState(ClusterState.CREATED);
    cluster.setId(new ClusterId(UUID.randomUUID().toString()));
    ClusterConfig clusterConfig = new ClusterConfig();
    List<Property> propertyList = new ArrayList<>();
    propertyList.add(new Property(FS_DEFAULT_NAME_KEY, "hdfs://name-node:8020"));
    propertyList.add(new Property(RM_HOSTNAME, "resource-manager"));
    propertyList.add(new Property(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs"));
    RunId runId = RunIds.generate();
    clusterConfig.setSubPropertyList(propertyList);
    cluster.setClusterConfig(clusterConfig);
    cluster.setRunId(new com.dremio.provision.RunId(runId.toString()));
    YarnConfiguration yarnConfig = new YarnConfiguration();

    List<ClusterSpec> specs = Lists.asList(new ClusterSpec(2, 4096, 96000, 2),
      new ClusterSpec[] {new ClusterSpec(2, 1234, 96023, 2),
      new ClusterSpec(2, 4096, 8192, 2),
      new ClusterSpec(2, 8192, 72000, 2)});

    for (ClusterSpec spec : specs) {
      clusterConfig.setClusterSpec(spec);
      int onHeapMemory = spec.getMemoryMBOnHeap();
      int offHeapMemory = spec.getMemoryMBOffHeap();
      yarnService.updateYarnConfiguration(cluster, yarnConfig);
      double ratio = ((double) onHeapMemory) / (offHeapMemory + onHeapMemory);
      if (ratio < 0.1) {
        assertEquals(ratio, yarnConfig.getDouble(HEAP_RESERVED_MIN_RATIO, 0), 10e-6);
      } else {
        assertEquals(0.1, yarnConfig.getDouble(HEAP_RESERVED_MIN_RATIO, 0), 10e-9);
      }
      assertEquals(onHeapMemory,
        Resources.computeMaxHeapSize(offHeapMemory + onHeapMemory, offHeapMemory, yarnConfig.getDouble(HEAP_RESERVED_MIN_RATIO, 0)));
      assertEquals(onHeapMemory, yarnConfig.getInt(DacDaemonYarnApplication.YARN_MEMORY_ON_HEAP, 0));
      assertEquals(offHeapMemory, yarnConfig.getInt(DacDaemonYarnApplication.YARN_MEMORY_OFF_HEAP, 0));
      assertEquals(offHeapMemory, yarnConfig.getInt(JAVA_RESERVED_MEMORY_MB, 0));
    }
  }

  @Test
  public void testGetClusterInfo() throws Exception {
    assumeNonMaprProfile();

    YarnController controller = Mockito.mock(YarnController.class);
    YarnService yarnService = new YarnService(new TestListener(), controller, Mockito.mock(NodeProvider.class));
    Cluster cluster = new Cluster();
    cluster.setState(ClusterState.CREATED);
    cluster.setId(new ClusterId(UUID.randomUUID().toString()));
    ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setClusterSpec(new ClusterSpec(2, 4096, 4096, 2));
    List<Property> propertyList = new ArrayList<>();
    propertyList.add(new Property(FS_DEFAULT_NAME_KEY, "hdfs://name-node:8020"));
    propertyList.add(new Property(RM_HOSTNAME, "resource-manager"));
    propertyList.add(new Property(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs"));
    RunId runId = RunIds.generate();
    clusterConfig.setSubPropertyList(propertyList);
    cluster.setClusterConfig(clusterConfig);
    cluster.setRunId(new com.dremio.provision.RunId(runId.toString()));
    YarnConfiguration yarnConfig = new YarnConfiguration();
    yarnService.updateYarnConfiguration(cluster, yarnConfig);

    TwillController twillController = Mockito.mock(TwillController.class);
    ResourceReport resourceReport = Mockito.mock(ResourceReport.class);
    TwillRunnerService twillRunnerService = Mockito.mock(TwillRunnerService.class);

    List<TwillRunResources> resources = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      resources.add(createResource(i));
    }

    when(controller.startCluster(any(YarnConfiguration.class), eq(cluster.getClusterConfig().getSubPropertyList())))
      .thenReturn(twillController);
    when(controller.getTwillService(cluster.getId())).thenReturn(twillRunnerService);
    when(twillController.getRunId()).thenReturn(runId);
    when(resourceReport.getRunnableResources(DacDaemonYarnApplication.YARN_RUNNABLE_NAME)).thenReturn(resources);
    when(twillRunnerService.lookup(DacDaemonYarnApplication.YARN_APPLICATION_NAME_DEFAULT, runId)).thenReturn(twillController);
    when(twillController.getResourceReport()).thenReturn(resourceReport);

    ClusterEnriched clusterEnriched = yarnService.getClusterInfo(cluster);

    assertNotNull(clusterEnriched.getRunTimeInfo());
    assertEquals(8, clusterEnriched.getRunTimeInfo().getDecommissioningCount().intValue());
  }

  @Test
  public void testURI() throws Exception {
    URI defaultURI = new URI("maprfs", "", "/", null, null);
    assertTrue("maprfs:///".equalsIgnoreCase(defaultURI.toString()));
  }

  @Test
  public void testTimelineServiceIsDisabledByDefault(){
    YarnController controller = Mockito.mock(YarnController.class);
    YarnService yarnService = new YarnService(new TestListener(), controller, Mockito.mock(NodeProvider.class));
    Cluster cluster = new Cluster();
    cluster.setState(ClusterState.CREATED);
    cluster.setId(new ClusterId(UUID.randomUUID().toString()));
    ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setClusterSpec(new ClusterSpec(2, 4096, 4096, 2));
    List<Property> propertyList = new ArrayList<>();
    clusterConfig.setSubPropertyList(propertyList);
    RunId runId = RunIds.generate();
    cluster.setClusterConfig(clusterConfig);
    cluster.setRunId(new com.dremio.provision.RunId(runId.toString()));
    YarnConfiguration yarnConfig = new YarnConfiguration();

    // Test that it is false by default
    yarnConfig.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    yarnService.updateYarnConfiguration(cluster, yarnConfig);
    assertFalse(yarnConfig.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true));

    // Test that it can be overwritten
    yarnConfig.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    cluster.getClusterConfig().getSubPropertyList().add(new Property(YarnConfiguration.TIMELINE_SERVICE_ENABLED, "true"));
    yarnService.updateYarnConfiguration(cluster, yarnConfig);
    assertTrue(yarnConfig.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false));
  }

  private DefaultTwillRunResources createResource(int seed) {
    DefaultTwillRunResources resource = new DefaultTwillRunResources(seed,
      "container_e04_1487533082952_0033_01_00000"+seed,
      2, 8192, "container-host", 0, ImmutableMap.of(Logger.ROOT_LOGGER_NAME,LogEntry.Level.INFO));

    return resource;
  }

  private static class TestListener implements ProvisioningStateListener {

    TestListener() {

    }
    @Override
    public void started(Cluster cluster) throws ProvisioningHandlingException {
      // noop
    }

    @Override
    public void stopped(Cluster cluster) throws ProvisioningHandlingException {
      // noop
    }

    @Override
    public void resized(Cluster cluster) throws ProvisioningHandlingException {
      // noop
    }
  }

  private Cluster createCluster() {
    final Cluster cluster = new Cluster();
    cluster.setState(ClusterState.CREATED);
    cluster.setId(new ClusterId(UUID.randomUUID().toString()));
    ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setClusterSpec(new ClusterSpec(2, 4096, 4096, 2));
    List<Property> propertyList = new ArrayList<>();
    propertyList.add(new Property(FS_DEFAULT_NAME_KEY, "hdfs://name-node:8020"));
    propertyList.add(new Property(RM_HOSTNAME, "resource-manager"));
    propertyList.add(new Property(DremioConfig.DIST_WRITE_PATH_STRING, "pdfs:///data/mydata/pdfs"));
    clusterConfig.setSubPropertyList(propertyList);
    cluster.setClusterConfig(clusterConfig);

    return cluster;
  }
}
