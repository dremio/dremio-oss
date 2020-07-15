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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterConfig;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterId;
import com.dremio.provision.ClusterState;
import com.dremio.provision.ClusterType;
import com.dremio.test.DremioTest;

/**
 * Test ProvisioningServiceImpl
 */
public class TestProvisioningServiceImpl extends DremioTest {

  @Test
  public void testStartWithNoProviders() throws Exception {
    // SETUP
    LegacyKVStore<ClusterId, Cluster> kvStore = mock(LegacyKVStore.class);
    when(kvStore.find())
      .thenReturn(Collections.emptyList());

    LegacyKVStoreProvider legacyKVStore = mock(LegacyKVStoreProvider.class);
    when(legacyKVStore.getStore(ProvisioningServiceImpl.ProvisioningStoreCreator.class))
      .thenReturn(kvStore);

    ProvisioningServiceImpl subject =
      spy(new ProvisioningServiceImpl(
        Collections.emptyMap(),
        () -> legacyKVStore
      ));

    //TEST
    subject.start();

    //ASSERT
    verify(subject).syncClusters();
  }

  @Test
  public void testStartWithProviderThatDoesNotHaveDefaultCluster() throws Exception {
    // SETUP
    LegacyKVStore<ClusterId, Cluster> kvStore = mock(LegacyKVStore.class);
    when(kvStore.find())
      .thenReturn(Collections.emptyList());

    LegacyKVStoreProvider legacyKVStore = mock(LegacyKVStoreProvider.class);
    when(legacyKVStore.getStore(ProvisioningServiceImpl.ProvisioningStoreCreator.class))
      .thenReturn(kvStore);

    ProvisioningServiceDelegate provisioningServiceDelegate = mock(ProvisioningServiceDelegate.class);

    ProvisioningServiceImpl subject =
      spy(new ProvisioningServiceImpl(
        Collections.singletonMap(ClusterType.values()[0], provisioningServiceDelegate),
        () -> legacyKVStore
      ));

    //TEST
    subject.start();

    //ASSERT
    // Start the delegator then check for a default cluster
    InOrder order = inOrder(provisioningServiceDelegate,subject);
    order.verify(provisioningServiceDelegate).start();
    order.verify(provisioningServiceDelegate).defaultCluster();
    order.verify(subject).syncClusters();
    order.verifyNoMoreInteractions();
  }

  @Test
  public void testStartWithProviderWithDefaultCluster() throws Exception {
    // SETUP
    ClusterType clusterType = ClusterType.values()[0];
    ClusterConfig clusterConfig = new ClusterConfig(clusterType)
      .setName("ClusterConfigName");
    Cluster cluster = new Cluster()
      .setClusterConfig(clusterConfig);
    Cluster cluster2 = new Cluster()
      .setClusterConfig(clusterConfig);

    LegacyKVStore<ClusterId, Cluster> kvStore = mock(LegacyKVStore.class);
    when(kvStore.get(any(ClusterId.class)))
      .thenReturn(cluster);
    when(kvStore.find())
      .thenReturn(Collections.emptyList());

    LegacyKVStoreProvider legacyKVStore = mock(LegacyKVStoreProvider.class);
    when(legacyKVStore.getStore(ProvisioningServiceImpl.ProvisioningStoreCreator.class))
      .thenReturn(kvStore);


    ProvisioningServiceDelegate provisioningServiceDelegate = mock(ProvisioningServiceDelegate.class);
    when(provisioningServiceDelegate.defaultCluster())
      .thenReturn(clusterConfig);
    when(provisioningServiceDelegate.startCluster(cluster))
      .thenReturn(new ClusterEnriched(cluster2));

    ProvisioningServiceImpl subject =
      spy(
        new ProvisioningServiceImpl(
          Collections.singletonMap(clusterType, provisioningServiceDelegate),
          () -> legacyKVStore
        ));

    //TEST
    subject.start();

    //ASSERT
    ArgumentCaptor<Cluster> clusterCapture = ArgumentCaptor.forClass(Cluster.class);
    ArgumentCaptor<ClusterId> clusterIdCapture = ArgumentCaptor.forClass(ClusterId.class);
    ArgumentCaptor<ClusterId> clusterIdCapture2 = ArgumentCaptor.forClass(ClusterId.class);

    InOrder order = inOrder(provisioningServiceDelegate, kvStore, subject);
    order.verify(provisioningServiceDelegate).start();
    order.verify(provisioningServiceDelegate).defaultCluster();

    // creating new cluster and starting it
    order.verify(kvStore).put(clusterIdCapture.capture(), clusterCapture.capture());
    order.verify(subject).startCluster(clusterIdCapture2.capture());
    order.verify(kvStore).get(Mockito.any(ClusterId.class));

    order.verify(subject).syncClusters();

    Assert.assertEquals(clusterIdCapture.getValue().getId(), clusterIdCapture2.getValue().getId());
    Assert.assertEquals(clusterConfig,clusterCapture.getValue().getClusterConfig());
    Assert.assertEquals(ClusterState.RUNNING, clusterCapture.getValue().getDesiredState());
  }
}
