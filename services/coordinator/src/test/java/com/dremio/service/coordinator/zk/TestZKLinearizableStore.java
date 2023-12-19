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
package com.dremio.service.coordinator.zk;

import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.CREATE_EPHEMERAL;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.CREATE_EPHEMERAL_SEQUENTIAL;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.CREATE_PERSISTENT;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.DELETE;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.SET_DATA;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.PathCommand;
import static com.dremio.test.DremioTest.DEFAULT_SABOT_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.dremio.io.file.Path;
import com.dremio.service.coordinator.LinearizableHierarchicalStore;
import com.dremio.service.coordinator.exceptions.PathExistsException;
import com.dremio.service.coordinator.exceptions.PathMissingException;
import com.dremio.test.zookeeper.ZkTestServerRule;

public class TestZKLinearizableStore {
  private static final String TEST_CLUSTER_FORMAT = "%s/dremio/test/test-clustered";

  @Rule
  public final ZkTestServerRule zooKeeperServer = new ZkTestServerRule();
  @Rule
  public Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);
  private ZKClusterCoordinator zkCoordinator1;
  private ZKClusterCoordinator zkCoordinator2;

  @Before
  public void setup() throws Exception {
    zkCoordinator1 = new ZKClusterCoordinator(DEFAULT_SABOT_CONFIG,
      String.format(TEST_CLUSTER_FORMAT, zooKeeperServer.getConnectionString()));
    zkCoordinator2 = new ZKClusterCoordinator(DEFAULT_SABOT_CONFIG,
      String.format(TEST_CLUSTER_FORMAT, zooKeeperServer.getConnectionString()));
    zkCoordinator1.start();
    zkCoordinator2.start();
  }

  @After
  public void tearDown() throws Exception {
    zkCoordinator1.close();
    zkCoordinator2.close();
  }

  @Test
  public void testPersistentNodeWithoutData() throws Exception {
    final LinearizableHierarchicalStore store = zkCoordinator1.getHierarchicalStore();
    final String path = Path.SEPARATOR + "cleanerPath" + Path.SEPARATOR + "12345";
    store.executeSingle(new PathCommand(CREATE_PERSISTENT, path));
    assertTrue(store.checkExists(path));
    assertNull(store.getData(path));
    try {
      zkCoordinator1.getHierarchicalStore().executeSingle(new PathCommand(CREATE_PERSISTENT, path));
      fail("Expected Path exists exception");
    } catch (PathExistsException e) {
      assertThat(e.getMessage()).contains(path);
    }
  }

  @Test
  public void testPersistentNodeWithData() throws Exception {
    final LinearizableHierarchicalStore store = zkCoordinator1.getHierarchicalStore();
    final String path = Path.SEPARATOR + "cleanerPath" + Path.SEPARATOR + "123456";
    final byte[] data = path.getBytes();
    store.executeSingle(new PathCommand(CREATE_PERSISTENT, path, data));
    assertTrue(store.checkExists(path));
    byte[] returned = store.getData(path);
    assertNotNull(returned);
    assertEquals(new String(returned), path);
    returned = zkCoordinator2.getHierarchicalStore().getData(path);
    assertEquals(new String(returned), path);
  }

  @Test
  public void testPersistentNodeWithSetData() throws Exception {
    final LinearizableHierarchicalStore store = zkCoordinator1.getHierarchicalStore();
    final String path = Path.SEPARATOR + "cleanerPath" + Path.SEPARATOR + "1234567";
    store.executeSingle(new PathCommand(CREATE_PERSISTENT, path));
    assertTrue(store.checkExists(path));
    assertNull(store.getData(path));
    final byte[] data = path.getBytes();
    store.executeSingle(new PathCommand(SET_DATA, path, data));
    byte[] returned = store.getData(path);
    assertNotNull(returned);
    assertEquals(new String(returned), path);
    returned = zkCoordinator2.getHierarchicalStore().getData(path);
    assertEquals(new String(returned), path);
  }

  @Test
  public void testBookingUsingEphemeralNodes() throws Exception {
    final LinearizableHierarchicalStore store = zkCoordinator1.getHierarchicalStore();
    final String path = Path.SEPARATOR + "cleanerPath" + Path.SEPARATOR + "234567";
    store.executeSingle(new PathCommand(CREATE_PERSISTENT, path));
    final String bookingPath = path + Path.SEPARATOR + "book";
    store.executeSingle(new PathCommand(CREATE_EPHEMERAL, bookingPath));
    assertTrue(store.checkExists(path));
    assertTrue(store.checkExists(bookingPath));
    assertNull(store.getData(path));
    final LinearizableHierarchicalStore secondStore = zkCoordinator2.getHierarchicalStore();
    try {
      secondStore.executeSingle(new PathCommand(CREATE_EPHEMERAL, bookingPath));
      fail("Ephemeral path creation succeeded unexpectedly");
    } catch (PathExistsException e) {
      assertThat(e.getMessage()).contains(bookingPath);
    }
    // when the first coordinator closes we should succeed
    zkCoordinator1.close();
    try {
      secondStore.executeSingle(new PathCommand(CREATE_PERSISTENT, path));
      fail("Persistent path creation succeeded unexpectedly");
    } catch (PathExistsException e) {
      assertThat(e.getMessage()).contains(path);
    }
    assertFalse(secondStore.checkExists(bookingPath));
    secondStore.executeSingle(new PathCommand(CREATE_EPHEMERAL, bookingPath));
    assertTrue(secondStore.checkExists(bookingPath));
  }

  @Test
  public void testDoneBroadcastUsingEphemeralSequentialNodes() throws Exception {
    final LinearizableHierarchicalStore store = zkCoordinator1.getHierarchicalStore();
    final String path = Path.SEPARATOR + "task5" + Path.SEPARATOR + "234567";
    store.executeSingle(new PathCommand(CREATE_PERSISTENT, path));
    final String donePath = path + Path.SEPARATOR + "done-";
    store.executeSingle(new PathCommand(CREATE_EPHEMERAL_SEQUENTIAL, donePath));
    assertTrue(store.checkExists(path));
    CountDownLatch latch = new CountDownLatch(1);
    CompletableFuture<Void> onChange = new CompletableFuture<>();
    onChange.thenRun(latch::countDown);
    List<String> roundOneChildren = store.getChildren(path, onChange);
    assertThat(roundOneChildren.size()).isEqualTo(1);
    final LinearizableHierarchicalStore secondStore = zkCoordinator2.getHierarchicalStore();
    secondStore.executeSingle(new PathCommand(CREATE_EPHEMERAL_SEQUENTIAL, donePath));
    List<String> roundTwoChildren = secondStore.getChildren(path, null);
    assertThat(roundTwoChildren.size()).isEqualTo(2);
    // latch countdown should have been triggered by now
    latch.await();
    // when the first coordinator closes we should succeed
    zkCoordinator1.close();
    roundTwoChildren = secondStore.getChildren(path, null);
    assertThat(roundTwoChildren.size()).isEqualTo(1);
  }

  @Test
  public void testMultiTransaction() throws Exception {
    final LinearizableHierarchicalStore store = zkCoordinator1.getHierarchicalStore();
    final String path1 = Path.SEPARATOR + "multiPath1" + Path.SEPARATOR + "1234567";
    final String path2 = Path.SEPARATOR + "multiPath2" + Path.SEPARATOR + "1234567";
    final String bookingPath = path1 + Path.SEPARATOR + "book";
    final String donePath = path2 + Path.SEPARATOR + "done";
    final PathCommand path1Persistent = new PathCommand(CREATE_PERSISTENT, path1);
    final PathCommand path2Persistent = new PathCommand(CREATE_PERSISTENT, path2);
    final PathCommand createBookingCommand = new PathCommand(CREATE_EPHEMERAL, bookingPath);
    final PathCommand createDoneCommand = new PathCommand(CREATE_EPHEMERAL, donePath);
    final PathCommand releaseBookingCommand = new PathCommand(DELETE, bookingPath);
    store.executeSingle(path1Persistent);
    store.executeSingle(path2Persistent);
    store.executeSingle(createBookingCommand);
    // now delete booking and mark done in a single transaction
    final PathCommand[] cmds = new PathCommand[] {releaseBookingCommand, createDoneCommand};
    store.executeMulti(cmds);
    final LinearizableHierarchicalStore secondStore = zkCoordinator1.getHierarchicalStore();
    assertFalse(secondStore.checkExists(bookingPath));
    assertTrue(secondStore.checkExists(donePath));
    secondStore.executeSingle(createBookingCommand);
    assertTrue(store.checkExists(bookingPath));
  }

  @Test
  public void testBasicWatcherPreExist() throws Exception {
    final LinearizableHierarchicalStore store1 = zkCoordinator1.getHierarchicalStore();
    final String path = Path.SEPARATOR + "task1";
    final String bookPath = path + Path.SEPARATOR + "book";
    store1.executeSingle(new PathCommand(CREATE_PERSISTENT, path));
    assertTrue(store1.checkExists(path));
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger watchCount = new AtomicInteger(0);
    store1.whenCreated(bookPath).thenRun(() -> {
      watchCount.incrementAndGet();
      latch.countDown();
    });
    final LinearizableHierarchicalStore store2 = zkCoordinator2.getHierarchicalStore();
    store2.executeSingle(new PathCommand(CREATE_EPHEMERAL, bookPath));
    latch.await();
    assertEquals(watchCount.get(), 1);
  }

  @Test
  public void testBasicWatcherPostExist() throws Exception {
    final LinearizableHierarchicalStore store1 = zkCoordinator1.getHierarchicalStore();
    final String path = Path.SEPARATOR + "task2";
    final String bookPath = path + Path.SEPARATOR + "book";
    store1.executeSingle(new PathCommand(CREATE_PERSISTENT, path));
    store1.executeSingle(new PathCommand(CREATE_EPHEMERAL, bookPath));
    assertTrue(store1.checkExists(path));
    assertNull(store1.getData(path));
    final LinearizableHierarchicalStore store2 = zkCoordinator2.getHierarchicalStore();
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger watchCount = new AtomicInteger(0);
    store2.whenDeleted(bookPath).thenRun(() -> {
      watchCount.incrementAndGet();
      latch.countDown();
    });
    assertTrue(store1.checkExists(path));
    zkCoordinator1.close();
    latch.await();
    assertEquals(watchCount.get(), 1);
  }

  @Test
  public void testGetStat() throws Exception {
    final LinearizableHierarchicalStore store1 = zkCoordinator1.getHierarchicalStore();
    final String path = Path.SEPARATOR + "task5";
    final String bookPath = path + Path.SEPARATOR + "book";
    store1.executeSingle(new PathCommand(CREATE_PERSISTENT, path));
    assertTrue(store1.checkExists(path));
    assertNull(store1.getData(path));
    assertThat(store1.getStats(path).getNumChanges()).isEqualTo(0);
    assertNull(store1.getStats(bookPath));
    store1.executeSingle(new PathCommand(CREATE_EPHEMERAL, bookPath));
    store1.executeSingle(new PathCommand(SET_DATA, path, path.getBytes()));
    final long creationTime = store1.getStats(bookPath).getCreationTime();
    final long currentTime = System.currentTimeMillis();
    assertThat(creationTime).isBetween(currentTime - 100000, currentTime + 100000);
    final LinearizableHierarchicalStore store2 = zkCoordinator2.getHierarchicalStore();
    assertThat(store2.getStats(bookPath).getCreationTime()).isEqualTo(creationTime);
    assertThat(store2.getStats(path).getNumChanges()).isEqualTo(1);
  }

  @Test
  public void testCheckExistReverse() throws Exception {
    final LinearizableHierarchicalStore store1 = zkCoordinator1.getHierarchicalStore();
    final String path = Path.SEPARATOR + "task5";
    final String bookPath = path + Path.SEPARATOR + "book";
    store1.executeSingle(new PathCommand(CREATE_PERSISTENT, path));
    assertTrue(store1.checkExists(path));
    assertNull(store1.getData(path));

    final LinearizableHierarchicalStore store2 = zkCoordinator2.getHierarchicalStore();
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger watchCount = new AtomicInteger(0);
    try {
      store2.whenDeleted(bookPath).thenRun(() -> {
        watchCount.incrementAndGet();
        latch.countDown();
      });
      fail("Path " + bookPath + " should not be existing");
    } catch (PathMissingException e) {
      assertThat(e.getMessage()).contains(bookPath);
    }
    store1.executeSingle(new PathCommand(CREATE_EPHEMERAL, bookPath));
    try {
      store2.whenCreated(bookPath).thenRun(() -> {});
      fail("Path " + bookPath + " should be existing");
    } catch (PathExistsException e) {
      assertThat(e.getMessage()).contains(bookPath);
    }
    store2.whenDeleted(bookPath).thenRun(() -> {
      watchCount.incrementAndGet();
      latch.countDown();
    });
    assertTrue(store1.checkExists(path));
    zkCoordinator1.close();
    latch.await();
    assertEquals(watchCount.get(), 1);
  }

  @Test
  public void testTransactionWithWatcher() throws Exception {
    final LinearizableHierarchicalStore store1 = zkCoordinator1.getHierarchicalStore();
    final String servicePath = Path.SEPARATOR + "cls";
    final String taskPath = servicePath + Path.SEPARATOR + "task3";
    final String bookPath = taskPath + Path.SEPARATOR + "book";
    final String stealPath = servicePath + Path.SEPARATOR + "steal";
    final String stealTaskPath = stealPath + Path.SEPARATOR + "task3";
    store1.executeSingle(new PathCommand(CREATE_PERSISTENT, taskPath));
    store1.executeSingle(new PathCommand(CREATE_PERSISTENT, stealPath));
    store1.executeSingle(new PathCommand(CREATE_EPHEMERAL, bookPath));
    assertTrue(store1.checkExists(bookPath));
    assertTrue(store1.checkExists(stealPath));
    final LinearizableHierarchicalStore store2 = zkCoordinator2.getHierarchicalStore();
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger watchCount = new AtomicInteger(0);
    final CompletableFuture<Void> onChange = new CompletableFuture<>();
    onChange.thenRun(() -> {
      watchCount.incrementAndGet();
      assertFalse(store2.checkExists(bookPath));
      assertTrue(store2.checkExists(stealTaskPath));
      latch.countDown();
    });
    List<String> children = store2.getChildren(stealPath, onChange);
    assertThat(children.size()).isEqualTo(0);
    final PathCommand releaseBookingCommand = new PathCommand(DELETE, bookPath);
    final PathCommand stealCmd = new PathCommand(CREATE_EPHEMERAL, stealTaskPath);
    final PathCommand[] cmds = new PathCommand[] {releaseBookingCommand, stealCmd};
    store1.executeMulti(cmds);
    latch.await();
    assertEquals(watchCount.get(), 1);
    children = store2.getChildren(stealPath, null);
    assertThat(children.size()).isEqualTo(1);
  }
}
