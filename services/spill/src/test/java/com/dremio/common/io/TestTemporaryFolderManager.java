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
package com.dremio.common.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Test the temporary folder management functionality. */
@RunWith(Parameterized.class)
public class TestTemporaryFolderManager {
  private static final int DEFAULT_TEST_STALENESS_LIMIT_SECONDS = 5;
  private static final int DEFAULT_TEST_DELAY_SECONDS = 2;
  private static final int DEFAULT_TEST_VARIATION_SECONDS = 2;
  private static final int DEFAULT_MIN_UNHEALTHY_CYCLES = 5;

  private static final Configuration TEST_CONFIG;

  static {
    TEST_CONFIG = new Configuration();
    TEST_CONFIG.set("fs.file.impl.disable.cache", "true");
    // If the location URI doesn't contain any schema, fall back to local.
    TEST_CONFIG.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
  }

  @Parameterized.Parameters(name = "Executor 1: {0}, Executor 2: {1}, Executor 3: {2}")
  public static List<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {"localhost1", "localhost2", "localhost3"},
        new Object[] {
          "dremio-executor-reflections-1.dremio-cluster-pod.abc-test.svc.cluster.local",
          "dremio-executor-reflections-2.dremio-cluster-pod.abc-test.svc.cluster.local",
          "dremio-executor-reflections-3.dremio-cluster-pod.abc-test.svc.cluster.local"
        },
        new Object[] {"192.168.1.1", "192.168.1.2", "http://192.168.1.3"});
  }

  @Rule public TemporaryFolder testRootDir = new TemporaryFolder();

  private final ExecutorId thisExecutor;
  private final ExecutorId otherExecutor1;
  private final ExecutorId otherExecutor2;
  private final MockZooKeeper zkService = new MockZooKeeper();

  public TestTemporaryFolderManager(String one, String two, String three) {
    thisExecutor = new ExecutorId(one, 4440);
    otherExecutor1 = new ExecutorId(two, 4441);
    otherExecutor2 = new ExecutorId(three, 4442);
  }

  private Path[] rootPaths;

  @Before
  public void setup() throws IOException {
    rootPaths = new Path[4];
    for (int i = 0; i < 4; i++) {
      final File folder = testRootDir.newFolder();
      rootPaths[i] = new Path(folder.getPath());
    }
    zkService.addExecutor(thisExecutor);
    zkService.addExecutor(otherExecutor1);
    zkService.addExecutor(otherExecutor2);
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testTempDirsAreCreated() throws Exception {
    try (final TemporaryFolderManager ignored = createManager()) {
      assertRootPathsHasTempDirs(1, 1);
    }
  }

  @Test
  public void testUpdatedPostRestartDirsAreNotDeleted() throws Exception {
    TemporaryFolderManager folderManagerUnderTest = createManagerOnly(thisExecutor, 1, 1, 1, 1);
    final Path testPath1 = new Path(testRootDir.newFolder().getPath());
    final Path oldPath = folderManagerUnderTest.createTmpDirectory(testPath1);
    folderManagerUnderTest.close();
    folderManagerUnderTest = createManagerOnly(thisExecutor, 1, 5, 2, 1);
    folderManagerUnderTest.createTmpDirectory(testPath1);
    createLoop(oldPath, 21);
    folderManagerUnderTest.close();
    assertIndividualPath(testPath1, 1, 2);
  }

  @Test
  public void testStaleDirsAreDeletedOnRestart() throws Exception {
    TemporaryFolderManager folderManagerUnderTest = createManager();
    folderManagerUnderTest.close();
    folderManagerUnderTest = createManager(thisExecutor, 0, 5, 2, 1);
    folderManagerUnderTest.close();
    // there should be only latest dirs in all root paths
    assertRootPathsHasTempDirs(1, 1);
  }

  @Test
  public void testFailedExecutorsAreDeleted() throws Exception {
    TemporaryFolderManager mgr1 = createManager(thisExecutor, 1, 1, 1, 2);
    createManager(otherExecutor1, 1, 1, 1, 2);
    TemporaryFolderManager mgr3 = createManager(otherExecutor2, 1, 1, 1, 2);
    assertRootPathsHasTempDirs(3, 1);
    // fail executor2
    zkService.removeExecutor(otherExecutor2);
    // assert that the failed executors are deleted
    assertRootPathsHasTempDirsWithWait(20, 3, new int[] {1, 1, 0});

    zkService.removeExecutor(thisExecutor);
    assertRootPathsHasTempDirsWithWait(20, 3, new int[] {0, 1, 0});

    // bring the executors back up
    zkService.addExecutor(otherExecutor2);
    mgr3.close();
    createManager(otherExecutor2, 1, 1, 1, 2);

    zkService.addExecutor(thisExecutor);
    mgr1.close();
    createManager(thisExecutor, 1, 1, 1, 2);
    assertRootPathsHasTempDirs(3, 1);
  }

  @Test
  public void testTransientExecutorFailuresAreIgnored() throws Exception {
    createManager(thisExecutor, 1, 1, 1, 5);
    createManager(otherExecutor1, 1, 1, 1, 5);
    createManager(otherExecutor2, 1, 1, 1, 5);
    assertRootPathsHasTempDirs(3, 1);
    // fail executor2
    zkService.transientRemoveExecutor(otherExecutor2, 3);
    assertRootPathsHasTempDirs(3, 1);
  }

  @Test
  public void testDirectoryWithRecentFilesAreNotDeleted() throws Exception {
    TemporaryFolderManager mgr1 = createManagerOnly(thisExecutor, 3, 1, 0, 0);
    TemporaryFolderManager mgr2 = createManagerOnly(otherExecutor1, 3, 1, 0, 0);
    TemporaryFolderManager mgr3 = createManagerOnly(otherExecutor2, 3, 1, 0, 0);

    final Path testPath1 = new Path(testRootDir.newFolder().getPath());
    final Path testPath2 = new Path(testRootDir.newFolder().getPath());
    final FileSystem fs = testPath1.getFileSystem(TEST_CONFIG);

    final Path tmpPath1 = mgr3.createTmpDirectory(testPath1);
    final Path tmpPath2 = mgr3.createTmpDirectory(testPath2);

    mgr1.createTmpDirectory(testPath1);
    mgr2.createTmpDirectory(testPath2);
    mgr1.createTmpDirectory(testPath1);
    mgr2.createTmpDirectory(testPath2);

    zkService.waitCycles(3);

    final CompletableFuture<Void> f = CompletableFuture.runAsync(() -> createLoop(tmpPath2, 100));

    // fail executor2
    mgr3.close();
    zkService.removeExecutor(otherExecutor2);

    zkService.waitCycles(10);

    assertTrue(fs.exists(tmpPath2));
    assertFalse(fs.exists(tmpPath1));

    f.get();

    zkService.waitCycles(20);

    assertFalse(fs.exists(tmpPath2));
    assertFalse(fs.exists(tmpPath1));
  }

  @Test
  public void testAutoClose() throws Exception {
    final Path testPath = new Path(testRootDir.newFolder().getPath());
    try (TemporaryFolderManager manager =
        new DefaultTemporaryFolderManager(() -> otherExecutor1, TEST_CONFIG, () -> null, "test")) {
      manager.startMonitoring();
      manager.createTmpDirectory(testPath);
    }
    assertIndividualPath(testPath, 1, 1);
  }

  @Test
  public void testClose() throws Exception {
    final TemporaryFolderManager folderManagerUnderTest = createManager(thisExecutor, 5, 1, 1, 1);
    folderManagerUnderTest.close();
    final Path testPath = new Path(testRootDir.newFolder().getPath());
    assertThatThrownBy(() -> folderManagerUnderTest.createTmpDirectory(testPath))
        .hasMessageContaining("closed");
  }

  @Test
  public void testOldFunctionalityRetained() throws Exception {
    try (final TemporaryFolderManager mgr = createNullManager()) {
      final Path testPath = new Path(testRootDir.newFolder().getPath());
      assertThat(mgr.createTmpDirectory(testPath).toString()).isEqualTo(testPath.toString());
    }
  }

  private void createLoop(Path tmpPath2, int loopCount) {
    try (FileSystem fs = tmpPath2.getFileSystem(TEST_CONFIG)) {
      Path nextPath = tmpPath2;
      for (int i = 0; i < loopCount; i++) {
        if (i % 10 == 0) {
          final Path createdDir = new Path(nextPath, "test" + i);
          fs.mkdirs(createdDir);
          nextPath = new Path(nextPath, "test" + i);
        } else {
          final String fName = "test" + i + ".txt";
          final Path createdFile = new Path(nextPath, fName);
          fs.create(createdFile).close();
        }
        Thread.sleep(100);
      }
      final Path lastCreatedDir = new Path(tmpPath2, "test" + loopCount);
      fs.mkdirs(lastCreatedDir);
    } catch (IOException | InterruptedException e) {
      fail();
    }
  }

  private void assertRootPathsHasTempDirs(int expectedExecutors, int expectedIncarnations)
      throws IOException {
    for (final Path rootPath : rootPaths) {
      assertIndividualPath(rootPath, expectedExecutors, expectedIncarnations);
    }
  }

  private void assertIndividualPath(Path rootPath, int expectedExecutors, int expectedIncarnations)
      throws IOException {
    final FileSystem fs = rootPath.getFileSystem(TEST_CONFIG);
    final FileStatus[] statuses = fs.listStatus(rootPath);
    assertThat(statuses).hasSize(expectedExecutors);
    for (final FileStatus status : statuses) {
      assertTrue(status.isDirectory());
      final FileStatus[] l2Statuses = fs.listStatus(status.getPath());
      assertThat(l2Statuses).hasSize(expectedIncarnations);
      for (final FileStatus l2Status : l2Statuses) {
        assertTrue(l2Status.isDirectory());
        long incarnation = Long.parseLong(l2Status.getPath().getName());
        assertThat(incarnation).isGreaterThan(Instant.now().toEpochMilli() - 60000);
      }
    }
  }

  private void assertRootPathsHasTempDirsWithWait(
      int maxWaitSeconds, int expectedExecutors, int[] expectedIncarnations) throws IOException {
    for (final Path rootPath : rootPaths) {
      assertIndividualPathWithWait(
          maxWaitSeconds, rootPath, expectedExecutors, expectedIncarnations);
    }
  }

  private void assertIndividualPathWithWait(
      int maxWaitSeconds, Path rootPath, int expectedExecutors, int[] expectedIncarnations)
      throws IOException {
    final FileSystem fs = rootPath.getFileSystem(TEST_CONFIG);
    final FileStatus[] statuses = fs.listStatus(rootPath);
    assertThat(statuses).hasSize(expectedExecutors);
    for (final FileStatus status : statuses) {
      assertTrue(status.isDirectory());
      String executor = status.getPath().getName();
      int idx = executor.charAt(executor.length() - 1) - '0';
      waitUntil(
          maxWaitSeconds,
          expectedIncarnations[idx],
          () -> {
            try {
              return fs.listStatus(status.getPath()).length;
            } catch (IOException e) {
              return 0;
            }
          });
      final FileStatus[] l2Statuses = fs.listStatus(status.getPath());
      assertThat(l2Statuses).hasSize(expectedIncarnations[idx]);
      for (final FileStatus l2Status : l2Statuses) {
        assertTrue(l2Status.isDirectory());
        long incarnation = Long.parseLong(l2Status.getPath().getName());
        assertThat(incarnation).isGreaterThan(Instant.now().toEpochMilli() - 60000);
      }
    }
  }

  private void waitUntil(int maxWaitSeconds, int expectedValue, IntSupplier checker) {
    Instant start = Instant.now();
    Instant current = start;
    boolean interrupted = false;
    boolean done = false;
    try {
      while (Duration.between(start, current).getSeconds() < maxWaitSeconds && !done) {
        int val = checker.getAsInt();
        if (val != expectedValue) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        } else {
          done = true;
        }
        current = Instant.now();
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private TemporaryFolderManager createManager() throws IOException {
    final TemporaryFolderManager mgrUnderTest =
        new DefaultTemporaryFolderManager(
            () -> thisExecutor, TEST_CONFIG, () -> null, "test", new TestCleanupConfig());
    mgrUnderTest.startMonitoring();
    for (final Path rootPath : rootPaths) {
      mgrUnderTest.createTmpDirectory(rootPath);
    }
    return mgrUnderTest;
  }

  private TemporaryFolderManager createManager(
      ExecutorId executorId, int stale, int delay, int variation, int unhealthy)
      throws IOException {
    final TemporaryFolderManager mgrUnderTest =
        new DefaultTemporaryFolderManager(
            () -> executorId,
            TEST_CONFIG,
            zkService,
            "test",
            new TestCleanupConfig(stale, delay, variation, unhealthy));
    mgrUnderTest.startMonitoring();
    for (final Path rootPath : rootPaths) {
      mgrUnderTest.createTmpDirectory(rootPath);
    }
    return mgrUnderTest;
  }

  private TemporaryFolderManager createManagerOnly(
      ExecutorId executorId, int stale, int delay, int variation, int unhealthy) {
    final TemporaryFolderManager mgrUnderTest =
        new DefaultTemporaryFolderManager(
            () -> executorId,
            TEST_CONFIG,
            zkService,
            "test",
            new TestCleanupConfig(stale, delay, variation, unhealthy));
    mgrUnderTest.startMonitoring();
    return mgrUnderTest;
  }

  private TemporaryFolderManager createNullManager() {
    final TemporaryFolderManager mgrUnderTest =
        new DefaultTemporaryFolderManager(
            () -> null, TEST_CONFIG, () -> null, "test", new TestCleanupConfig());
    mgrUnderTest.startMonitoring();
    return mgrUnderTest;
  }

  private static final class MockZooKeeper implements Supplier<Set<ExecutorId>> {
    private final Set<ExecutorId> availableExecutors;
    private final Map<ExecutorId, Integer> transientMap;
    private final AtomicInteger currentCycle;

    private volatile CountDownLatch waitLatch;

    private MockZooKeeper() {
      this.availableExecutors = ConcurrentHashMap.newKeySet();
      this.transientMap = new ConcurrentHashMap<>();
      this.currentCycle = new AtomicInteger(0);
    }

    void addExecutor(ExecutorId executorId) {
      this.availableExecutors.add(executorId);
    }

    void removeExecutor(ExecutorId executorId) {
      this.availableExecutors.remove(executorId);
    }

    void transientRemoveExecutor(ExecutorId executorId, int numCycles) {
      if (availableExecutors.remove(executorId)) {
        transientMap.put(executorId, currentCycle.get() + numCycles);
      }
    }

    void waitCycles(int numCycles) {
      waitLatch = new CountDownLatch(numCycles);
      boolean interrupted = Thread.currentThread().isInterrupted();

      try {
        if (!waitLatch.await(2, TimeUnit.MINUTES)) {
          fail("Waited too long for cycles to reach target");
        }
      } catch (InterruptedException e) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        waitLatch = null;
      }
    }

    @Override
    public Set<ExecutorId> get() {
      currentCycle.incrementAndGet();
      if (!transientMap.isEmpty()) {
        final Set<ExecutorId> executorIds =
            transientMap.entrySet().stream()
                .filter((e) -> e.getValue() > currentCycle.get())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        availableExecutors.addAll(executorIds);
        executorIds.forEach(transientMap::remove);
      }
      final CountDownLatch latch = waitLatch;
      if (latch != null) {
        latch.countDown();
      }
      return availableExecutors;
    }
  }

  private static final class TestCleanupConfig
      implements DefaultTemporaryFolderManager.CleanupConfig {
    private final int stalenessLimitSeconds;
    private final int cleanupDelaySeconds;
    private final int cleanupDelayMaxVariationSeconds;
    private final int minUnhealthyCycles;

    private TestCleanupConfig() {
      stalenessLimitSeconds = DEFAULT_TEST_STALENESS_LIMIT_SECONDS;
      cleanupDelaySeconds = DEFAULT_TEST_DELAY_SECONDS;
      cleanupDelayMaxVariationSeconds = DEFAULT_TEST_VARIATION_SECONDS;
      minUnhealthyCycles = DEFAULT_MIN_UNHEALTHY_CYCLES;
    }

    private TestCleanupConfig(int stale, int delay, int variation, int unhealthy) {
      stalenessLimitSeconds = stale;
      cleanupDelaySeconds = delay;
      cleanupDelayMaxVariationSeconds = variation;
      minUnhealthyCycles = unhealthy;
    }

    @Override
    public int getStalenessOnRestartSeconds() {
      // for tests, staleness checks can be same for restart of same executor and for monitoring
      // other
      // executors
      return stalenessLimitSeconds;
    }

    @Override
    public int getStalenessLimitSeconds() {
      return stalenessLimitSeconds;
    }

    @Override
    public int getCleanupDelaySeconds() {
      return cleanupDelaySeconds;
    }

    @Override
    public int getCleanupDelayMaxVariationSeconds() {
      return cleanupDelayMaxVariationSeconds;
    }

    @Override
    public int getOneShotCleanupDelaySeconds() {
      return stalenessLimitSeconds + 1;
    }

    @Override
    public int getMinUnhealthyCyclesBeforeDelete() {
      return minUnhealthyCycles;
    }
  }
}
