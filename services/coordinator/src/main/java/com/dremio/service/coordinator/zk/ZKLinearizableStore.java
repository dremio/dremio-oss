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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.dremio.service.coordinator.LinearizableHierarchicalStore;
import com.dremio.service.coordinator.exceptions.PathExistsException;
import com.dremio.service.coordinator.exceptions.PathMissingException;
import com.dremio.service.coordinator.exceptions.StoreFatalException;
import com.google.common.base.Preconditions;

/**
 * An implementation of {@code LinearizableHierarchicalStore} that uses zookeeper.
 * <p>
 * Zoo keeper provides linearizability guarantees for writes, and sequential consistency guarantees for reads.
 * </p>
 */
public class ZKLinearizableStore implements LinearizableHierarchicalStore {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ZKLinearizableStore.class);
  private static final Map<CommandType, Function<ZKLinearizableStore, Op>> OP_LAB;
  private static final NoOp NO_OP_OBJ = new NoOp();
  private static final Function<ZKLinearizableStore, Op> NO_OP = (z) -> NO_OP_OBJ;

  static {
    Map<CommandType, Function<ZKLinearizableStore, Op>> tmpMap = new HashMap<>();
    tmpMap.put(CommandType.CREATE_EPHEMERAL, (z) -> z.new CreateEphemeralOp());
    tmpMap.put(CommandType.CREATE_EPHEMERAL_SEQUENTIAL, (z) -> z.new CreateEphemeralSequentialOp());
    tmpMap.put(CommandType.CREATE_PERSISTENT, (z) -> z.new CreatePersistentOp());
    tmpMap.put(CommandType.DELETE, (z) -> z.new DeleteOp());
    tmpMap.put(CommandType.SET_DATA, (z) -> z.new SetDataOp());
    OP_LAB = Collections.unmodifiableMap(tmpMap);
  }

  private final CuratorFramework zkClient;
  private final Set<ZKStoreOpWatcher> watcherSet;

  ZKLinearizableStore(CuratorFramework zkClient) {
    this.zkClient = zkClient;
    this.watcherSet = ConcurrentHashMap.newKeySet();
  }

  @Override
  public void executeMulti(PathCommand[] commands) throws PathMissingException, PathExistsException {
    Preconditions.checkArgument(commands.length >= 2, "Unexpected number of store commands");
    List<CuratorOp> curatorOps = new ArrayList<>();
    try {
      for (PathCommand command : commands) {
        Op op = OP_LAB.getOrDefault(command.getCommandType(), NO_OP).apply(this);
        curatorOps.add(op.createOp(command));
      }
      List<CuratorTransactionResult> results = zkClient.transaction().forOperations(curatorOps);
      checkResults(results);
    } catch (Exception e) {
      if (e instanceof KeeperException.NodeExistsException) {
        throw new PathExistsException(((KeeperException.NodeExistsException) e).getPath());
      }
      if (e instanceof KeeperException.NoNodeException) {
        throw new PathMissingException(((KeeperException.NoNodeException) e).getPath());
      }
      throw new StoreFatalException(e);
    }
  }

  @Override
  public void executeSingle(PathCommand command) throws PathMissingException, PathExistsException {
    Op op = OP_LAB.getOrDefault(command.getCommandType(), NO_OP).apply(this);
    try {
      op.doOp(command);
    } catch (Exception e) {
      if (e instanceof KeeperException.NodeExistsException) {
        throw new PathExistsException(((KeeperException.NodeExistsException) e).getPath());
      }
      if (e instanceof KeeperException.NoNodeException) {
        throw new PathMissingException(((KeeperException.NoNodeException) e).getPath());
      }
      throw new StoreFatalException(e);
    }
  }

  @Override
  public byte[] getData(String fullPath) throws PathMissingException {
    try {
      byte[] data = zkClient.getData().forPath(fullPath);
      return (data == null || data.length == 0) ? null : data;
    } catch (Exception e) {
      if (e instanceof KeeperException.NoNodeException) {
        throw new PathMissingException(fullPath);
      }
      throw new StoreFatalException(e);
    }
  }

  @Override
  public boolean checkExists(String fullPath) {
    try {
      return zkClient.checkExists().forPath(fullPath) != null;
    } catch (Exception e) {
      throw new StoreFatalException(e);
    }
  }

  @Override
  public Stats getStats(String fullPath) {
    try {
      final Stat s = zkClient.checkExists().forPath(fullPath);
      return s == null ? null : new Stats() {
        @Override
        public long getCreationTime() {
          return s.getCtime();
        }

        @Override
        public long getLastModifiedTime() {
          return s.getMtime();
        }

        @Override
        public int getNumChanges() {
          return s.getVersion();
        }
      };
    } catch (Exception e) {
      throw new StoreFatalException(e);
    }
  }

  @Override
  public CompletableFuture<Void> whenDeleted(String fullPath) throws PathMissingException {
    // use asserts here instead of pre-conditions as only trusted clients use this interface as of now
    assert fullPath != null;
    final CompletableFuture<Void> onPathDeletion = new CompletableFuture<>();
    boolean exists;
    try {
      ZKStoreOpWatcher watcher = new ZKStoreOpWatcher(fullPath, onPathDeletion, Watcher.Event.EventType.NodeDeleted);
      // record it for future recovery in case connection breaks
      watcherSet.add(watcher);
      exists = zkClient.checkExists().usingWatcher(watcher).forPath(fullPath) != null;
    } catch (Exception e) {
      throw new StoreFatalException(e);
    }
    if (!exists) {
      throw new PathMissingException(fullPath);
    }
    return onPathDeletion;
  }

  @Override
  public CompletableFuture<Void> whenCreated(String fullPath) throws PathExistsException {
    // use asserts here instead of pre-conditions as only trusted clients use this interface as of now
    assert fullPath != null;
    final CompletableFuture<Void> onPathCreation = new CompletableFuture<>();
    boolean exists;
    try {
      ZKStoreOpWatcher watcher = new ZKStoreOpWatcher(fullPath, onPathCreation, Watcher.Event.EventType.NodeCreated);
      // record it for future recovery in case connection breaks
      watcherSet.add(watcher);
      exists = zkClient.checkExists().usingWatcher(watcher).forPath(fullPath) != null;
    } catch (Exception e) {
      throw new StoreFatalException(e);
    }
    if (exists) {
      throw new PathExistsException(fullPath);
    }
    return onPathCreation;
  }

  @Override
  public List<String> getChildren(String fullPath, CompletableFuture<Void> onChildrenChanged)
    throws PathMissingException {
    assert fullPath != null;
    try {
      if (onChildrenChanged == null) {
        return zkClient.getChildren().forPath(fullPath);
      } else {
        final ZKStoreOpWatcher watcher = new ZKStoreOpWatcher(fullPath, onChildrenChanged,
          Watcher.Event.EventType.NodeChildrenChanged);
        // record it for future recovery in case connection breaks
        watcherSet.add(watcher);
        return zkClient.getChildren().usingWatcher(watcher).forPath(fullPath);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void checkResults(List<CuratorTransactionResult> results) {
    for (CuratorTransactionResult result : results) {
      Preconditions.checkArgument(result.getError() == KeeperException.Code.OK.intValue(),
        "Unexpected failure result on Path " + result.getForPath());
    }
  }

  private interface Op {
    CuratorOp createOp(PathCommand cmd) throws Exception;

    void doOp(PathCommand cmd) throws Exception;
  }

  private final class CreateEphemeralOp implements Op {
    @Override
    public CuratorOp createOp(PathCommand cmd) throws Exception {
      byte[] dataToSend = (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      return zkClient.transactionOp().create().withMode(CreateMode.EPHEMERAL).forPath(cmd.getFullPath(), dataToSend);
    }

    @Override
    public void doOp(PathCommand cmd) throws Exception {
      byte[] dataToSend = (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(cmd.getFullPath(), dataToSend);
    }
  }

  private final class CreateEphemeralSequentialOp implements Op {
    @Override
    public CuratorOp createOp(PathCommand cmd) throws Exception {
      byte[] dataToSend = (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      return zkClient.transactionOp().create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(cmd.getFullPath(), dataToSend);
    }

    @Override
    public void doOp(PathCommand cmd) throws Exception {
      byte[] dataToSend = (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      zkClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(cmd.getFullPath(), dataToSend);
    }
  }

  private final class CreatePersistentOp implements Op {
    @Override
    public CuratorOp createOp(PathCommand cmd) throws Exception {
      byte[] dataToSend = (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      return zkClient.transactionOp().create().withMode(CreateMode.PERSISTENT).forPath(cmd.getFullPath(), dataToSend);
    }

    @Override
    public void doOp(PathCommand cmd) throws Exception {
      byte[] dataToSend = (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(cmd.getFullPath(), dataToSend);
    }
  }

  private final class DeleteOp implements Op {
    @Override
    public CuratorOp createOp(PathCommand cmd) throws Exception {
      return zkClient.transactionOp().delete().forPath(cmd.getFullPath());
    }

    @Override
    public void doOp(PathCommand cmd) throws Exception {
      zkClient.delete().guaranteed().forPath(cmd.getFullPath());
    }
  }

  private final class SetDataOp implements Op {
    @Override
    public CuratorOp createOp(PathCommand cmd) throws Exception {
      Preconditions.checkArgument(cmd.getData() != null, "Valid data must be presented to store");
      return zkClient.transactionOp().setData().forPath(cmd.getFullPath(), cmd.getData());
    }

    @Override
    public void doOp(PathCommand cmd) throws Exception {
      Preconditions.checkArgument(cmd.getData() != null, "Valid data must be presented to store");
      zkClient.setData().forPath(cmd.getFullPath(), cmd.getData());
    }
  }

  private static final class NoOp implements Op {
    @Override
    public CuratorOp createOp(PathCommand cmd) {
      throw new IllegalArgumentException("Unimplemented hierarchical store operation type " + cmd.getCommandType());
    }

    @Override
    public void doOp(PathCommand cmd) {
      throw new IllegalArgumentException("Unimplemented hierarchical store operation type " + cmd.getCommandType());
    }
  }

  private final class ZKStoreOpWatcher implements CuratorWatcher {
    private final String fullPath;
    private final CompletableFuture<Void> onOpComplete;
    private final Watcher.Event.EventType expectedEventType;

    private ZKStoreOpWatcher(String fullPath, CompletableFuture<Void> onOpComplete, Watcher.Event.EventType eventType) {
      this.fullPath = fullPath;
      this.onOpComplete = onOpComplete;
      this.expectedEventType = eventType;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
      LOGGER.info("Watcher Event {} triggered for path {}", watchedEvent.getType(), fullPath);
      if (watchedEvent.getType().equals(expectedEventType)) {
        watcherSet.remove(this);
        onOpComplete.complete(null);
      } else {
        LOGGER.warn("Unknown watcher event {} received for path {}", watchedEvent.getType(), fullPath);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ZKStoreOpWatcher that = (ZKStoreOpWatcher) o;
      return fullPath.equals(that.fullPath) && onOpComplete.equals(that.onOpComplete)
        && expectedEventType == that.expectedEventType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fullPath, onOpComplete, expectedEventType);
    }
  }
}
