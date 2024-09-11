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

import com.dremio.service.coordinator.LinearizableHierarchicalStore;
import com.dremio.service.coordinator.LostConnectionObserver;
import com.dremio.service.coordinator.exceptions.PathExistsException;
import com.dremio.service.coordinator.exceptions.PathMissingException;
import com.dremio.service.coordinator.exceptions.StoreFatalException;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
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

/**
 * An implementation of {@code LinearizableHierarchicalStore} that uses zookeeper.
 *
 * <p>Zoo keeper provides linearizability guarantees for writes, and sequential consistency
 * guarantees for reads.
 */
public class ZKLinearizableStore implements LinearizableHierarchicalStore, LostConnectionObserver {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ZKLinearizableStore.class);
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
  private final Map<ZKStoreOpWatcher, CompletableFuture<Void>> watcherMap;
  private final List<LostConnectionObserver> lostConnectionObservers;
  private final String rootLatchPath;

  ZKLinearizableStore(CuratorFramework zkClient, String rootLatchPath) {
    this.zkClient = zkClient;
    this.watcherMap = new ConcurrentHashMap<>();
    this.lostConnectionObservers = new CopyOnWriteArrayList<>();
    this.rootLatchPath = rootLatchPath;
  }

  // allows to check if the leader election mechanism for a given service no longer exists without
  // having to
  // join the election. Mainly useful during rolling upgrades. The assumption here is that during
  // rolling upgrade,
  // the old mechanism will never return back when we roll in the new mechanism.
  @Override
  public boolean electionPathExists(final String name) {
    final String latchPath = rootLatchPath + name;
    boolean leaderElectionOn = false;
    try {
      if (zkClient.checkExists().forPath(latchPath) != null) {
        List<String> allChildren = zkClient.getChildren().forPath(latchPath);
        leaderElectionOn = !allChildren.isEmpty();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return leaderElectionOn;
  }

  @Override
  public void executeMulti(PathCommand[] commands)
      throws PathMissingException, PathExistsException {
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
  public byte[] getData(String fullPath, CompletableFuture<Void> onDataChanged)
      throws PathMissingException {
    try {
      final ZKStoreOpWatcher watcher =
          new ZKStoreOpWatcher(fullPath, onDataChanged, Watcher.Event.EventType.NodeDataChanged);
      byte[] data;
      if (onDataChanged == null || watcherMap.containsKey(watcher)) {
        data = zkClient.getData().forPath(fullPath);
      } else {
        watcherMap.put(watcher, onDataChanged);
        data = zkClient.getData().usingWatcher(watcher).forPath(fullPath);
      }
      return data;
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
      return s == null
          ? null
          : new Stats() {
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

            @Override
            public long getSessionId() {
              return s.getEphemeralOwner();
            }
          };
    } catch (Exception e) {
      throw new StoreFatalException(e);
    }
  }

  @Override
  public CompletableFuture<Void> whenDeleted(String fullPath) throws PathMissingException {
    // use asserts here instead of pre-conditions as only trusted clients use this interface as of
    // now
    assert fullPath != null;
    final CompletableFuture<Void> onPathDeletion = new CompletableFuture<>();
    CompletableFuture<Void> actual;
    final ZKStoreOpWatcher watcher =
        new ZKStoreOpWatcher(fullPath, onPathDeletion, Watcher.Event.EventType.NodeDeleted);
    actual =
        watcherMap.compute(
            watcher,
            (k, v) -> {
              if (v == null) {
                try {
                  final boolean exists =
                      (zkClient.checkExists().usingWatcher(watcher).forPath(fullPath) != null);
                  return exists ? onPathDeletion : null;
                } catch (Exception e) {
                  throw new StoreFatalException(e);
                }
              } else {
                return v;
              }
            });
    if (actual == null) {
      throw new PathMissingException(fullPath);
    }
    return actual;
  }

  @Override
  public CompletableFuture<Void> whenCreated(String fullPath) throws PathExistsException {
    // use asserts here instead of pre-conditions as only trusted clients use this interface as of
    // now
    assert fullPath != null;
    final CompletableFuture<Void> onPathCreation = new CompletableFuture<>();
    CompletableFuture<Void> actual;
    final ZKStoreOpWatcher watcher =
        new ZKStoreOpWatcher(fullPath, onPathCreation, Watcher.Event.EventType.NodeCreated);
    actual =
        watcherMap.compute(
            watcher,
            (k, v) -> {
              if (v == null) {
                try {
                  final boolean exists =
                      (zkClient.checkExists().usingWatcher(watcher).forPath(fullPath) != null);
                  return exists ? null : onPathCreation;
                } catch (Exception e) {
                  throw new StoreFatalException(e);
                }
              } else {
                return v;
              }
            });
    if (actual == null) {
      throw new PathExistsException(fullPath);
    }
    return actual;
  }

  @Override
  public List<String> getChildren(String fullPath, CompletableFuture<Void> onChildrenChanged)
      throws PathMissingException {
    assert fullPath != null;
    try {
      List<String> origChildren;
      final ZKStoreOpWatcher watcher =
          new ZKStoreOpWatcher(
              fullPath, onChildrenChanged, Watcher.Event.EventType.NodeChildrenChanged);
      if (onChildrenChanged == null || watcherMap.containsKey(watcher)) {
        origChildren = zkClient.getChildren().forPath(fullPath);
      } else {
        watcherMap.put(watcher, onChildrenChanged);
        origChildren = zkClient.getChildren().usingWatcher(watcher).forPath(fullPath);
        watcher.setLastSeen(origChildren);
      }
      return origChildren;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void registerLostConnectionObserver(LostConnectionObserver observer) {
    LOGGER.info("Setting external connection lost observer");
    lostConnectionObservers.add(observer);
  }

  private void checkResults(List<CuratorTransactionResult> results) {
    for (CuratorTransactionResult result : results) {
      Preconditions.checkArgument(
          result.getError() == KeeperException.Code.OK.intValue(),
          "Unexpected failure result on Path " + result.getForPath());
    }
  }

  @Override
  public void notifyLostConnection() {
    // session is lost, notify registered observers
    LOGGER.info("Notifying connection lost to all external registered observers");
    watcherMap.clear();
    lostConnectionObservers.forEach(LostConnectionObserver::notifyLostConnection);
  }

  @Override
  public void notifyConnectionRegainedAfterLost() {
    LOGGER.info("Notifying Reconnection to all external registered observers");
    lostConnectionObservers.forEach(LostConnectionObserver::notifyConnectionRegainedAfterLost);
  }

  private interface Op {
    CuratorOp createOp(PathCommand cmd) throws Exception;

    void doOp(PathCommand cmd) throws Exception;
  }

  private final class CreateEphemeralOp implements Op {
    @Override
    public CuratorOp createOp(PathCommand cmd) throws Exception {
      byte[] dataToSend =
          (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      return zkClient
          .transactionOp()
          .create()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(cmd.getFullPath(), dataToSend);
    }

    @Override
    public void doOp(PathCommand cmd) throws Exception {
      byte[] dataToSend =
          (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      String ret =
          zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(cmd.getFullPath(), dataToSend);
      cmd.setReturnValue(ret);
    }
  }

  private final class CreateEphemeralSequentialOp implements Op {
    @Override
    public CuratorOp createOp(PathCommand cmd) throws Exception {
      byte[] dataToSend =
          (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      return zkClient
          .transactionOp()
          .create()
          .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
          .forPath(cmd.getFullPath(), dataToSend);
    }

    @Override
    public void doOp(PathCommand cmd) throws Exception {
      byte[] dataToSend =
          (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      String ret =
          zkClient
              .create()
              .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
              .forPath(cmd.getFullPath(), dataToSend);
      cmd.setReturnValue(ret);
    }
  }

  private final class CreatePersistentOp implements Op {
    @Override
    public CuratorOp createOp(PathCommand cmd) throws Exception {
      byte[] dataToSend =
          (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      return zkClient
          .transactionOp()
          .create()
          .withMode(CreateMode.PERSISTENT)
          .forPath(cmd.getFullPath(), dataToSend);
    }

    @Override
    public void doOp(PathCommand cmd) throws Exception {
      byte[] dataToSend =
          (cmd.getData() == null || cmd.getData().length == 0) ? null : cmd.getData();
      String ret =
          zkClient
              .create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .forPath(cmd.getFullPath(), dataToSend);
      cmd.setReturnValue(ret);
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
      throw new IllegalArgumentException(
          "Unimplemented hierarchical store operation type " + cmd.getCommandType());
    }

    @Override
    public void doOp(PathCommand cmd) {
      throw new IllegalArgumentException(
          "Unimplemented hierarchical store operation type " + cmd.getCommandType());
    }
  }

  private final class ZKStoreOpWatcher implements CuratorWatcher {
    private final String fullPath;
    private final CompletableFuture<Void> onOpComplete;
    private final Watcher.Event.EventType expectedEventType;
    private final AtomicReference<List<String>> lastSeenChildren;

    private ZKStoreOpWatcher(
        String fullPath, CompletableFuture<Void> onOpComplete, Watcher.Event.EventType eventType) {
      this.fullPath = fullPath;
      this.onOpComplete = onOpComplete;
      this.expectedEventType = eventType;
      this.lastSeenChildren = new AtomicReference<>(null);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
      LOGGER.debug("Watcher Event {} triggered for path {}", watchedEvent.getType(), fullPath);
      if (watchedEvent.getType().equals(expectedEventType)) {
        watcherMap.remove(this);
        onOpComplete.complete(null);
      } else {
        if (Watcher.Event.EventType.None.equals(watchedEvent.getType())
            && Watcher.Event.KeeperState.SyncConnected.equals(watchedEvent.getState())) {
          LOGGER.info(
              "Reconnect Event Received for watcher of type {} on path {}",
              expectedEventType,
              fullPath);
          recover();
        } else {
          LOGGER.debug(
              "Unknown watcher event {} received for path {}", watchedEvent.getType(), fullPath);
        }
      }
    }

    public void setLastSeen(List<String> origChildren) {
      lastSeenChildren.set(origChildren);
    }

    private void recover() {
      switch (expectedEventType) {
        case NodeDeleted:
          try {
            final boolean exists =
                (zkClient.checkExists().usingWatcher(this).forPath(fullPath) != null);
            if (!exists) {
              watcherMap.remove(this);
              // node is found to be deleted on reconnection. Mark operation complete.
              if (!onOpComplete.isDone()) {
                onOpComplete.complete(null);
              }
            }
          } catch (Exception e) {
            LOGGER.warn(
                "Internal Error: Unexpected exception while recovering deletion watcher for {}",
                fullPath,
                e);
          }
          break;
        case NodeCreated:
          try {
            final boolean exists =
                (zkClient.checkExists().usingWatcher(this).forPath(fullPath) != null);
            if (exists) {
              // node is found to be created on reconnection. Mark operation complete.
              watcherMap.remove(this);
              if (!onOpComplete.isDone()) {
                onOpComplete.complete(null);
              }
            }
          } catch (Exception e) {
            LOGGER.warn(
                "Internal Error: Unexpected exception while recovering creation watcher for {}",
                fullPath,
                e);
          }
          break;
        case NodeChildrenChanged:
          try {
            final List<String> currentSeen =
                zkClient.getChildren().usingWatcher(this).forPath(fullPath);
            // assumption is that even a change in order denotes a difference
            if (!currentSeen.equals(lastSeenChildren.get())) {
              watcherMap.remove(this);
              if (!onOpComplete.isDone()) {
                onOpComplete.complete(null);
              }
            }
          } catch (Exception e) {
            LOGGER.warn("Internal Error: Unexpected exception while recovering watcher", e);
          }
          break;
        case NodeDataChanged:
          watcherMap.remove(this);
          if (!onOpComplete.isDone()) {
            onOpComplete.complete(null);
          }
          break;
        default:
          LOGGER.debug("Unknown event type {}", expectedEventType);
          break;
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
      return fullPath.equals(that.fullPath) && expectedEventType == that.expectedEventType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fullPath, expectedEventType);
    }
  }
}
