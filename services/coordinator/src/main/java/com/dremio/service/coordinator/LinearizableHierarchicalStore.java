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
package com.dremio.service.coordinator;

import com.dremio.service.coordinator.exceptions.PathExistsException;
import com.dremio.service.coordinator.exceptions.PathMissingException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Interface to a <i>linearizable</i> store that understands a hierarchical path, with each node in
 * the path having capability to store small amounts of data.
 *
 * <p>All implementations must guarantee that writes are linearizable. The reads need not be
 * linearizable, but they need to be sequentially consistent.
 */
public interface LinearizableHierarchicalStore {
  enum CommandType {
    CREATE_EPHEMERAL, // create a path that is destroyed when the creator 'exits' the cluster
    CREATE_EPHEMERAL_SEQUENTIAL, // create an ephemeral path that is never repeated
    CREATE_PERSISTENT, // create a path and associated data that is persistent
    SET_DATA, // store data against a path
    DELETE // delete the path
  }

  interface Stats {
    long getCreationTime();

    long getLastModifiedTime();

    int getNumChanges();

    long getSessionId();
  }

  class PathCommand {
    private final CommandType commandType;
    private final String fullPath;
    private final byte[] data;
    private String returnValue;

    public PathCommand(CommandType commandType, String fullPath, byte[] data) {
      this.commandType = commandType;
      this.fullPath = fullPath;
      this.data = data;
      this.returnValue = null;
    }

    public PathCommand(CommandType commandType, String fullPath) {
      this(commandType, fullPath, null);
    }

    public CommandType getCommandType() {
      return commandType;
    }

    public String getFullPath() {
      return fullPath;
    }

    public byte[] getData() {
      return data;
    }

    public String getReturnValue() {
      return returnValue;
    }

    public void setReturnValue(String returnValue) {
      this.returnValue = returnValue;
    }
  }

  /**
   * Executes a set of commands in an all-or-nothing (atomic) fashion.
   *
   * <p>Implementations must ensure transactionality or at the very least compensate and do reverse
   * operations, if any of the commands fails.
   *
   * @param commands Set of commands to execute
   * @throws PathExistsException If path already exists
   * @throws PathMissingException If parent path (create) or given path (other operations) was
   *     missing
   */
  void executeMulti(PathCommand[] commands) throws PathExistsException, PathMissingException;

  /**
   * Executes a single command.
   *
   * @param command Command to execute
   * @throws PathExistsException If path already exists
   * @throws PathMissingException If parent path (create) or given path (other operations) was
   *     missing
   */
  void executeSingle(PathCommand command) throws PathExistsException, PathMissingException;

  /**
   * Get data from a path.
   *
   * @param path Path
   * @return data stored in the path
   * @throws PathMissingException If given path was missing
   */
  byte[] getData(String path) throws PathMissingException;

  /**
   * Get data from a path and notify when data changes.
   *
   * @param fullPath node path
   * @param onDataChanged future to complete when data changes
   * @return current data
   * @throws PathMissingException if given path was missing
   */
  byte[] getData(String fullPath, CompletableFuture<Void> onDataChanged)
      throws PathMissingException;

  /**
   * Check if path exists.
   *
   * <p>Call this function for merely checking for path existence/non-existence
   *
   * @param path Path
   * @return true if path exists, false otherwise
   */
  boolean checkExists(String path);

  /**
   * Get stats about a given path.
   *
   * @param path Path to get stats from
   * @return Stats
   */
  Stats getStats(String path);

  /**
   * Check if path exists and complete the returned future when the path gets deleted.
   *
   * <p>Call this function if the expectation is an existence of the path and the caller wants to
   * get notified when the path disappears, through the returned future. Implementations must
   * guarantee completion of the future when the path disappears, regardless of underlying failures.
   *
   * <p><strong>NOTE:</strong> If the completion handling of the returned future is light weight
   * then the next handler in the chain can be synchronous as shown in the example below.
   *
   * <pre>{@code
   * store.whenDeleted("/test/path").thenRun(() -> veryFastOp());
   * }</pre>
   *
   * On the other hand, if the completion handling of the returned future is heavy weight then the
   * next handler in the chain MUST be asynchronous as shown in the example below. It is also
   * advisable to explicitly provide an executor instead of using the common fork join pool for such
   * slow operations.
   *
   * <pre>{@code
   * store.whenDeleted("/test/path").thenRunAsync(() -> slowOp(), myExecutor);
   * }</pre>
   *
   * @param path Path
   * @throws PathMissingException path does not exist
   */
  CompletableFuture<Void> whenDeleted(String path) throws PathMissingException;

  /**
   * Check if path does not exist and notify when it gets created.
   *
   * <p>Call this function if the expectation is a non-existence of the path and the caller wants to
   * get notified when the path appears, through the returned future. Implementations must guarantee
   * completion of the future when the path appears, regardless of underlying failures.
   *
   * <p><strong>NOTE:</strong> If the completion handling of the returned future is light weight
   * then the next handler in the chain can be synchronous as shown in the example below.
   *
   * <pre>{@code
   * store.whenCreated("/test/path").thenRun(() -> veryFastOp());
   * }</pre>
   *
   * On the other hand, if the completion handling of the returned future is heavy weight then the
   * next handler in the chain MUST be asynchronous as shown in the example below. It is also
   * advisable to provide your own executor for such slow operations rather than using the common
   * fork join pool.
   *
   * <pre>{@code
   * store.whenCreated("/test/path").thenRunAsync(() -> slowOp(), myExecutor);
   * }</pre>
   *
   * @param path Path
   * @throws PathExistsException if Path already exists
   */
  CompletableFuture<Void> whenCreated(String path) throws PathExistsException;

  /**
   * Get all immediate children of a given path.
   *
   * <p>If {@code onChangeFuture} is specified, the implementation must guarantee completing it on
   * any change detected under the parent path, regardless of any underlying issues (such as
   * disconnects)
   *
   * <p><strong>NOTE:</strong> If the completion handling of the {@code onChangeFuture} is light
   * weight, then the next handler in the chain can be synchronous as shown in the example below:
   *
   * <pre>{@code
   * CompletableFuture<Void> onChildrenChanged = new CompletableFuture<>();
   * onChildrenChanged.thenRun(() -> veryFastOp());
   * store.getChildren("/test/path", onChildrenChanged);
   * }</pre>
   *
   * On the other hand, if the completion handling of the {@code onChangeFuture} is heavy weight,
   * then the next handler in the chain MUST be asynchronous as shown in the example below. It is
   * also advisable to explicitly provide an executor instead of using the common fork join pool for
   * such slow operations.
   *
   * <pre>{@code
   * CompletableFuture<Void> onChildrenChanged = new CompletableFuture<>();
   * onChildrenChanged.thenRun(() -> slowOp(), myExecutor);
   * store.getChildren("/test/path", onChildrenChanged);
   * }</pre>
   *
   * @param path parent path
   * @param onChangeFuture if non null, completes this future if there is any change in the children
   * @return List of existing children
   * @throws PathMissingException if given parent path is missing
   */
  List<String> getChildren(String path, CompletableFuture<Void> onChangeFuture)
      throws PathMissingException;

  /**
   * Registers a lost connection observer to the underlying hierarchical store.
   *
   * <p>This observer is invoked by the store if and only if it receives an unrecoverable session
   * lost error from the underlying store.
   *
   * @param observer the observer to invoke on a session lost error
   */
  void registerLostConnectionObserver(LostConnectionObserver observer);

  /**
   * Returns true if a leader election path exists for the given service,
   *
   * <p><i>NOTE:</i> This is only temporary and will be removed in the next release.
   *
   * @param name name of the service
   * @return true if a leader election path exists with children
   */
  boolean electionPathExists(String name);
}
