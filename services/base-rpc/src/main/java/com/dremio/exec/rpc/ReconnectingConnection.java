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
package com.dremio.exec.rpc;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.dremio.common.SerializedExecutor;
import com.dremio.exec.rpc.RpcConnectionHandler.FailureType;
import com.dremio.telemetry.api.metrics.Counter;
import com.dremio.telemetry.api.metrics.Metrics;
import com.dremio.telemetry.api.metrics.Metrics.ResetType;
import com.google.common.base.Preconditions;
import com.google.protobuf.MessageLite;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * Manages connection between a pair of servers.
 */
public abstract class ReconnectingConnection<CONNECTION_TYPE extends RemoteConnection, OUTBOUND_HANDSHAKE extends MessageLite>
    implements Closeable {
  private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

  private static final Counter CONNECTION_BREAK_15M = Metrics.newCounter(Metrics.join("rpc", "failure_15m"), ResetType.PERIODIC_15M);
  private static final Counter CONNECTION_BREAK_1D = Metrics.newCounter(Metrics.join("rpc", "failure_1d"), ResetType.PERIODIC_1D);

  /**
   * Amount of time to wait after failing to establishing a connection before establishing again.
   */
  private static final long LOST_CONNECTION_REATTEMPT = TimeUnit.MINUTES.toMillis(2);

  /**
   * Amount of time to attempt a connection repeatedly until giving up.
   */
  private static final long CONNECTION_SUCCESS_TIMEOUT = TimeUnit.MINUTES.toMillis(1);

  /**
   * Amount of time to wait between each attempt before trying again (within the CONNECTION_SUCCESS_TIMEOUT).
   */
  private static final long TIME_BETWEEN_ATTEMPT = TimeUnit.SECONDS.toMillis(5);

  private final AtomicReference<CONNECTION_TYPE> connectionHolder = new AtomicReference<CONNECTION_TYPE>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final String host;
  private final int port;
  private final OUTBOUND_HANDSHAKE handshake;
  private final String name;
  private final Exec connector;
  private final long lostConnectionReattemptMS;
  private final long connectionSuccessTimeoutMS;
  private final long timeBetweenAttemptMS;

  private volatile ConnectionFailure lastConnectionFailure;

  public ReconnectingConnection(String name, OUTBOUND_HANDSHAKE handshake, String host, int port) {
    this(name, handshake, host, port, LOST_CONNECTION_REATTEMPT, CONNECTION_SUCCESS_TIMEOUT, TIME_BETWEEN_ATTEMPT);
  }

  public ReconnectingConnection(String name, OUTBOUND_HANDSHAKE handshake, String host, int port, long lostConnectionReattemptMS, long connectionSuccessTimeoutMS, long timeBetweenAttemptMS) {
    Preconditions.checkNotNull(host);
    Preconditions.checkNotNull(name);
    Preconditions.checkArgument(port > 0);
    this.host = host;
    this.port = port;
    this.name = name;
    this.handshake = handshake;
    this.connector = new Exec();
    this.lostConnectionReattemptMS = lostConnectionReattemptMS;
    this.connectionSuccessTimeoutMS = connectionSuccessTimeoutMS;
    this.timeBetweenAttemptMS = timeBetweenAttemptMS;
  }

  protected abstract AbstractClient<?, CONNECTION_TYPE, OUTBOUND_HANDSHAKE> getNewClient() throws RpcException;

  /**
   * Runs the RPC command on this connection. If the underlying connection is not active, a new connection is
   * established, and the command is run (so the caller is unaware if the connection already exists). Although
   * communication may be bi-directional, the host that initiates and successfully establishes the connection is the
   * client in the pair.
   *
   * @param cmd command to run
   * @param <R> (unused)
   * @param <C> command type
   */
  public <R extends MessageLite, C extends RpcCommand<R, CONNECTION_TYPE>> void runCommand(C cmd) {

    if (closed.get()) {
      cmd.connectionFailed(FailureType.CONNECTION, new IOException("Connection has been closed: " + toString()));
    }

    ConnectionRunner r = new ConnectionRunner();

    // get a connection (possibly including making the connection). The actual connection may be made in a thread already working on that.
    connector.execute(r);

    // execute the actual command within the calling thread (not the connecting thread).
    r.executeCommand(cmd);
  }

  /** Factory for close handlers **/
  public class CloseHandlerCreator {
    public ChannelFutureListener getHandler(
        CONNECTION_TYPE connection,
        ChannelFutureListener parent) {
      return new CloseHandler(connection, parent);
    }
  }

  /**
   * Listens for connection closes and clears connection holder.
   */
  protected class CloseHandler implements ChannelFutureListener {
    private CONNECTION_TYPE connection;
    private ChannelFutureListener parent;

    public CloseHandler(CONNECTION_TYPE connection, ChannelFutureListener parent) {
      super();
      this.connection = connection;
      this.parent = parent;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      connectionHolder.compareAndSet(connection, null);
      parent.operationComplete(future);
    }

  }

  public CloseHandlerCreator getCloseHandlerCreator() {
    return new CloseHandlerCreator();
  }

  public void addExternalConnection(CONNECTION_TYPE connection) {
    // if the connection holder is not set, set it to this incoming connection. We'll simply ignore if already set.
    this.connectionHolder.compareAndSet(null, connection);
  }

  @Override
  public void close() {
    if (closed.getAndSet(true)) {
      // Connection was already closed. Let's print out
      logger.info("Attempting to close connection again");
    }

    CONNECTION_TYPE c = connectionHolder.getAndSet(null);
    if (c != null) {
      try {
        c.getChannel().close().sync();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * System that serializes the creation of a connection, making others wait until the connection is established.
   */
  private class Exec extends SerializedExecutor<ConnectionRunner> {

    public Exec() {
      super(name, r -> r.run() /** same thread **/, false);
    }

    @Override
    protected void runException(ConnectionRunner command, Throwable t) {
      command.futureConnection.complete(new ConnectionResult(t));
    }

  }

  /**
   * The class is responsible for establishing a connection if one does not exist, returning one if it does exist.
   */
  private class ConnectionRunner implements Runnable {

    private final CompletableFuture<ConnectionResult> futureConnection = new CompletableFuture<ConnectionResult>();

    @Override
    public void run() {

      // try to use the active connection. If someone else is setting, restart with the new value.
      while(true) {
        // first, try to get existing connection.
        final CONNECTION_TYPE conn = connectionHolder.get();
        if(conn != null && conn.isActive()) {
          futureConnection.complete(new ConnectionResult(false, conn));
          return;
        }

        // bad connection, clear it.
        if(!connectionHolder.compareAndSet(conn, null)) {
          // if we failed to clear, someone changed the connection, restart the process.
          continue;
        }

        break;
      }

      // if we failed recently, fail this command immediately.
      final ConnectionFailure failure = lastConnectionFailure;
      if(failure != null && failure.isStillValid()) {
        futureConnection.complete(new ConnectionResult(failure));
        return;
      }

      logger.info("[{}]: No connection active, opening new connection to {}:{}.", name, host, port);
      final long runUntil = System.currentTimeMillis() + connectionSuccessTimeoutMS;


      ConnectionResult lastResult = null;
      // keep attempting a connection until we hit the timeout.
      while (System.currentTimeMillis() < runUntil) {

        try {
          final ConnectionResult result = attempt(runUntil);
          if(result.ok()) {
            futureConnection.complete(result);
            return;
          } else {
            // we'll attempt again.

            lastResult = result;
            try {
              final long currentTime = System.currentTimeMillis();
              if (currentTime + timeBetweenAttemptMS < runUntil) {
                Thread.sleep(timeBetweenAttemptMS);
              } else {
                Thread.sleep(currentTime < runUntil ? (runUntil - currentTime) : 0);
              }
            } catch (InterruptedException e) {
              // ignore.
            }

          }
        } catch(RpcException e) {
          ConnectionResult failureResult = new ConnectionResult(e);
          lastConnectionFailure = failureResult.failure;
          futureConnection.complete(failureResult);

          // don't wait when the creation of a client occurs.
          return;
        }

      }

      // we failed to complete within the timeout.
      if(lastResult == null) {
        lastResult = new ConnectionResult(new TimeoutException("Unable to connect within requested time for " + toString()));
      }
      lastConnectionFailure = lastResult.failure;
      futureConnection.complete(lastResult);
    }

    private ConnectionResult attempt(long runUntil) throws RpcException {
      ConnectionHandle future = new ConnectionHandle();
      AbstractClient<?, CONNECTION_TYPE, OUTBOUND_HANDSHAKE> client = getNewClient();
      client.connectAsClient(future, handshake, host, port);

      ConnectionResult result = future.waitForFinished(runUntil);
      if (!result.ok()) {
        return result;
      }

      boolean wasSet = connectionHolder.compareAndSet(null, result.connection);
      if(wasSet) {
        return result;
      }

      // we failed to set the new connection, close it.
      result.discard();

      CONNECTION_TYPE outsideSet = connectionHolder.get();
      if(outsideSet == null) {
        // unexpected but let's handle.
        return new ConnectionResult(new IllegalStateException("Connection was attempted but then identified as missing " + toString()));
      } else {
        return new ConnectionResult(false, outsideSet);
      }

    }

    /**
     * Once we've done our best to establish a connection, dispatch the provided command.
     *
     * We separate this from establishing connection so that the original thread can execute. If this was done in the
     * run() method, the first thread entering the code would be running all commands under a lock. Instead, we want to
     * quickly get out of the protected block in the common case.
     *
     * @param cmd Command to be executed or failed.
     */
    public void executeCommand(RpcCommand<? extends MessageLite, CONNECTION_TYPE> cmd) {
      try {
        // no need to set timing here since the lower layers have the timeouts necessary.
        ConnectionResult result = futureConnection.get();
        if(result.ok()) {
          if(result.hadToConnect()) {
            cmd.connectionSucceeded(result.connection);
          } else {
            cmd.connectionAvailable(result.connection);
          }
        } else {
          cmd.connectionFailed(result.failure.type, result.failure.throwable);
        }

      } catch (InterruptedException e) {
        // shouldn't happen
        cmd.connectionFailed(FailureType.CONNECTION, e);
      } catch (ExecutionException e) {
        // shouldn't happen
        cmd.connectionFailed(FailureType.CONNECTION, e.getCause());
      }
    }



  }

  /**
   * A connection handler that also synchronizes whether the connection attempt was completed within the serialized
   * executor (and thus usable).
   */
  private class ConnectionHandle implements RpcConnectionHandler<CONNECTION_TYPE> {

    private CompletableFuture<ConnectionResult> conn = new CompletableFuture<>();

    /**
     * Whether this connection attempt too long and should be thrown away.
     */
    private boolean tookTooLong;

    @Override
    public synchronized void connectionSucceeded(CONNECTION_TYPE connection) {
      // this connection is only good if we completed on time and could set it to the connection holder.

      if(!tookTooLong) {
        conn.complete(new ConnectionResult(true, connection));
        return;
      }

      // close channel since it took too long to create.
      try {
        connection.getChannel().close().sync();
      } catch (InterruptedException e) {
        // ignore.
      }

    }

    @Override
    public synchronized void connectionFailed(FailureType type, Throwable t) {
      if(!tookTooLong) {
        conn.complete(new ConnectionResult(new ConnectionFailure(type, t)));
      }
    }

    public ConnectionResult waitForFinished(long untilTime) {
      try {
        return conn.get(Math.max(1, untilTime - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
      } catch (TimeoutException | InterruptedException e) {
        return new ConnectionResult(new ConnectionFailure(FailureType.CONNECTION, e));
      } catch (ExecutionException  e) {
        return new ConnectionResult(new ConnectionFailure(FailureType.CONNECTION, e.getCause()));
      } finally {
        // always set as took too long. Either it did (and thus this setting should be set) or it didn't (and this setting is ignored).
        tookTooLong = true;
      }
    }

  }

  /**
   * The result of a connection operation (whether succesful or not).
   */
  private class ConnectionResult {

    private final boolean hadToConnect;
    private final CONNECTION_TYPE connection;
    private final ConnectionFailure failure;

    public ConnectionResult(boolean hadToConnect, CONNECTION_TYPE connection) {
      super();
      this.hadToConnect = hadToConnect;
      this.connection = connection;
      this.failure = null;
    }

    public ConnectionResult(ConnectionFailure failure) {
      super();
      this.hadToConnect = false;
      this.connection = null;
      this.failure = failure;
    }

    public ConnectionResult(Throwable t) {
      this(new ConnectionFailure(FailureType.CONNECTION, t));
    }

    public boolean ok(){
      return failure == null;
    }

    public boolean hadToConnect() {
      return hadToConnect;
    }

    public void discard() {
      if (connection != null) {
        try {
          connection.getChannel().close().sync();
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }
  }

  /**
   * Internal class to encapsulate a failure and type.
   */
  private class ConnectionFailure {

    private final long validUntil = System.currentTimeMillis() + lostConnectionReattemptMS;
    private final FailureType type;
    private final Throwable throwable;

    public ConnectionFailure(FailureType type, Throwable throwable) {
      this.type = type;
      this.throwable = throwable;
      CONNECTION_BREAK_15M.increment();
      CONNECTION_BREAK_1D.increment();
    }

    /**
     * Whether the previous connection failure is still valid.
     * @return True if still a valid failure.
     */
    private boolean isStillValid() {
      return System.currentTimeMillis() < validUntil;
    }

  }

  public String toString() {
    return String.format("[%s] %s:%d", name, host, port);
  }

}
