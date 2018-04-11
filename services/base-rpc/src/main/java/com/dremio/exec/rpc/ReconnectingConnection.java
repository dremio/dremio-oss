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
package com.dremio.exec.rpc;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.dremio.exec.rpc.RpcConnectionHandler.FailureType;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.protobuf.MessageLite;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Manager all connections between two particular bits.
 */
public abstract class ReconnectingConnection<CONNECTION_TYPE extends RemoteConnection, OUTBOUND_HANDSHAKE extends MessageLite>
    implements Closeable {
  private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());


  private final AtomicReference<CONNECTION_TYPE> connectionHolder = new AtomicReference<CONNECTION_TYPE>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final String host;
  private final int port;
  private final OUTBOUND_HANDSHAKE handshake;
  private final String name;

  public ReconnectingConnection(String name, OUTBOUND_HANDSHAKE handshake, String host, int port) {
    Preconditions.checkNotNull(host);
    Preconditions.checkNotNull(name);
    Preconditions.checkArgument(port > 0);
    this.host = host;
    this.port = port;
    this.name = name;
    this.handshake = handshake;
  }

  public ReconnectingConnection(OUTBOUND_HANDSHAKE handshake, String host, int port) {
    this("unknown", handshake, host, port);
  }

  protected abstract BasicClient<?, CONNECTION_TYPE, OUTBOUND_HANDSHAKE, ?> getNewClient();

  public <R extends MessageLite, C extends RpcCommand<R, CONNECTION_TYPE>> void runCommand(C cmd) {
//    logger.info(String.format("Running command %s sending to host %s:%d", cmd, host, port));
    if (closed.get()) {

      cmd.connectionFailed(FailureType.CONNECTION, new IOException("Connection has been closed"));
    }
    CONNECTION_TYPE connection = connectionHolder.get();
    if (connection != null) {
      if (connection.isActive()) {
        cmd.connectionAvailable(connection);
//        logger.info("Connection available and active, command run inline.");
        return;
      } else {
        // remove the old connection. (don't worry if we fail since someone else should have done it.
        connectionHolder.compareAndSet(connection, null);
      }
    }

    /**
     * We've arrived here without a connection, let's make sure only one of us makes a connection. (fyi, another
     * endpoint could create a reverse connection
     **/
    synchronized (this) {
      connection = connectionHolder.get();
      if (connection != null) {
        cmd.connectionAvailable(connection);

      } else {
        logger.info("[{}]: No connection active, opening new connection to {}:{}.", name, host, port);
        BasicClient<?, CONNECTION_TYPE, OUTBOUND_HANDSHAKE, ?> client = getNewClient();
        ConnectionListeningFuture<R, C> future = new ConnectionListeningFuture<R, C>(cmd);
        client.connectAsClient(future, handshake, host, port);
//        logger.info("Connection available and active, command now being run inline.");
        future.waitAndRun();
//        logger.info("Connection available. Command now run.");
      }
      return;

    }
  }

  public class ConnectionListeningFuture<R extends MessageLite, C extends RpcCommand<R, CONNECTION_TYPE>> extends
      AbstractFuture<CONNECTION_TYPE> implements RpcConnectionHandler<CONNECTION_TYPE> {

    private C cmd;

    public ConnectionListeningFuture(C cmd) {
      super();
      this.cmd = cmd;
    }

    /**
     * Called by
     */
    public void waitAndRun() {
      boolean isInterrupted = false;

      // We want to wait for at least 120 secs when interrupts occur. Establishing a connection fails/succeeds quickly,
      // So there is no point propagating the interruption as failure immediately.
      long remainingWaitTimeMills = 120000;
      long startTime = System.currentTimeMillis();

      while(true) {
        try {
          //        logger.debug("Waiting for connection.");
          CONNECTION_TYPE connection = this.get(remainingWaitTimeMills, TimeUnit.MILLISECONDS);

          if (connection == null) {
            //          logger.debug("Connection failed.");
          } else {
            //          logger.debug("Connection received. {}", connection);
            cmd.connectionSucceeded(connection);
            //          logger.debug("Finished connection succeeded activity.");
          }
          break;
        } catch (final InterruptedException interruptEx) {
          remainingWaitTimeMills -= (System.currentTimeMillis() - startTime);
          startTime = System.currentTimeMillis();
          isInterrupted = true;
          if (remainingWaitTimeMills < 1) {
            cmd.connectionFailed(FailureType.CONNECTION, interruptEx);
            break;
          }
          // Ignore the interrupt and continue to wait until we elapse remainingWaitTimeMills.
        } catch (final ExecutionException | TimeoutException ex) {
          logger.error("Failed to establish connection", ex);
          cmd.connectionFailed(FailureType.CONNECTION, ex);
          break;
        }
      }

      if (isInterrupted) {
        // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
        // interruption and respond to it if it wants to.
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void connectionFailed(com.dremio.exec.rpc.RpcConnectionHandler.FailureType type, Throwable t) {
      set(null);
      cmd.connectionFailed(type, t);
    }

    @Override
    public void connectionSucceeded(CONNECTION_TYPE incoming) {
      CONNECTION_TYPE connection = connectionHolder.get();
      while (true) {
        boolean setted = connectionHolder.compareAndSet(null, incoming);
        if (setted) {
          connection = incoming;
          break;
        }
        connection = connectionHolder.get();
        if (connection != null) {
          break;
        }
      }

      set(connection);

    }

  }

  /** Factory for close handlers **/
  public class CloseHandlerCreator {
    public ChannelFutureListener getHandler(CONNECTION_TYPE connection,
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
}
