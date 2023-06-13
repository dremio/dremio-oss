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

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.exec.rpc.RpcConnectionHandler.FailureType;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.SocketChannel;

/**
 * Test the reconnecting connection for key behaviors
 */
@SuppressWarnings({"unchecked"})
public class TestReconnectingConnection {


  @Test
  public void ensureDirectFailureOnRpcException() {
    conn().runCommand(Cmd.expectFail());
  }

  @Test
  public void ensureSecondSuccess() {
    conn(
        r -> r.connectionFailed(FailureType.CONNECTION, new IllegalStateException()),
        r -> r.connectionSucceeded(newRemoteConnection())
        ).runCommand(Cmd.expectSucceed());
  }

  @Test
  public void ensureTimeoutFailAndRecover() {
    TestReConnection c = conn(200,100,1,
        r -> {
          wt(100);
          r.connectionFailed(FailureType.CONNECTION, new IllegalStateException());
        },
        r -> r.connectionSucceeded(newRemoteConnection())
        );

    c.runCommand(Cmd.expectFail());
    wt(200);
    c.runCommand(Cmd.expectSucceed());
  }

  @Test
  public void ensureSuccessThenAvailable() {
    TestReConnection c = conn(r -> r.connectionSucceeded(newRemoteConnection()));
    c.runCommand(Cmd.expectSucceed());
    c.runCommand(Cmd.expectAvailable());
  }

  @Test
  public void ensureClosedFails() {
    TestReConnection c = conn();
    c.close();
    conn().runCommand(Cmd.expectFail());
  }

  @Test
  public void ensureInActiveCausesReconnection() {
    TestReConnection c = conn(
        100, 100, 1,
        r -> r.connectionSucceeded(newBadConnection()),
        r -> {
          wt(200);
          r.connectionFailed(FailureType.CONNECTION, new IllegalStateException());
        },
        r -> r.connectionSucceeded(newRemoteConnection())
        );

    // we assume in the reconnector that the initial connection is good (don't check active).
    c.runCommand(Cmd.expectSucceed());

    // second command: should identify connection as inactive and reattempt connection, and get a failure as we go past timeout.
    c.runCommand(Cmd.expectFail());

    // wait long enough that we consider the previous failure not valid and reconnect again.
    wt(300);
    c.runCommand(Cmd.expectSucceed());
    c.runCommand(Cmd.expectAvailable());
  }

  @Test
  public void ensureFirstFailsWaiting() throws InterruptedException, ExecutionException {
    CountDownLatch latch = new CountDownLatch(1);
    TestReConnection c = conn(
        100000, 100, 100,
        r -> {
          try {
            latch.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          r.connectionFailed(FailureType.CONNECTION, new IllegalStateException());
        },
        r -> r.connectionSucceeded(newRemoteConnection())
        );

    CompletableFuture<Void> first = CompletableFuture.runAsync(() -> c.runCommand(Cmd.expectFail()));

    // line up ten more messages that will be waiting on the first message.
    CompletableFuture<Void> rest = CompletableFuture.allOf(IntStream.range(0, 10).mapToObj(i -> CompletableFuture.runAsync(() -> {
      c.runCommand(Cmd.expectFail());
    })).collect(Collectors.toList()).toArray(new CompletableFuture[0]));

    try {
      // check that rest are blocked on the countdown.
      rest.get(0, TimeUnit.SECONDS);
      Assert.fail();
    } catch (TimeoutException e) {
      // ignore.
    }

    // fail first connection
    latch.countDown();

    // check first result failed.
    first.get();

    // check the rest of results failed.
    rest.get();
  }

  private TestReConnection conn(Consumer<RpcConnectionHandler<RemoteConnection>>...consumersArr) {
    return new TestReConnection(10000,10000,1, Stream.of(consumersArr).iterator());
  }

  private static RemoteConnection newRemoteConnection() {
    RemoteConnection conn = Mockito.mock(RemoteConnection.class);
    Mockito.when(conn.isActive()).thenReturn(true);
    return conn;
  }

  private static RemoteConnection newBadConnection() {
    RemoteConnection conn = Mockito.mock(RemoteConnection.class);
    Mockito.when(conn.isActive()).thenReturn(false);
    return conn;
  }

  private TestReConnection conn(long failFor, long tryFor, long tryEach, Consumer<RpcConnectionHandler<RemoteConnection>>...consumersArr) {
    return new TestReConnection(failFor, tryFor, tryEach, Stream.of(consumersArr).iterator());
  }

  private static class TestReConnection extends ReconnectingConnection<RemoteConnection, MessageLite> {

    private Iterator<Consumer<RpcConnectionHandler<RemoteConnection>>> iter;

    public TestReConnection(long failFor, long tryFor, long tryEach, Iterator<Consumer<RpcConnectionHandler<RemoteConnection>>> iter) {
      super("test", Mockito.mock(MessageLite.class), "", 1, failFor, tryFor, tryEach);
      this.iter = iter;
    }

    @Override
    protected String getLocalAddress() {
      return "";
    }

    @Override
    protected ResponseClient getNewClient() throws RpcException {
      if(!iter.hasNext()) {
        throw new RpcException("Failure creating client");
      }
      return new ResponseClient(iter.next());
    }

  }


  private static class Cmd implements RpcCommand<MessageLite, RemoteConnection> {

    @Override
    public void connectionSucceeded(RemoteConnection connection) {
      Assert.fail();
    }

    @Override
    public void connectionFailed(FailureType type, Throwable t) {
      Assert.fail();
    }

    @Override
    public void connectionAvailable(RemoteConnection connection) {
      Assert.fail();
    }

    static Cmd expectFail() {
      return new Cmd() {
        @Override
        public void connectionFailed(FailureType type, Throwable t) {
          //
        }
      };
    }

    static Cmd expectAvailable() {
      return new Cmd() {
        @Override
        public void connectionAvailable(RemoteConnection connection) {
          //
        }
      };
    }

    static Cmd expectSucceed() {
      return new Cmd() {
        @Override
        public void connectionSucceeded(RemoteConnection connection) {
          //
        }
      };
    }
  }

  private static RpcConnectionHandler<RemoteConnection> good(Consumer<RemoteConnection> consumer){
    return new Handler() {
      @Override
      public void connectionSucceeded(RemoteConnection connection) {
        consumer.accept(connection);
      }
    };
  }

  private static RpcConnectionHandler<RemoteConnection> bad(BiConsumer<FailureType, Throwable> consumer){
    return new Handler() {
      @Override
      public void connectionFailed(FailureType type, Throwable t) {
        consumer.accept(type, t);
      }
    };
  }

  private static RpcConnectionHandler<RemoteConnection> noop(){
    return new Handler();
  }

  private static class Handler implements RpcConnectionHandler<RemoteConnection> {
    @Override
    public void connectionSucceeded(RemoteConnection connection) {
      Assert.fail();
    }

    @Override
    public void connectionFailed(FailureType type, Throwable t) {
      Assert.fail();
    }
  }

  private static class ResponseClient extends AbstractClient<EnumLite, RemoteConnection, MessageLite> {

    private final Consumer<RpcConnectionHandler<RemoteConnection>> consumer;

    public ResponseClient(Consumer<RpcConnectionHandler<RemoteConnection>> consumer) {
      super(RpcConfig.newBuilder().name("test").timeout(0).build());
      this.consumer = consumer;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    protected void connectAsClient(RpcConnectionHandler<RemoteConnection> connectionHandler, MessageLite handshakeValue,
        String host, int port) {
      consumer.accept(connectionHandler);
    }

    @Override
    protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Response handle(RemoteConnection connection, int rpcType, byte[] pBody, ByteBuf dBody)
        throws RpcException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RemoteConnection initRemoteConnection(SocketChannel channel) {
      return Mockito.mock(RemoteConnection.class);
    }

  }

  /**
   * Utility wait function to avoid lots of try blocks.
   * @param millis Amount to wait.
   */
  private static void wt(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
