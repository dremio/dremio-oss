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

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.dremio.exec.rpc.RpcConnectionHandler.FailureType;
import com.dremio.ssl.SSLEngineFactory;
import com.google.common.base.Preconditions;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Client side of a RPC system. Supports {@link #send sending} and {@link #handle receiving} data.
 *
 * @param <T>  handshake rpc type
 * @param <R>  connection type
 * @param <HS> handshake request type (send type)
 * @param <HR> handshake response type (receive type)
 */
public abstract class BasicClient<T extends EnumLite, R extends RemoteConnection, HS extends MessageLite, HR extends MessageLite>
    extends AbstractClient<T, R, HS> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicClient.class);

  protected static final String PROTOCOL_DECODER = "protocol-decoder";
  protected static final String HANDSHAKE_REQUESTER = "handshake-requester";
  protected static final String SSL_CLIENT_HANDLER = "ssl-client-handler";
  protected static final String IDLE_STATE_HANDLER = "idle-state-handler";

  private static final OutboundRpcMessage PING_MESSAGE = new OutboundRpcMessage(RpcMode.PING, 0, 0, Acks.OK);

  // The percentage of time that should pass before sending a ping message to ensure server doesn't time us out. For
  // example, if timeout is set to 30 seconds and we set percentage to 0.5, then if no write has happened within 15
  // seconds, the idle state handler will send a ping message.
  private static final double PERCENT_TIMEOUT_BEFORE_SENDING_PING = 0.5;

  private final Class<HR> responseClass;
  private final T handshakeType;
  private final Parser<HR> handshakeParser;

  private final Optional<SSLEngineFactory> engineFactory;
  private final Bootstrap b;

  protected volatile R connection; // null if a channel is uninitialized

  public BasicClient(
      final RpcConfig rpcMapping,
      final ByteBufAllocator alloc,
      final EventLoopGroup eventLoopGroup,
      final T handshakeType,
      final Class<HR> responseClass,
      final Parser<HR> handshakeParser,
      Optional<SSLEngineFactory> engineFactory
  ) throws RpcException {
    super(rpcMapping);
    this.responseClass = responseClass;
    this.handshakeType = handshakeType;
    this.handshakeParser = handshakeParser;

    this.engineFactory = engineFactory;

    final long timeoutInMillis =
        rpcMapping.hasTimeout()
            ? (long) (rpcMapping.getTimeout() * 1000.0 * PERCENT_TIMEOUT_BEFORE_SENDING_PING)
            : -1;

    b = new Bootstrap()
        .group(eventLoopGroup)
        .channel(TransportCheck.getClientSocketChannel())
        .option(ChannelOption.ALLOCATOR, alloc)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30 * 1000)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_RCVBUF, 1 << 17)
        .option(ChannelOption.SO_SNDBUF, 1 << 17)
        .option(ChannelOption.TCP_NODELAY, true)
        .handler(new ChannelInitializer<SocketChannel>() {

          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            connection = initRemoteConnection(ch);

            // each handler in the pipeline is created per connection

            ch.closeFuture().addListener(newCloseListener(ch, connection));

            final ChannelPipeline pipe = ch.pipeline();

            pipe.addLast(PROTOCOL_ENCODER, new RpcEncoder("c-" + rpcConfig.getName()));
            pipe.addLast(PROTOCOL_DECODER, newDecoder(connection.getAllocator()));
            pipe.addLast(HANDSHAKE_HANDLER, new ClientHandshakeHandler());

            if (timeoutInMillis != -1) {
              pipe.addLast(IDLE_STATE_HANDLER, new IdlePingHandler(timeoutInMillis));
            }

            pipe.addLast(MESSAGE_HANDLER, new InboundHandler(connection));
            pipe.addLast(EXCEPTION_HANDLER, new RpcExceptionHandler<>(connection));
          }
        });
  }

  /**
   * Return new message decoder to be added to channel pipeline.
   *
   * @param allocator allocator
   * @return message decoder
   */
  public abstract MessageDecoder newDecoder(BufferAllocator allocator);

  /**
   * Validate the handshake message.
   *
   * @param handshake handshake
   * @throws RpcException if handshake message in invalid
   */
  protected abstract void validateHandshake(HR handshake) throws RpcException;

  /**
   * Callback invoked on connection finalization.
   *
   * @param handshake  handshake
   * @param connection connection
   */
  protected abstract void finalizeConnection(HR handshake, R connection);

  /**
   * Delegates to {@link RpcBus#send} for this client.
   */
  public <SEND extends MessageLite, RECEIVE extends MessageLite>
  void send(RpcOutcomeListener<RECEIVE> listener, T rpcType, SEND protobufBody, Class<RECEIVE> clazz,
            ByteBuf... dataBodies) {
    super.send(listener, connection, rpcType, protobufBody, clazz, dataBodies);
  }

  /**
   * Delegates to {@link RpcBus#send} for this client.
   */
  public <SEND extends MessageLite, RECEIVE extends MessageLite>
  RpcFuture<RECEIVE> send(T rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    return super.send(connection, rpcType, protobufBody, clazz, dataBodies);
  }

  /**
   * Establish a connection to the server running on {@param host} and {@param port}. After successfully connecting to
   * the server, which may include setting up SSL, send the {@param handshakeValue handshake message} to the server. If
   * handshake is successful, invoke {@link RpcConnectionHandler#connectionSucceeded} on the given
   * {@param connectionHandler}. In case of any failure, invoke {@link RpcConnectionHandler#connectionFailed}, with
   * the reason, on the {@param connectionHandler}.
   *
   * @param connectionHandler connection handler
   * @param handshakeValue    handshake value
   * @param host              server hostname
   * @param port              server port
   */
  protected void connectAsClient(RpcConnectionHandler<R> connectionHandler, HS handshakeValue, String host, int port) {
    ConnectionMultiListener cml = new ConnectionMultiListener(connectionHandler, handshakeValue, host, port);
    b.connect(host, port)
        .addListener(cml.establishmentListener);
  }

  public boolean isActive() {
    return connection != null
        && connection.getChannel() != null
        && connection.getChannel().isActive();
  }

  public void setAutoRead(boolean enableAutoRead) {
    connection.setAutoRead(enableAutoRead);
  }

  @Override
  public void close() {
    logger.debug("Closing client");
    try {
      if (connection != null) {
        connection.getChannel().close().sync();
      }
    } catch (final InterruptedException e) {
      logger.warn("Failure while shutting {}", this.getClass().getName(), e);

      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Holder class for listeners needed to establish a connection to a server, and eventually convey to the requester.
   *
   * A {@link ConnectionEstablishmentListener} is added as listener on the {@link Bootstrap#connect} future. On
   * successfully connecting to the server (i.e. the future is successful), a negotiator is added to the pipeline.
   *
   * 1. If SSL is disabled, the negotiator is a {@link HandshakeRequester}. When the channel becomes active, a
   * handshake message is sent to the server. A {@link HandshakeSendListener} is registered to listen to the server's
   * response to that handshake request. The handshake response from the server is decoded by
   * {@link AbstractHandshakeHandler}, and the response is passed along to the registered {@link HandshakeSendListener}.
   * A {@link HandshakeSendListener#success successful} response from the server is validated and the connection is
   * {@link #finalizeConnection finalized}. And finally {@link RpcConnectionHandler#connectionSucceeded} is invoked on
   * {@param connectionHandler}.
   *
   * 2. If SSL is enabled, the negotiator is a {@link SslHandler} which is configured based on the
   * {@link RpcConfig#sslConfig} for a client. The {@link SslHandler} is added as the first handler in the pipeline.
   * A listener is registered on the {@link SslHandler#handshakeFuture SSL handshake future}, which adds a
   * {@link HandshakeRequester} on success. And then the logic mentioned in (1) applies.
   *
   * On any connection-related, handshake-related or SSL-related failure, the {@param connectionHandler} is notified.
   */
  private class ConnectionMultiListener {

    // listener that acts on the connection establishment event
    private final ConnectionEstablishmentListener establishmentListener = new ConnectionEstablishmentListener();

    // listener that acts on the handshake response event
    private final HandshakeSendListener handshakeSendListener = new HandshakeSendListener();

    private final RpcConnectionHandler<R> connectionHandler;
    private final HS handshakeValue;
    private final String hostName;
    private final int port;

    ConnectionMultiListener(RpcConnectionHandler<R> connectionHandler, HS handshakeValue, String hostName, int port) {
      this.hostName = hostName;
      this.port = port;
      assert connectionHandler != null;
      assert handshakeValue != null;

      this.connectionHandler = connectionHandler;
      this.handshakeValue = handshakeValue;
    }

    /**
     * Listens to connection establishment outcomes, and adds a negotiator accordingly.
     */
    private class ConnectionEstablishmentListener implements GenericFutureListener<ChannelFuture> {

      @Override
      public void operationComplete(ChannelFuture connectionFuture) throws Exception {
        boolean isInterrupted = false;

        // We want to wait for at least 120 secs when interrupts occur. Establishing a connection fails/succeeds
        // quickly, so there is no point propagating the interruption as failure immediately.
        long remainingWaitTimeMillis = 120_000L;
        long startTime = System.currentTimeMillis();
        while (true) {
          try {
            connectionFuture.get(remainingWaitTimeMillis, TimeUnit.MILLISECONDS);
            logger.trace("Connection establishment to '{}' completed with state '{}'", connectionFuture.channel(),
                connectionFuture.isSuccess());
            if (!connectionFuture.isSuccess()) {
              connectionHandler.connectionFailed(FailureType.CONNECTION,
                  new RpcException("General connection failure.", connectionFuture.cause()));
            } else {
              addNegotiator(connectionFuture);
            }
            break;
          } catch (final InterruptedException ie) {
            final long now = System.currentTimeMillis();
            remainingWaitTimeMillis -= (now - startTime);
            startTime = now;
            isInterrupted = true;
            if (remainingWaitTimeMillis < 1) {
              connectionHandler.connectionFailed(FailureType.CONNECTION, ie);
              break;
            }
            // Ignore the interrupt and continue to wait until we elapse remainingWaitTimeMillis.
            // TODO: this listener is invoked AFTER the operation completes.. so who interrupts?
          } catch (final Exception ex) {
            logger.error("Failed to establish connection", ex);
            connectionHandler.connectionFailed(FailureType.CONNECTION, ex);
            break;
          }
        }

        if (isInterrupted) {
          // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
          // interruption and respond to it if it wants to.
          Thread.currentThread().interrupt();
        }
      }

      void addNegotiator(ChannelFuture connectionFuture) throws Exception {
        if (!rpcConfig.getSSLConfig().isPresent()) {
          logger.trace("Adding handshake negotiator on '{}'", connectionFuture.channel());
          addHandshakeRequester(connectionFuture);
          return;
        }

        assert engineFactory.isPresent();
        final SSLEngine clientEngine =
            engineFactory.get()
                .newClientEngine(connectionFuture.channel().alloc(), hostName, port);

        final SslHandler sslHandler = new SslHandler(clientEngine);
        sslHandler.handshakeFuture()
            .addListener(future -> {
              logger.debug("SSL client state '{}' on connection '{}'", future.isSuccess(),
                  connectionFuture.channel());

              if (future.isSuccess()) {
                logger.trace("Adding handshake negotiator on '{}', after SSL succeeded", connectionFuture.channel());
                addHandshakeRequester(connectionFuture);
              } else {
                connectionHandler.connectionFailed(FailureType.CONNECTION,
                    new RpcException("SSL negotiation failed", future.cause()));
              }
            });
        // TODO(DX-12921): sslHandler.setHandshakeTimeoutMillis(sslConfig.getHandshakeTimeoutMillis());

        logger.trace("Adding SSL negotiator on '{}'", connectionFuture.channel());
        connectionFuture.channel()
            .pipeline()
            .addFirst(SSL_CLIENT_HANDLER, sslHandler);
      }

      void addHandshakeRequester(ChannelFuture connectionFuture) {
        connectionFuture.channel()
            .pipeline()
            .addBefore(HANDSHAKE_HANDLER, HANDSHAKE_REQUESTER, new HandshakeRequester());
      }
    }

    void sendHandshake() {
      Preconditions.checkState(connection != null, "connection is not yet initialized");

      // send a handshake on the current thread. This is the only time we will send from within the event thread.
      // We can do this because the connection will not be backed up.
      send(handshakeSendListener, connection, handshakeType, handshakeValue, responseClass, true);
    }

    /**
     * Sends a handshake request to the server when the channel becomes active, or if it is already active. After
     * sending, the handler removes itself from the pipeline.
     */
    private class HandshakeRequester extends ChannelInboundHandlerAdapter {

      private void sendHandshakeAndRemoveSelf(ChannelHandlerContext ctx) {
        sendHandshake();
        ctx.channel().pipeline().remove(this);
      }

      @Override // channel may already be active
      public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isActive()) {
          sendHandshakeAndRemoveSelf(ctx);
        }
      }

      @Override
      public void channelActive(ChannelHandlerContext ctx) throws Exception {
        sendHandshakeAndRemoveSelf(ctx);
        super.channelActive(ctx);
      }
    }

    /**
     * Listens to handshake response from the server, and conveys the outcome to the {@link #connectionHandler}.
     */
    private class HandshakeSendListener implements RpcOutcomeListener<HR> {

      @Override
      public void failed(RpcException ex) {
        logger.debug("Failure while initiating handshake", ex);
        connectionHandler.connectionFailed(FailureType.HANDSHAKE_COMMUNICATION, ex);
      }

      @Override
      public void success(HR value, ByteBuf buffer) {
        logger.debug("Handshake received on {}", connection);
        try {
          validateHandshake(value);
          finalizeConnection(value, connection);

          connectionHandler.connectionSucceeded(connection);
          logger.trace("Handshake completed successfully on {}", connection);
        } catch (RpcException ex) {
          logger.debug("Failure while validating handshake", ex);
          connectionHandler.connectionFailed(FailureType.HANDSHAKE_VALIDATION, ex);
        }
      }

      @Override
      public void interrupted(final InterruptedException ex) {
        logger.warn("Interrupted while waiting for handshake response", ex);
        connectionHandler.connectionFailed(FailureType.HANDSHAKE_COMMUNICATION, ex);
      }
    }
  }

  /**
   * Handler to invoke the {@link ConnectionMultiListener.HandshakeSendListener handshake listener}, but generic
   * enough to support listeners on handshake response.
   */
  private class ClientHandshakeHandler extends AbstractHandshakeHandler<HR> {

    ClientHandshakeHandler() {
      super(BasicClient.this.handshakeType, BasicClient.this.handshakeParser);
    }

    @Override
    protected final void consumeHandshake(ChannelHandlerContext ctx, HR msg) throws Exception {
      // remove the handshake information from the queue so it doesn't sit there forever.
      final RpcOutcome<HR> response =
          connection.getAndRemoveRpcOutcome(handshakeType.getNumber(), coordinationId, responseClass);
      response.set(msg, null);
    }
  }

  /**
   * Handler that watches for situations where we have not written to the socket in a certain timeout. If we exceed
   * this timeout, we send a PING message to the server to convey that we are still alive.
   */
  private class IdlePingHandler extends IdleStateHandler {

    private final ChannelFutureListener pingFailedListener =
        future -> {
          if (!future.isSuccess()) {
            logger.error("Unable to maintain connection {}. Closing connection.", connection.getName());
            connection.close();
          }
        };

    IdlePingHandler(long idleWaitInMillis) {
      super(0, idleWaitInMillis, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
      if (evt.state() == IdleState.WRITER_IDLE) {
        ctx.writeAndFlush(PING_MESSAGE)
            .addListener(pingFailedListener);
      }
    }
  }

}
