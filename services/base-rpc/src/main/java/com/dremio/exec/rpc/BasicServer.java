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
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

/**
 * A server is bound to a port and is responsible for responding to various type of requests. In some cases,
 * the inbound requests will generate more than one outbound request.
 *
 * TODO: Above comment seems incorrect.. with each request, the client sends a coordination id which is single-use.
 *
 * @param <T> rpc type
 * @param <C> connection type
 */
public abstract class BasicServer<T extends EnumLite, C extends RemoteConnection> extends RpcBus<T, C> {

  protected static final String TIMEOUT_HANDLER = "timeout-handler";
  protected static final String MESSAGE_DECODER = "message-decoder";

  private final ServerBootstrap b;
  private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  public BasicServer(
      final RpcConfig rpcMapping,
      ByteBufAllocator alloc,
      EventLoopGroup eventLoopGroup) {
    super(rpcMapping);

    b = new ServerBootstrap()
        .channel(TransportCheck.getServerSocketChannel())
        .option(ChannelOption.SO_BACKLOG, 1000)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) TimeUnit.SECONDS.toMillis(30))
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_RCVBUF, 1 << 17)
        .option(ChannelOption.SO_SNDBUF, 1 << 17)
        .group(eventLoopGroup)
        .childOption(ChannelOption.ALLOCATOR, alloc)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            BasicServer.this.initChannel(ch);
          }

          @Override
          public void channelActive(ChannelHandlerContext ctx) throws Exception {
            allChannels.add(ctx.channel());
            super.channelActive(ctx);
          }

          @Override
          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.warn("Failed to initialize a channel. Closing: {}", ctx.channel(), cause);
            ctx.close();
          }
        });
  }

  /**
   * Initialize the {@code SocketChannel}.
   *
   * This method initializes a new channel created by the {@code ServerBootstrap}
   *
   * The default implementation create a remote connection, configures a default pipeline
   * which handles coding/decoding messages, handshaking, timeout and error handling based
   * on {@code RpcConfig} instance provided at construction time.
   *
   * On each call to this method, every handler added must be a new instance. As of now, the
   * handlers cannot be shared across connections.
   *
   * Subclasses can override it to add extra handlers if needed.
   *
   * @param ch the socket channel
   */
  protected void initChannel(final SocketChannel ch) throws SSLException {
    C connection = initRemoteConnection(ch);
    connection.setChannelCloseHandler(newCloseListener(ch, connection));

    final ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast(PROTOCOL_ENCODER, new RpcEncoder("s-" + rpcConfig.getName()));
    pipeline.addLast(MESSAGE_DECODER, newDecoder(connection.getAllocator()));
    pipeline.addLast(HANDSHAKE_HANDLER, newHandshakeHandler(connection));

    if (rpcConfig.hasTimeout()) {
      pipeline.addLast(TIMEOUT_HANDLER,
          new LoggingReadTimeoutHandler(connection, rpcConfig.getTimeout()));
    }

    pipeline.addLast(MESSAGE_HANDLER, new InboundHandler(connection));
    pipeline.addLast(EXCEPTION_HANDLER, new RpcExceptionHandler<>(connection));
  }

  /**
   * Closes a connection if no data was read on the channel for the given timeout.
   */
  private class LoggingReadTimeoutHandler extends ReadTimeoutHandler {

    private final C connection;
    private final int timeoutSeconds;

    private LoggingReadTimeoutHandler(C connection, int timeoutSeconds) {
      super(timeoutSeconds);
      this.connection = connection;
      this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    protected void readTimedOut(ChannelHandlerContext ctx) throws Exception {
      logger.info("RPC connection {} timed out.  Timeout was set to {} seconds. Closing connection.",
          connection.getName(), timeoutSeconds);
      super.readTimedOut(ctx);
    }
  }

  /**
   * Return new message decoder to be added to channel pipeline.
   *
   * @param allocator allocator
   * @return message decoder
   */
  protected abstract MessageDecoder newDecoder(BufferAllocator allocator);

  /**
   * Return new handshake handler to be added to channel pipeline.
   *
   * @param connection connection
   * @return handshake handler
   */
  protected abstract ServerHandshakeHandler<?> newHandshakeHandler(C connection);

  /**
   * {@inheritDoc}
   */
  @Override
  protected abstract Response handle(C connection, int rpcType, byte[] pBody, ByteBuf dBody) throws RpcException;

  /**
   * See {@link RpcBus#send}.
   */
  @Override // overridden to expand visibility
  public <SEND extends MessageLite, RECEIVE extends MessageLite>
  RpcFuture<RECEIVE> send(C connection, T rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    return super.send(connection, rpcType, protobufBody, clazz, dataBodies);
  }

  /**
   * Bind the server to a port on which the server reads and writes messages to.
   * <p>
   * If port hunting is disabled, the initial port is the port the server binds to, without retry in case of failures.
   * If port hunting is enabled, this method tries to bind to a port starting from the initial port, until successful.
   *
   * @param initialPort      initial port
   * @param allowPortHunting if port hunting is enabled
   * @return the port that the server bound to
   */
  public int bind(final int initialPort, boolean allowPortHunting) {
    int port = initialPort;
    while (true) {
      try {
        Channel channel = b.bind(port).sync().channel();
        allChannels.add(channel);
        port = ((InetSocketAddress)channel.localAddress()).getPort();
        break;
      } catch (Exception e) {
        // TODO(DRILL-3026):  Revisit:  Exception is not (always) BindException.
        // One case is "java.io.IOException: bind() failed: Address already in
        // use".
        if (e instanceof BindException && allowPortHunting) {
          port++;
          continue;
        }
        throw UserException.resourceError(e)
            .addContext("Server", rpcConfig.getName())
            .message("Could not bind to port %s.", port)
            .build(logger);
      }
    }

    logger.info("[{}]: Server started on port {}.", rpcConfig.getName(), port);
    return port;
  }

  @Override
  public void close() throws IOException {
    try {
      allChannels.close().sync();
    } catch (InterruptedException e) {
      logger.warn("[{}]: Failure while shutting down.", rpcConfig.getName(), e);

      Thread.currentThread().interrupt();
    }
    logger.info("[{}]: Server shutdown.", rpcConfig.getName());
  }

  /**
   * Server handshake handler.
   *
   * @param <T> handshake type
   */
  protected abstract static class ServerHandshakeHandler<T extends MessageLite> extends AbstractHandshakeHandler<T> {

    protected ServerHandshakeHandler(EnumLite handshakeType, Parser<T> parser) {
      super(handshakeType, parser);
    }

    @Override
    protected void consumeHandshake(ChannelHandlerContext ctx, T inbound) throws Exception {
      OutboundRpcMessage msg = new OutboundRpcMessage(RpcMode.RESPONSE, this.handshakeType, coordinationId,
          getHandshakeResponse(inbound));
      ctx.writeAndFlush(msg).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    /**
     * Returns the response for the given handshake request.
     *
     * @param inbound handshake request
     * @return handshake response
     * @throws Exception if handshake is unsuccessful for some reason
     */
    public abstract MessageLite getHandshakeResponse(T inbound) throws Exception;
  }

}
