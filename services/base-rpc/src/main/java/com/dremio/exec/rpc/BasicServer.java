/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.arrow.memory.BufferAllocator;

import com.google.protobuf.Internal.EnumLite;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
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
 * A server is bound to a port and is responsible for responding to various type of requests. In some cases, the inbound
 * requests will generate more than one outbound request.
 */
public abstract class BasicServer<T extends EnumLite, C extends RemoteConnection> extends RpcBus<T, C> {
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

  protected static final String TIMEOUT_HANDLER = "timeout-handler";
  protected static final String PROTOCOL_ENCODER = "protocol-encoder";

  private final ServerBootstrap b;
  private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private volatile boolean connect = false;

  public BasicServer(
      final RpcConfig rpcMapping,
      ByteBufAllocator alloc,
      EventLoopGroup eventLoopGroup) {
    super(rpcMapping);

    b = new ServerBootstrap()
        .channel(TransportCheck.getServerSocketChannel())
        .option(ChannelOption.SO_BACKLOG, 1000)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30*1000)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_RCVBUF, 1 << 17)
        .option(ChannelOption.SO_SNDBUF, 1 << 17)
        .group(eventLoopGroup) //
        .childOption(ChannelOption.ALLOCATOR, alloc)

        // .handler(new LoggingHandler(LogLevel.INFO))

        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
//            logger.debug("Starting initialization of server connection.");

            BasicServer.this.initChannel(ch);

            connect = true;
//            logger.debug("Server connection initialization completed.");
          }



          @Override
          public void channelActive(ChannelHandlerContext ctx) throws Exception {
            allChannels.add(ctx.channel());
            super.channelActive(ctx);
          }


        });

//     if(TransportCheck.SUPPORTS_EPOLL){
//       b.option(EpollChannelOption.SO_REUSEPORT, true); //
//     }
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
   * Subclasses can override it to add extra handlers if needed.
   *
   * Note that this method might be called while the instance is still under construction.
   *
   * @param ch the socket channel
   */
  protected void initChannel(final SocketChannel ch) {
    C connection = initRemoteConnection(ch);
    connection.setChannelCloseHandler(getCloseHandler(ch, connection));

    final ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast(PROTOCOL_ENCODER, new RpcEncoder("s-" + rpcConfig.getName()));
    pipeline.addLast("message-decoder", getDecoder(connection.getAllocator()));
    pipeline.addLast("handshake-handler", getHandshakeHandler(connection));

    if (rpcConfig.hasTimeout()) {
      pipeline.addLast(TIMEOUT_HANDLER,
          new LogggingReadTimeoutHandler(connection, rpcConfig.getTimeout()));
    }

    pipeline.addLast("message-handler", new InboundHandler(connection));
    pipeline.addLast("exception-handler", new RpcExceptionHandler<>(connection));
  }

  private class LogggingReadTimeoutHandler extends ReadTimeoutHandler {

    private final C connection;
    private final int timeoutSeconds;
    public LogggingReadTimeoutHandler(C connection, int timeoutSeconds) {
      super(timeoutSeconds);
      this.connection = connection;
      this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    protected void readTimedOut(ChannelHandlerContext ctx) throws Exception {
      logger.info("RPC connection {} timed out.  Timeout was set to {} seconds. Closing connection.", connection.getName(),
          timeoutSeconds);
      super.readTimedOut(ctx);
    }

  }

  protected void removeTimeoutHandler() {

  }

  public abstract MessageDecoder getDecoder(BufferAllocator allocator);

  protected abstract ServerHandshakeHandler<?> getHandshakeHandler(C connection);

  protected static abstract class ServerHandshakeHandler<T extends MessageLite> extends AbstractHandshakeHandler<T> {

    public ServerHandshakeHandler(EnumLite handshakeType, Parser<T> parser) {
      super(handshakeType, parser);
    }

    @Override
    protected void consumeHandshake(ChannelHandlerContext ctx, T inbound) throws Exception {
      OutboundRpcMessage msg = new OutboundRpcMessage(RpcMode.RESPONSE, this.handshakeType, coordinationId,
          getHandshakeResponse(inbound));
      ctx.writeAndFlush(msg).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    public abstract MessageLite getHandshakeResponse(T inbound) throws Exception;

  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return null;
  }

  @Override
  protected Response handle(C connection, int rpcType, byte[] pBody, ByteBuf dBody) throws RpcException {
    return null;
  }

  @Override
  public <SEND extends MessageLite, RECEIVE extends MessageLite> RpcFuture<RECEIVE> send(C connection, T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    return super.send(connection, rpcType, protobufBody, clazz, dataBodies);
  }

  @Override
  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> listener,
      C connection, T rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    super.send(listener, connection, rpcType, protobufBody, clazz, dataBodies);
  }

  public int bind(final int initialPort, boolean allowPortHunting) {
    int port = initialPort - 1;
    while (true) {
      try {
        allChannels.add(b.bind(++port).sync().channel());
        break;
      } catch (Exception e) {
        // TODO(DRILL-3026):  Revisit:  Exception is not (always) BindException.
        // One case is "java.io.IOException: bind() failed: Address already in
        // use".
        if (e instanceof BindException && allowPortHunting) {
          continue;
        }
        throw UserException.resourceError( e )
              .addContext( "Server", rpcConfig.getName())
              .message( "Could not bind to port %s.", port )
              .build(logger);
      }
    }

    connect = !connect;
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

}
