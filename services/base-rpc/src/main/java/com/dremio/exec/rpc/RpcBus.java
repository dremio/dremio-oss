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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.Histogram;
import com.dremio.common.SerializedExecutor;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.metrics.Metrics;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * The Rpc Bus deals with incoming and outgoing communication and is used on both the server and the client side of a
 * system.
 *
 * @param <T>
 */
public abstract class RpcBus<T extends EnumLite, C extends RemoteConnection> implements Closeable {
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

  private static final OutboundRpcMessage PONG = new OutboundRpcMessage(RpcMode.PONG, 0, 0, Acks.OK);
  private static final boolean ENABLE_SEPARATE_THREADS = "true".equals(System.getProperty("dremio.enable_rpc_offload", "true"));

  public static final long RPC_DELAY_WARNING_THRESHOLD =
      Integer.parseInt(System.getProperty("dremio.exec.rpcDelayWarning", "500"));

  protected abstract MessageLite getResponseDefaultInstance(int rpcType) throws RpcException;

  protected void handle(C connection, int rpcType, byte[] pBody, ByteBuf dBody, ResponseSender sender) throws RpcException{
    sender.send(handle(connection, rpcType, pBody, dBody));
  }

  protected abstract Response handle(C connection, int rpcType, byte[] pBody, ByteBuf dBody) throws RpcException;

  protected final RpcConfig rpcConfig;

  private final Histogram sendDurations;

  public RpcBus(RpcConfig rpcConfig) {
    this.rpcConfig = rpcConfig;
    this.sendDurations = Metrics.getInstance()
        .histogram(rpcConfig.getName() + "-send-durations-ms");
  }

  <SEND extends MessageLite, RECEIVE extends MessageLite> RpcFuture<RECEIVE> send(C connection, T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    RpcFutureImpl<RECEIVE> rpcFuture = new RpcFutureImpl<>();
    this.send(rpcFuture, connection, rpcType, protobufBody, clazz, dataBodies);
    return rpcFuture;
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> listener, C connection, T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    send(listener, connection, rpcType, protobufBody, clazz, false, dataBodies);
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> listener, C connection, T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, boolean allowInEventLoop, ByteBuf... dataBodies) {
    Preconditions
        .checkArgument(
            allowInEventLoop || !connection.inEventLoop(),
            "You attempted to send while inside the rpc event thread.  This isn't allowed because sending will block if the channel is backed up.");

    boolean completed = false;

    try {

      if (!allowInEventLoop && !connection.blockOnNotWritable(listener)) {
        // if we're in not in the event loop and we're interrupted while blocking, skip sending this message.
        return;
      }

      assert !Arrays.asList(dataBodies).contains(null);
      assert rpcConfig.checkSend(rpcType, protobufBody.getClass(), clazz);

      Preconditions.checkNotNull(protobufBody);
      final Stopwatch stopwatch = Stopwatch.createStarted();
      ChannelListenerWithCoordinationId futureListener = connection.createNewRpcListener(listener, clazz);
      OutboundRpcMessage m = new OutboundRpcMessage(RpcMode.REQUEST, rpcType, futureListener.getCoordinationId(), protobufBody, dataBodies);
      ChannelFuture channelFuture = connection.getChannel().writeAndFlush(m);
      channelFuture.addListener(futureListener);
      channelFuture.addListener(new GenericFutureListener<Future<? super Void>>() {
        @Override
        public void operationComplete(Future<? super Void> future) throws Exception {
          sendDurations.update(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
      });
      completed = true;
    } catch (Exception | AssertionError e) {
      listener.failed(new RpcException("Failure sending message.", e));
    } finally {

      if (!completed) {
        if (dataBodies != null) {
          for (ByteBuf b : dataBodies) {
            b.release();
          }

        }
      }
    }
  }

  public abstract C initRemoteConnection(SocketChannel channel);

  public class ChannelClosedHandler implements ChannelFutureListener {

    final C clientConnection;

    public ChannelClosedHandler(C clientConnection, Channel channel) {
      this.clientConnection = clientConnection;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      final String msg = String.format("[%s]: Channel closed %s", rpcConfig.getName(), clientConnection.getName());

      final ChannelClosedException ex = future.cause() != null ? new ChannelClosedException(msg, future.cause()) : new ChannelClosedException(msg);
      logger.info(msg);
      clientConnection.channelClosed(ex);
    }

  }

  protected ChannelFutureListener getCloseHandler(SocketChannel channel, C clientConnection) {
    return new ChannelClosedHandler(clientConnection, channel);
  }


  private class ResponseSenderImpl implements ResponseSender {
    private FirstFailureHandler failureHandler = new FirstFailureHandler();
    private RemoteConnection connection;
    private int coordinationId;
    private final AtomicBoolean sent = new AtomicBoolean(false);

    public ResponseSenderImpl() {
    }

    void set(RemoteConnection connection, int coordinationId){
      this.connection = connection;
      this.coordinationId = coordinationId;
      sent.set(false);
    }

    @Override
    public void send(Response r) {
      assert rpcConfig.checkResponseSend(r.rpcType, r.pBody.getClass());
      final OutboundRpcMessage outMessage =
          new OutboundRpcMessage(RpcMode.RESPONSE, r.rpcType, coordinationId, r.pBody, r.dBodies);
      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Adding message to outbound buffer. {}", outMessage);
        logger.debug("Sending response with Sender {}", System.identityHashCode(this));
      }
      ChannelFuture future = connection.getChannel().writeAndFlush(outMessage);
      future.addListener(failureHandler);
    }

    private class FirstFailureHandler implements ChannelFutureListener {

      @Override
      public void operationComplete(ChannelFuture future) {
          if (!future.isSuccess()) {
            Throwable ex = future.cause();
            if(ex == null){
              sendFailure(new UserRpcException(null, "Unknown failure when sending message.", null));
            } else {
              sendFailure(new UserRpcException(null, "Failure when sending message.", ex));
            }
          }
      }
    }

    /**
     * Ensures that each sender is only used once.
     */
    private void sendOnce() {
      if (!sent.compareAndSet(false, true)) {
        throw new IllegalStateException("Attempted to utilize a sender multiple times.");
      }
    }

    void sendFailure(UserRpcException e){
      sendFailure(e, true);
    }

    private boolean sendFailure(UserRpcException e, boolean failOnAlreadySent){
      if(failOnAlreadySent){
        sendOnce();
      }else{
        if (!sent.compareAndSet(false, true)) {
          return false;
        }
      }

      UserException uex = UserException.systemError(e)
          .addIdentity(e.getEndpoint())
          .build(logger);

      OutboundRpcMessage outMessage = new OutboundRpcMessage(
          RpcMode.RESPONSE_FAILURE,
          0,
          coordinationId,
          uex.getOrCreatePBError(false)
          );

      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Adding message to outbound buffer. {}", outMessage);
      }

      final ChannelFuture future = connection.getChannel().writeAndFlush(outMessage);
      // if the failure message can't be propagated, we need to close the connection to avoid having hanging messages.
      future.addListener(RESPONSE_FAILURE_FAILURE);

      // if this
      return true;
    }

  }


  private SecondFailureHandler RESPONSE_FAILURE_FAILURE = new SecondFailureHandler();

  private class SecondFailureHandler implements ChannelFutureListener {

    @Override
    public void operationComplete(ChannelFuture future) {
        if (!future.isSuccess()) {
          logger.error("Failure sending response failure message, closing connection.", future.cause());
          future.channel().close();
        }
    }
  }

  private class SameExecutor implements Executor {

    @Override
    public void execute(Runnable command) {
      command.run();
    }

  }

  protected class InboundHandler extends MessageToMessageDecoder<InboundRpcMessage> {

    private final Executor exec;
    private final C connection;

    public InboundHandler(C connection) {
      super();
      Preconditions.checkNotNull(connection);
      this.connection = connection;
      final Executor underlyingExecutor = ENABLE_SEPARATE_THREADS ? rpcConfig.getExecutor() : new SameExecutor();
      this.exec = new RpcEventHandler(underlyingExecutor);
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final InboundRpcMessage msg, final List<Object> output) throws Exception {
      if (!ctx.channel().isOpen()) {
        return;
      }
      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Received message {}", msg);
      }
      final Channel channel = connection.getChannel();
      final Stopwatch watch = Stopwatch.createStarted();

      try{

        switch (msg.mode) {
        case REQUEST:
          RequestEvent reqEvent = new RequestEvent(msg.coordinationId, connection, msg.rpcType, msg.pBody, msg.dBody);
          exec.execute(reqEvent);
          break;

        case RESPONSE:
          ResponseEvent respEvent = new ResponseEvent(connection, msg.rpcType, msg.coordinationId, msg.pBody, msg.dBody);
          exec.execute(respEvent);
          break;

        case RESPONSE_FAILURE:
          DremioPBError failure = DremioPBError.parseFrom(msg.pBody);
          connection.recordRemoteFailure(msg.coordinationId, failure);
          if (RpcConstants.EXTRA_DEBUGGING) {
            logger.debug("Updated rpc future with coordinationId {} with failure ", msg.coordinationId, failure);
          }
          break;

        case PING:
          channel.writeAndFlush(PONG);
          break;

        case PONG:
          // noop.
          break;

        default:
          throw new UnsupportedOperationException();
        }
      } finally {
        long time = watch.elapsed(TimeUnit.MILLISECONDS);
        if (time > RPC_DELAY_WARNING_THRESHOLD) {
          logger.warn(String.format(
              "Message of mode %s of rpc type %d took longer than %dms.  Actual duration was %dms.",
              msg.mode, msg.rpcType, RPC_DELAY_WARNING_THRESHOLD, time));
        }
        msg.release();
      }
    }
  }

  public static <T> T get(ByteBuf pBody, Parser<T> parser) throws RpcException {
    try {
      ByteBufInputStream is = new ByteBufInputStream(pBody);
      return parser.parseFrom(is);
    } catch (InvalidProtocolBufferException e) {
      throw new RpcException(String.format("Failure while decoding message with parser of type. %s", parser.getClass().getCanonicalName()), e);
    }
  }

  public static <T> T get(byte[] pBody, Parser<T> parser) throws RpcException {
    try {
      return parser.parseFrom(pBody);
    } catch (InvalidProtocolBufferException e) {
      throw new RpcException(String.format("Failure while decoding message with parser of type. %s", parser.getClass().getCanonicalName()), e);
    }
  }

  public static <T> T get(ByteString pBody, Parser<T> parser) throws RpcException {
    try {
      return parser.parseFrom(pBody);
    } catch (InvalidProtocolBufferException e) {
      throw new RpcException(String.format("Failure while decoding message with parser of type. %s", parser.getClass().getCanonicalName()), e);
    }
  }

  class RpcEventHandler extends SerializedExecutor {

    public RpcEventHandler(Executor underlyingExecutor) {
      super(rpcConfig.getName() + "-rpc-event-queue", underlyingExecutor);
    }

    @Override
    protected void runException(Runnable command, Throwable t) {
      logger.error("Failure while running rpc command.", t);
    }

  }

  private class RequestEvent implements Runnable {
    private final ResponseSenderImpl sender;
    private final C connection;
    private final int rpcType;
    private final byte[] pBody;
    private final ByteBuf dBody;

    RequestEvent(int coordinationId, C connection, int rpcType, byte[] pBody, ByteBuf dBody) {
      sender = new ResponseSenderImpl();
      this.connection = connection;
      this.rpcType = rpcType;
      this.pBody = pBody;
      this.dBody = dBody;
      sender.set(connection, coordinationId);

      if(dBody != null){
        dBody.retain();
      }
    }

    @Override
    public void run() {
      try {
        handle(connection, rpcType, pBody, dBody, sender);
      } catch(UserRpcException e){
        sender.sendFailure(e);
      } catch (Exception e) {
        final UserRpcException genericException = new UserRpcException(NodeEndpoint.getDefaultInstance(), "Remote message leaked.", e);
        if(!sender.sendFailure(genericException, false)){
          logger.error("Message handling failed for rpcType {} after response already sent. Logging locally since it cannot be communicated back to sender.", rpcType, e);
        }
      }finally{

        if(dBody != null){
          dBody.release();
        }
      }

    }


  }

  private class ResponseEvent implements Runnable {

    private final int rpcType;
    private final int coordinationId;
    private final byte[] pBody;
    private final ByteBuf dBody;
    private final C connection;

    public ResponseEvent(C connection, int rpcType, int coordinationId, byte[] pBody, ByteBuf dBody) {
      this.rpcType = rpcType;
      this.coordinationId = coordinationId;
      this.pBody = pBody;
      this.dBody = dBody;
      this.connection = connection;

      if(dBody != null){
        dBody.retain();
      }
    }

    @Override
    public void run(){
      try {
        MessageLite m = getResponseDefaultInstance(rpcType);
        assert rpcConfig.checkReceive(rpcType, m.getClass());
        RpcOutcome<?> rpcFuture = connection.getAndRemoveRpcOutcome(rpcType, coordinationId, m.getClass());
        Parser<?> parser = m.getParserForType();
        Object value = parser.parseFrom(pBody);
        rpcFuture.set(value, dBody);
        if (RpcConstants.EXTRA_DEBUGGING) {
          logger.debug("Updated rpc future {} with value {}", rpcFuture, value);
        }
      } catch (Exception ex) {
        logger.error("Failure while handling response.", ex);
      }finally{

        if(dBody != null){
          dBody.release();
        }

      }

    }

  }
}
