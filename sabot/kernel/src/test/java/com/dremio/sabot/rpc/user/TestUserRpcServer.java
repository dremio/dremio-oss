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
package com.dremio.sabot.rpc.user;

import static com.dremio.exec.proto.UserProtos.RpcType.CANCEL_QUERY_VALUE;
import static com.dremio.exec.proto.UserProtos.RpcType.GET_CATALOGS_VALUE;
import static com.dremio.exec.proto.UserProtos.RpcType.GET_SCHEMAS_VALUE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.exec.server.options.SessionOptionManagerFactoryImpl;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionValidatorListing;
import com.dremio.sabot.rpc.user.UserRPCServer.UserClientConnectionImpl;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.opentracing.mock.MockTracer;
import java.io.IOException;
import java.util.UUID;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestUserRpcServer {

  // Build Args - Because UserRpcServer Extends BasicServer - we need to provide some basic config
  // to create an instance.
  RpcConfig rpcConfig = RpcConfig.newBuilder().timeout(10).name("testing").build();
  @Mock Provider<UserService> userServiceProvider;
  @Mock Provider<NodeEndpoint> nodeEndpointProvider;
  @Mock WorkIngestor ingestor;
  @Mock UserBitShared.UserCredentials userCredentials;
  @Mock Provider<UserWorker> worker;
  final BufferAllocator allocator = DremioRootAllocator.create(10, 1);
  final EventLoopGroup loopGroup = new DefaultEventLoop();
  @Mock InboundImpersonationManager impersonationManager;

  // Handle args
  @Mock UserClientConnectionImpl connection;
  @Mock UserSession userSession; // Session is returned from the connection
  @Mock SocketChannel socketChannel;
  @Mock UserService userService;
  @Mock User user;
  @Mock UID uid;

  final byte[] pBody = "abc".getBytes(UTF_8);
  @Mock ByteBuf dBody;
  @Mock ResponseSender responseSender;
  @Mock OptionValidatorListing optionValidatorListing;

  private MockTracer tracer = new MockTracer();
  private UserRPCServer server;

  private static final String USERNAME = "test_user";

  @Captor ArgumentCaptor<ResponseSender> captorSender;

  public void setup(boolean tracingEnabled, boolean[] closed) {
    // Use the mock ingestor by default.
    setup(ingestor, tracingEnabled, closed);
  }

  public void setup(WorkIngestor ingestor, boolean enabled, boolean[] closed) {
    // Simply connect the session to the connection.
    // It's up to the individual tests return from calls to the session.
    when(connection.getSession()).thenReturn(userSession);
    when(userSession.getCredentials()).thenReturn(userCredentials);
    when(userCredentials.getUserName()).thenReturn(USERNAME);
    when(userServiceProvider.get()).thenReturn(userService);
    try {
      when(userService.getUser(USERNAME)).thenReturn(user);
    } catch (UserNotFoundException e) {
      // This doesn't actually happen because the mock is stubbed to return a real value, but it
      // prevents us from needing to declare the exception in the method signature.
    }
    when(user.getUID()).thenReturn(uid);
    when(uid.getId()).thenReturn(UUID.randomUUID().toString());
    when(userSession.isTracingEnabled()).thenReturn(enabled);
    when(connection.newRequestHandle(anyInt()))
        .thenReturn(
            () -> {
              closed[0] = true;
            });

    // #getAuthenticator() is never called in this test, hence using null.
    server =
        new UserRPCServer(
            rpcConfig,
            null,
            userServiceProvider,
            nodeEndpointProvider,
            ingestor,
            worker,
            allocator,
            loopGroup,
            impersonationManager,
            tracer,
            optionValidatorListing);
  }

  @After
  public void teardown() throws IOException {
    server.close();
    tracer.reset();
  }

  public void verifySendResponse(int numFinishedSpans) {
    Response r = mock(Response.class);
    verifyNoInteractions(responseSender);
    captorSender.getValue().send(r);
    verify(responseSender).send(r);
    assertEquals(numFinishedSpans, tracer.finishedSpans().size());
  }

  @Test
  public void testHandlePassesNoopTracesByDefault() throws RpcException {
    boolean[] closed = {false};
    setup(false, closed);

    server.handle(connection, 10, GET_CATALOGS_VALUE, pBody, dBody, responseSender);

    verify(ingestor)
        .feedWork(
            eq(connection), eq(GET_CATALOGS_VALUE), eq(pBody), eq(dBody), captorSender.capture());
    assertEquals(tracer.finishedSpans().size(), 0);
    assertFalse(closed[0]);
    verifySendResponse(0);
  }

  @Test
  public void testHandleCreatesSpansFromTracerWhenTracingEnabled() throws RpcException {
    boolean[] closed = {false};
    setup(true, closed);

    server.handle(connection, 11, GET_SCHEMAS_VALUE, pBody, dBody, responseSender);

    verify(ingestor)
        .feedWork(
            eq(connection), eq(GET_SCHEMAS_VALUE), eq(pBody), eq(dBody), captorSender.capture());
    assertEquals(tracer.finishedSpans().size(), 0);
    verifySendResponse(1);
    assertTrue(closed[0]);
    assertEquals("GET_SCHEMAS", tracer.finishedSpans().get(0).tags().get("rpc_type"));
  }

  @Test
  public void testHandleSpansWhileSendingFailure() throws RpcException {
    boolean[] closed = {false};
    setup(true, closed);

    server.handle(connection, 12, GET_CATALOGS_VALUE, pBody, dBody, responseSender);

    verify(ingestor)
        .feedWork(
            eq(connection), eq(GET_CATALOGS_VALUE), eq(pBody), eq(dBody), captorSender.capture());
    assertEquals(tracer.finishedSpans().size(), 0);

    UserRpcException r = mock(UserRpcException.class);
    verifyNoInteractions(responseSender);
    captorSender.getValue().sendFailure(r);
    verify(responseSender).sendFailure(r);
    assertEquals(1, tracer.finishedSpans().size());
    assertTrue(closed[0]);
    assertEquals("GET_CATALOGS", tracer.finishedSpans().get(0).tags().get("rpc_type"));
  }

  @Test
  public void testHandleFinishesSpanIfFeedFailure() throws RpcException {
    WorkIngestor ingest =
        (con, rpc, pb, db, sender) -> {
          throw new RpcException();
        };
    boolean[] closed = {false};
    setup(ingest, true, closed);

    try {
      server.handle(connection, 13, CANCEL_QUERY_VALUE, pBody, dBody, responseSender);
    } catch (RpcException e) {
    }

    assertEquals(1, tracer.finishedSpans().size());
    assertEquals("CANCEL_QUERY", tracer.finishedSpans().get(0).tags().get("rpc_type"));
    assertTrue(closed[0]);
  }

  @Test
  public void testSessionOptionManagerLifetime() throws Exception {
    // create and drop a UserClientConnection, spy on factory in server
    WorkIngestor ingest =
        (con, rpc, pb, db, sender) -> {
          throw new RpcException();
        };
    boolean[] closed = {false};
    setup(ingest, true, closed);

    when(socketChannel.pipeline()).thenReturn(mock(ChannelPipeline.class));

    // create a UserClientConnection, create an associated SessionOptionManager
    final UserClientConnectionImpl userClientConnection =
        server.initRemoteConnection(socketChannel);
    final UserClientConnectionImpl spy = spy(userClientConnection);
    UserSession session = new UserSession();
    doReturn(session).when(spy).getSession();

    final String uuid = userClientConnection.getUuid().toString();
    server.getSessionOptionManagerFactory().getOrCreate(uuid);

    // SessionOptionManagerFactory should contain the SessionOptionManager associated with this
    // connection.
    assertTrue(containsSessionOptionManager(uuid));

    // close the UserClientConnection
    server.newCloseListener(socketChannel, spy).operationComplete(mock(ChannelFuture.class));

    // closing this connection should delete the SessionOptionManager associated with it.
    assertFalse(containsSessionOptionManager(uuid));
  }

  private boolean containsSessionOptionManager(String uuid) {
    return ((SessionOptionManagerFactoryImpl) server.getSessionOptionManagerFactory())
        .contains(uuid);
  }
}
