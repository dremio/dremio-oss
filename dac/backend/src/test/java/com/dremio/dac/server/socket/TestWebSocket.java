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
package com.dremio.dac.server.socket;

import static com.dremio.dac.proto.model.dataset.OrderDirection.ASC;
import static com.dremio.dac.server.UIOptions.JOBS_UI_CHECK;
import static com.dremio.options.OptionValue.OptionType.SYSTEM;
import static com.dremio.service.namespace.dataset.DatasetVersion.newVersion;
import static javax.ws.rs.client.Entity.entity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.InitialRunResponse;
import com.dremio.dac.explore.model.InitialTransformAndRunResponse;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.proto.model.dataset.TransformSort;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.socket.SocketMessage.JobProgressUpdate;
import com.dremio.dac.server.socket.SocketMessage.JobRecordsUpdate;
import com.dremio.dac.server.socket.SocketMessage.Payload;
import com.dremio.dac.util.JSONUtil;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.options.OptionValue;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * Testing web socket.
 */
public class TestWebSocket extends BaseTestServer {

  private WebSocketClient client = new WebSocketClient();
  private TestSocket socket;
  private static OptionValue option = OptionValue.createBoolean(SYSTEM, JOBS_UI_CHECK.getOptionName(), false);

  // Test query that takes reasonable amount of time (21^4 cartesian join)
  public static final String LONG_TEST_QUERY = "select t1.id as id \n" +
    "from (VALUES 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1) AS t1(id) \n" +
    "join (VALUES 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1) AS t2(id) on t1.id = t2.id \n" +
    "join (VALUES 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1) AS t3(id) on t2.id = t3.id \n" +
    "join (VALUES 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1) AS t4(id) on t3.id = t4.id \n" +
    "WHERE t1.id <> 1";

  @Before
  public void setup() throws Exception {
    client.start();
    URI socketUri = new URI(getAPIv2().path("socket").getUri().toString().replace("http://", "ws://"));
    ClientUpgradeRequest request = new ClientUpgradeRequest();
    request.setSubProtocols(Lists.newArrayList(getAuthHeaderValue()));
    this.socket = new TestSocket();
    client.connect(socket, socketUri, request);
    socket.awaitConnection(2);
    getSabotContext().getOptionManager().setOption(option);
    assertEquals(getAuthHeaderValue(), socket.session.getUpgradeResponse().getAcceptedSubProtocol());
  }

  @After
  public void teardown() throws Exception {
    client.stop();
  }

  @Test
  public void ping() throws Exception {
    socket.send(new SocketMessage.PingPayload());
    socket.awaitCompletion(1, 2);
  }


  @Test
  public void runAndTransform() throws Exception {
    final InitialPreviewResponse resp = createDatasetFromSQL(LONG_TEST_QUERY, null);

    final InitialTransformAndRunResponse runResp = expectSuccess(
      getBuilder(
        getAPIv2()
          .path(versionedResourcePath(resp.getDataset()))
          .path("transformAndRun")
          .queryParam("newVersion", newVersion())
      ).buildPost(entity(new TransformSort("id", ASC), JSON)), InitialTransformAndRunResponse.class);
    socket.send(new SocketMessage.ListenProgress(runResp.getJobId()));
    List<Payload> payloads = socket.awaitCompletion(10);
    if (payloads.get(1) instanceof SocketMessage.ErrorPayload) {
      Assert.fail(((SocketMessage.ErrorPayload) payloads.get(1)).getMessage());
    }
    JobProgressUpdate progressUpdate = (JobProgressUpdate) payloads.get(payloads.size() - 1);
    assertTrue(progressUpdate.getUpdate().isComplete());
    assertEquals(0L, (long) progressUpdate.getUpdate().getOutputRecords());
  }

  @Test
  public void testRunNewUntitledFromSql() throws IOException, InterruptedException {
    final String query = "select * from sys.version";
    final Invocation invocation = getBuilder(
      getAPIv2()
        .path("datasets/new_untitled_sql_and_run")
        .queryParam("newVersion", newVersion())
    ).buildPost(Entity.entity(new CreateFromSQL(query, null), MediaType.APPLICATION_JSON_TYPE));
    InitialRunResponse runResponse = expectSuccess(invocation, InitialRunResponse.class);
    socket.send(new SocketMessage.ListenProgress(runResponse.getJobId()));
    List<Payload> payloads = socket.awaitCompletion(10);
    if (payloads.get(1) instanceof SocketMessage.ErrorPayload) {
      Assert.fail(((SocketMessage.ErrorPayload) payloads.get(1)).getMessage());
    }
    JobProgressUpdate progressUpdate = (JobProgressUpdate) payloads.get(payloads.size() - 1);
    assertTrue(progressUpdate.getUpdate().isComplete());
    assertEquals(1L, (long) progressUpdate.getUpdate().getOutputRecords());
  }

  @Test
  public void jobProgress() throws Exception {
    final InitialPreviewResponse resp = createDatasetFromSQL(LONG_TEST_QUERY, null);
    final InitialRunResponse runResp = expectSuccess(getBuilder(getAPIv2().path(versionedResourcePath(resp.getDataset()) + "/run")).buildGet(), InitialRunResponse.class);
    socket.send(new SocketMessage.ListenProgress(runResp.getJobId()));
    List<Payload> payloads = socket.awaitCompletion(10);
    assertEquals(payloads.get(0), new SocketMessage.ConnectionEstablished(SocketServlet.SOCKET_TIMEOUT_MS));
    JobProgressUpdate progressUpdate = (JobProgressUpdate) payloads.get(payloads.size() - 1);
    assertTrue(progressUpdate.getUpdate().isComplete());
    expectSuccess(getBuilder(runResp.getPaginationUrl() + "?offset=0&limit=50").buildGet(), JobDataFragment.class);

    // verify that we got PLANNING, STARTING, RUNNING, FINISHED and in sequential order
    final AtomicInteger planningIndex = new AtomicInteger(-1);
    final AtomicInteger startIndex = new AtomicInteger(-1);
    final AtomicInteger runningIndex = new AtomicInteger(-1);
    final AtomicInteger completedIndex = new AtomicInteger(-1);
    IntStream.range(0, payloads.size()).forEach(index -> {
      final Payload payload = payloads.get(index);
      if (!(payload instanceof JobProgressUpdate)) {
        return;
      }

      final JobProgressUpdate update = (JobProgressUpdate) payload;
      switch (update.getUpdate().getState()) {
        case STARTING:
          startIndex.set(index);
          break;
        case RUNNING:
          runningIndex.set(index);
          break;
        case COMPLETED:
          completedIndex.set(index);
          break;
        case PLANNING:
          planningIndex.set(index);
          break;
        default:
          break;
      }
    });

    assertTrue(planningIndex.longValue() > -1);
    assertTrue(startIndex.longValue() > -1);
    assertTrue(runningIndex.longValue() > -1);
    assertTrue(completedIndex.longValue() > -1);

    assertTrue(planningIndex.get() < startIndex.get());
    assertTrue(startIndex.get() < runningIndex.get());
    assertTrue(runningIndex.get() < completedIndex.get());
  }

  @Test
  public void jobDetailsUpdate() throws Exception {
    final InitialPreviewResponse resp = createDatasetFromSQL(LONG_TEST_QUERY, null);
    final InitialRunResponse runResp = expectSuccess(getBuilder(getAPIv2().path(versionedResourcePath(resp.getDataset()) + "/run")).buildGet(), InitialRunResponse.class);
    socket.send(new SocketMessage.ListenDetails(runResp.getJobId()));
    List<Payload> payloads = socket.awaitCompletion(2, 10);
    SocketMessage.JobDetailsUpdate detailsUpdate = (SocketMessage.JobDetailsUpdate) payloads.get(payloads.size() - 1);
    assertTrue(detailsUpdate.getJobId().equals(runResp.getJobId()));
  }

  @Test
  public void testRecordCountProgress() throws Exception {
    final ForemenWorkManager foremenWorkManager = l(ForemenWorkManager.class);

    final InitialPreviewResponse resp = createDatasetFromSQL(LONG_TEST_QUERY, null);

    final InitialTransformAndRunResponse runResp = expectSuccess(
      getBuilder(
        getAPIv2()
          .path(versionedResourcePath(resp.getDataset()))
          .path("transformAndRun")
          .queryParam("newVersion", newVersion())
      ).buildPost(entity(new TransformSort("id", ASC), JSON)), InitialTransformAndRunResponse.class);

    socket.send(new SocketMessage.ListenRecords(runResp.getJobId()));

    final List<Payload> payloads = socket.awaitCompletion(15);

    if (payloads.get(1) instanceof SocketMessage.ErrorPayload) {
      Assert.fail(((SocketMessage.ErrorPayload) payloads.get(1)).getMessage());
    }

    boolean isRecordsUpdate = false;
    for (final Payload payload : payloads) {
      if (!(payload instanceof JobRecordsUpdate)) {
        if (payload instanceof SocketMessage.ConnectionEstablished) {
          continue;
        }
        throw new Exception();
      }

      final JobRecordsUpdate update = (JobRecordsUpdate) payload;

      isRecordsUpdate = true;

      assertNotEquals(update.getRecordCount(), -1);
    }

    assertTrue(isRecordsUpdate);
  }


  /**
   * A test web socket handling class.
   */
  @WebSocket(maxTextMessageSize = 128 * 1024)
  public static class TestSocket {

    private volatile Session session;
    private volatile Throwable error;
    private ConcurrentLinkedQueue<Payload> messages = new ConcurrentLinkedQueue<>();
    private final CountDownLatch connect = new CountDownLatch(1);
    private final Semaphore messagesArrived = new Semaphore(0);

    public TestSocket() {
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
      this.session = session;
      connect.countDown();
    }

    public void send(Payload payload) throws IOException {
      session.getRemote().sendString(JSONUtil.mapper().writeValueAsString(new SocketMessage(payload)));
    }

    @OnWebSocketError
    public void methodName(Session session, Throwable error) {
      this.error = error;
      connect.countDown();
    }

    @OnWebSocketMessage
    public void onMessage(String msg) throws IOException {
      SocketMessage message = JSONUtil.mapper().readValue(msg, SocketMessage.class);
      messages.add(message.getPayload());
      messagesArrived.release();
    }

    public void awaitConnection(long secondsToWait) throws InterruptedException, TimeoutException {
      if (!connect.await(secondsToWait, TimeUnit.SECONDS)) {
        throw new TimeoutException("Failure while waiting for connection to be established.");
      }

      if (error != null) {
        throw Throwables.propagate(error);
      }
    }

    public List<Payload> awaitCompletion(int messagesExpected, long secondsToWait) throws InterruptedException {
      if (!messagesArrived.tryAcquire(messagesExpected, secondsToWait, TimeUnit.SECONDS)) {
        throw new AssertionError(String.format("Expected %d messages but didn't receive them within timeout. Total messages received %d.",
          messagesExpected, messagesArrived.availablePermits()));
      }
      return Lists.newArrayList(messages);
    }

    public List<Payload> awaitCompletion(long secondsToWait) throws InterruptedException {
      boolean completed = false;

      while (!completed) {
        messagesArrived.tryAcquire(secondsToWait, TimeUnit.SECONDS);
        ArrayList<Payload> payloads = Lists.newArrayList(messages);

        Payload payload = payloads.get(payloads.size() - 1);

        if (payload instanceof JobProgressUpdate) {
          JobProgressUpdate update = (JobProgressUpdate) payload;

          if (update.getUpdate().isComplete()) {
            completed = true;
          }
        } else if (payload instanceof SocketMessage.JobProgressUpdateForNewUI) {
          SocketMessage.JobProgressUpdateForNewUI update = (SocketMessage.JobProgressUpdateForNewUI) payload;
          if (update.getUpdate().isComplete()) {
            completed = true;
          }
        } else if (payload instanceof JobRecordsUpdate) {
          completed = true;
        }
      }
      return Lists.newArrayList(messages);
    }

    public Session getSession() {
      return session;
    }
  }
}
