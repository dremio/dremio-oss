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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.server.socket.SocketMessage.JobDetailsUpdate;
import com.dremio.dac.server.socket.SocketMessage.JobProgressUpdate;
import com.dremio.dac.server.socket.SocketMessage.JobRecordsUpdate;
import com.dremio.dac.server.socket.SocketMessage.ListenDetails;
import com.dremio.dac.server.socket.SocketMessage.ListenProgress;
import com.dremio.dac.server.socket.SocketMessage.ListenRecords;
import com.dremio.dac.server.socket.SocketMessage.Payload;
import com.dremio.dac.server.tokens.TokenUtils;
import com.dremio.dac.util.JSONUtil;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.ExternalStatusListener;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.tokens.TokenManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;

/**
 * Servlet managing websockets.
 */
@SuppressWarnings("serial")
public class SocketServlet extends WebSocketServlet {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SocketServlet.class);

  static final long SOCKET_TIMEOUT_MS = 5 * 60 * 1000;

  private final AtomicLong socketIds = new AtomicLong();
  private final ConcurrentHashMap<Long, DremioSocket> sockets = new ConcurrentHashMap<>();
  private final ObjectReader reader;
  private final ObjectWriter writer;
  private final JobsService jobsService;
  private final TokenManager tokenManager;

  public SocketServlet(JobsService jobsService, final TokenManager tokenManager) {
    this.jobsService = jobsService;
    this.tokenManager = Preconditions.checkNotNull(tokenManager, "token manager is required");
    this.reader = JSONUtil.mapper().readerFor(SocketMessage.class);
    this.writer = JSONUtil.mapper().writerFor(SocketMessage.class);
  }

  @Override
  public void configure(WebSocketServletFactory factory) {
    factory.getPolicy().setIdleTimeout(SOCKET_TIMEOUT_MS);
    factory.setCreator(new Creator());
  }

  private class Creator implements WebSocketCreator {

    @Override
    public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
      // Web browsers don't support sending custom headers on web socket creation.
      // So use the optional "Sec-WebSocket-Protocol" header, which is a list of sub-protocols.
      // For now, ensure there is exactly one sub-protocol which is the auth header.
      if (req.getSubProtocols().size() != 1) {
        return sendForbiddenAsResponse(resp);
      }

      final String authHeader = req.getSubProtocols().get(0);
      final String token = TokenUtils.getToken(authHeader);

      final UserName userName;
      try {
        userName = new UserName(tokenManager.validateToken(token).username);
      } catch (final IllegalArgumentException ignored) {
        return sendForbiddenAsResponse(resp);
      }

      DremioSocket socket = new DremioSocket(userName);
      sockets.put(socket.getSocketId(), socket);
      resp.setAcceptedSubProtocol(authHeader);
      return socket;
    }
  }

  private static Object sendForbiddenAsResponse(ServletUpgradeResponse resp) {
    try {
      resp.sendForbidden("You are not authorized to open a web socket.");
    } catch (IOException ignored) {
    }
    return null;
  }

  /**
   * Individual socket between client and server.
   */
  @WebSocket(maxTextMessageSize = 128 * 1024)
  public class DremioSocket {

    private final UserName user;
    private final long socketId;
    private volatile Session session;

    public DremioSocket(UserName user) {
      super();
      this.user = user;
      this.socketId = socketIds.incrementAndGet();
    }

    @OnWebSocketError
    public void onError(Session session, Throwable error) {
      logger.error("Failure in web socket handling.", error);
    }

    @OnWebSocketMessage
    public void onText(Session session, String message) {
      logger.debug("socket message received.", message);
      try {
        SocketMessage wrapper = reader.readValue(message);
        Payload msg = wrapper.getPayload();
        if (msg instanceof SocketMessage.ListenDetails) {
          JobId id = ((ListenDetails) msg).getId();
          jobsService.registerListener(id, new DetailsListener(id, this));
        } else if (msg instanceof ListenProgress) {
          JobId id = ((ListenProgress) msg).getId();
          jobsService.registerListener(id, new ProgressListener(id, this));
        } else if (msg instanceof ListenRecords) {
          JobId id = ((ListenRecords) msg).getId();
          jobsService.registerListener(id, new RecordsListener(id, this));
        } else if (msg instanceof SocketMessage.ListenReflectionJobDetails) {
          JobId id = ((SocketMessage.ListenReflectionJobDetails) msg).getId();
          String reflectionId = ((SocketMessage.ListenReflectionJobDetails) msg).getReflectionId();
          jobsService.registerReflectionJobListener(id, user.getName(), reflectionId, new DetailsListener(id, this));
        } else if (msg instanceof SocketMessage.ListenReflectionJobProgress) {
          JobId id = ((SocketMessage.ListenReflectionJobProgress) msg).getId();
          String reflectionId = ((SocketMessage.ListenReflectionJobProgress) msg).getReflectionId();
          jobsService.registerReflectionJobListener(id, user.getName(), reflectionId, new ProgressListener(id, this));
        }

      } catch (Exception e) {
        logger.warn("Failure handling socket message of {}.", message, e);
        send(new SocketMessage.ErrorPayload(e.getMessage()));
      }
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
      this.session = session;
      send(new SocketMessage.ConnectionEstablished(SOCKET_TIMEOUT_MS));
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
      logger.debug("Socket closed with code {}", statusCode);
      sockets.remove(socketId);
    }

    public void send(Payload payload) {
      if (session != null && session.isOpen()) {
        try {
          session.getRemote().sendString(writer.writeValueAsString(new SocketMessage(payload)), null);
        } catch (JsonProcessingException e) {
          logger.warn("Failure while writing socket message.", e);
        }
      }
    }

    public long getSocketId() {
      return socketId;
    }

  }

  private class DetailsListener implements ExternalStatusListener {

    private final DremioSocket socket;
    private final JobId jobId;

    public DetailsListener(JobId jobId, DremioSocket socket) {
      super();
      this.socket = socket;
      this.jobId = jobId;
    }

    @Override
    public void queryProgressed(JobSummary jobSummary) {
      final JobDetailsUpdate update = new JobDetailsUpdate(JobsProtoUtil.toStuff(jobSummary.getJobId()));
      socket.send(update);
    }

    @Override
    public void queryCompleted(JobSummary jobSummary) {
      final JobDetailsUpdate update = new JobDetailsUpdate(JobsProtoUtil.toStuff(jobSummary.getJobId()));
      socket.send(update);
    }
  }

  private class ProgressListener implements ExternalStatusListener {

    private final DremioSocket socket;
    private final JobId jobId;

    public ProgressListener(JobId jobId, DremioSocket socket) {
      super();
      this.socket = socket;
      this.jobId = jobId;
    }

    @Override
    public void queryProgressed(JobSummary jobSummary) {
      final JobProgressUpdate update = new JobProgressUpdate(jobSummary);
      socket.send(update);
    }

    @Override
    public void queryCompleted(JobSummary jobSummary) {
      final JobProgressUpdate update = new JobProgressUpdate(jobSummary);
      socket.send(update);
    }
  }

  private class RecordsListener implements ExternalStatusListener {

    private final DremioSocket socket;
    private final JobId jobId;

    public RecordsListener(JobId jobId, DremioSocket socket) {
      super();
      this.socket = socket;
      this.jobId = jobId;
    }

    @Override
    public void queryProgressed(JobSummary jobSummary) {
      final JobRecordsUpdate update = new JobRecordsUpdate(JobsProtoUtil.toStuff(jobSummary.getJobId()), jobSummary.getRecordCount());
      socket.send(update);
    }
  }
}
