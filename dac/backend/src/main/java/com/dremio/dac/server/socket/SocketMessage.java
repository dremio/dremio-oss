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

import com.dremio.dac.model.job.PartialJobListItem;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.Job;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;

/**
 * Messages sent and received on the websocket.
 */
public class SocketMessage {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
  private final Payload payload;

  @JsonCreator
  public SocketMessage(@JsonProperty("payload") Payload payload){
    this.payload = payload;
  }

  public Payload getPayload(){
    return payload;
  }

  /**
   * Payload for a socket message
   */
  public abstract static class Payload {}

  /**
   * Message from server > client informing client of connection information.
   */
  @JsonTypeName("connection-established")
  public static class ConnectionEstablished extends Payload {
    private final long timeout;

    @JsonCreator
    public ConnectionEstablished(@JsonProperty("timeout") long timeout) {
      super();
      this.timeout = timeout;
    }

    public long getTimeout() {
      return timeout;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (timeout ^ (timeout >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ConnectionEstablished other = (ConnectionEstablished) obj;
      if (timeout != other.timeout) {
        return false;
      }
      return true;
    }

  }

  /**
   * Message from server > client informing client of update job details.
   */
  @JsonTypeName("job-details")
  public static class JobDetailsUpdate extends Payload {
    private final JobId jobId;

    @JsonCreator
    public JobDetailsUpdate(
        @JsonProperty("jobId") JobId jobId) {
      super();
      this.jobId = jobId;
    }

    public JobId getJobId() {
      return jobId;
    }
  }

  /**
   * Message from server > client informing client updated job progress for history.
   */
  @JsonTypeName("job-progress")
  public static class JobProgressUpdate extends Payload {
    private final JobId id;
    private final PartialJobListItem update;

    @VisibleForTesting
    @JsonCreator
    public JobProgressUpdate(@JsonProperty("id") JobId id, @JsonProperty("update") PartialJobListItem update) {
      super();
      this.id = id;
      this.update = update;
    }

    public JobProgressUpdate(Job job) {
      this.id = job.getJobId();
      this.update = new PartialJobListItem(job);
    }

    public JobId getId() {
      return id;
    }

    public PartialJobListItem getUpdate() {
      return update;
    }
  }

  /**
   * Message from client > server requesting events about job progress.
   */
  @JsonTypeName("job-progress-listen")
  public static class ListenProgress extends Payload {
    private final JobId id;

    @JsonCreator
    public ListenProgress(@JsonProperty("id") JobId id) {
      super();
      this.id = id;
    }

    public JobId getId() {
      return id;
    }
  }

  /**
   * Message from client > server requesting events about job details.
   */
  @JsonTypeName("job-details-listen")
  public static class ListenDetails extends Payload {
    private final JobId id;

    @JsonCreator
    public ListenDetails(@JsonProperty("id") JobId id) {
      super();
      this.id = id;
    }

    public JobId getId() {
      return id;
    }

  }

  /**
   * A ping message from client (for keep alive)
   */
  @JsonTypeName("ping")
  public static class PingPayload extends Payload {
  }

  /**
   * An error from server.
   */
  @JsonTypeName("error")
  public static class ErrorPayload extends Payload {
    private final String message;

    @JsonCreator
    public ErrorPayload(@JsonProperty("message") String message) {
      super();
      this.message = message;
    }

    public String getMessage(){
      return message;
    }

  }

  public static Class<?>[] getImplClasses(){
    return new Class[] {
        SocketMessage.ConnectionEstablished.class,
        SocketMessage.JobDetailsUpdate.class,
        SocketMessage.JobProgressUpdate.class,
        SocketMessage.ListenDetails.class,
        SocketMessage.ListenProgress.class,
        SocketMessage.ErrorPayload.class,
        SocketMessage.PingPayload.class,
        };
  }
}
