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
package com.dremio.service.namespace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

/**
 * State of the source.
 */
public class SourceState {
  /**
   * Source status
   */
  public enum SourceStatus {
    good, bad, warn
  }

  /**
   * Source state message levels
   */
  public enum MessageLevel {
    INFO, WARN, ERROR
  }

  public static final SourceState GOOD = new SourceState(SourceStatus.good, Collections.<Message>emptyList());

  private final SourceStatus status;
  private final List<Message> messages;

  @JsonCreator
  public SourceState(@JsonProperty("status") SourceStatus status, @JsonProperty("messages") List<Message> messages) {
    this.status = status;
    this.messages = messages;
  }

  @JsonProperty("status")
  public SourceStatus getStatus() {
    return status;
  }

  @JsonProperty("messages")
  public List<Message> getMessages() {
    return messages;
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, messages);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SourceState)) {
      return false;
    }
    SourceState that = (SourceState) obj;
    return Objects.equals(this.status, that.status) &&
      Objects.equals(this.messages, that.messages);
  }

  /**
   * Source state message
   */
  public static class Message {
    private final MessageLevel level;
    private final String message;

    @JsonCreator
    public Message(@JsonProperty("level") MessageLevel level, @JsonProperty("message") String message) {
      this.level = level;
      this.message = message;
    }

    public MessageLevel getLevel() {
      return level;
    }

    public String getMessage() {
      return message;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if(o == null || !(o instanceof Message)) {
        return false;
      }

      Message that = (Message) o;

      return Objects.equals(this.level, that.level) &&
        Objects.equals(this.message, that.message);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("level", level).add("msg", message).toString();
    }

    @Override
    public int hashCode() {
      return Objects.hash(level, message);
    }
  }

  private static SourceState getSourceState(SourceState.SourceStatus status, MessageLevel level, String... msgs) {
    List<Message> messageList = new ArrayList<>();
    for (String msg : msgs) {
      messageList.add(new Message(level, msg));
    }
    return new SourceState(status, messageList);
  }

  public static SourceState warnState(String... e) {
    return getSourceState(SourceStatus.warn, MessageLevel.WARN, e);
  }

  public static SourceState goodState(String... e){
    return getSourceState(SourceStatus.good, MessageLevel.INFO, e);
  }

  public static SourceState badState(String... e) {
    return getSourceState(SourceStatus.bad, MessageLevel.ERROR, e);
  }

  public static SourceState badState(Exception e) {
    return getSourceState(SourceStatus.bad, MessageLevel.ERROR, e.getMessage());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    switch(status) {
    case bad:
      sb.append("Unavailable");
      break;
    case good:
      sb.append("Healthy");
      break;
    case warn:
      sb.append("Unhealthy");
      break;
    default:
      throw new IllegalStateException(status.name());
    }

    if (messages != null) {
      for (Message m : messages) {
        if (messages.size() > 1) {
          sb.append("\n\t");
          switch (m.level) {
          case ERROR:
            sb.append("Error: ");
            break;
          case INFO:
            sb.append("Info: ");
            break;
          case WARN:
            sb.append("Warning: ");
            break;
          default:
            throw new IllegalStateException(m.level.name());
          }
        } else {
          sb.append(": ");
        }
        sb.append(m.message);
      }
    }
    return sb.toString();
  }
}
