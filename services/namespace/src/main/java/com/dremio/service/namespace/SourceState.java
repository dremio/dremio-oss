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

  public static final SourceState GOOD = new SourceState(SourceStatus.good, "", Collections.<Message>emptyList());
  public static final SourceState NOT_AVAILABLE = SourceState.badState("Source is not currently available.");

  private final SourceStatus status;
  private final List<Message> messages;
  private final String suggestedUserAction;

  @JsonCreator
  public SourceState(@JsonProperty("status") SourceStatus status,
                     @JsonProperty("suggestedUserAction") String suggestedUserAction,
                     @JsonProperty("messages") List<Message> messages) {
    this.status = status;
    this.suggestedUserAction = suggestedUserAction;
    this.messages = messages;
  }

  @JsonProperty("status")
  public SourceStatus getStatus() {
    return status;
  }

  @JsonProperty("suggestedUserAction")
  public String getSuggestedUserAction() {
    return suggestedUserAction;
  }

  @JsonProperty("messages")
  public List<Message> getMessages() {
    return messages;
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, suggestedUserAction, messages);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SourceState)) {
      return false;
    }
    SourceState that = (SourceState) obj;
    return Objects.equals(this.status, that.status) &&
      Objects.equals(this.suggestedUserAction, that.suggestedUserAction) &&
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

  private static SourceState getSourceState(SourceState.SourceStatus status, String suggestedUserAction, MessageLevel level, String... msgs) {
    List<Message> messageList = new ArrayList<>();
    for (String msg : msgs) {
      messageList.add(new Message(level, msg));
    }
    return new SourceState(status, suggestedUserAction, messageList);
  }

  public static SourceState warnState(String suggestedUserAction, String... e) {
    return getSourceState(SourceStatus.warn, suggestedUserAction, MessageLevel.WARN, e);
  }

  public static SourceState goodState(String... e){
    return getSourceState(SourceStatus.good, "", MessageLevel.INFO, e);
  }

  public static SourceState badState(String suggestedUserAction, String... e) {
    return getSourceState(SourceStatus.bad, suggestedUserAction, MessageLevel.ERROR, e);
  }

  public static SourceState badState(String suggestedUserAction, Exception e) {
    return getSourceState(SourceStatus.bad, suggestedUserAction, MessageLevel.ERROR, e.getMessage());
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
    if (suggestedUserAction != null && suggestedUserAction.length() > 0) {
      sb.append("\n\tSuggested User Action: ");
      sb.append(suggestedUserAction);
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
