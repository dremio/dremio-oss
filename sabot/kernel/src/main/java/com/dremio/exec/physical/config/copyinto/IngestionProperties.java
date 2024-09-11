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
package com.dremio.exec.physical.config.copyinto;

import com.dremio.exec.physical.config.ExtendedProperty;

/** This class represents ingestion properties associated with trigger pipe functionality. */
public class IngestionProperties implements ExtendedProperty {
  private String pipeName;
  private String pipeId;
  private String ingestionSourceType;
  private String requestId;
  private Long notificationTimestamp;

  /**
   * Constructs a new {@code IngestionProperties} instance. Needed for
   * serialization-deserialization.
   */
  public IngestionProperties() {
    // Needed for serialization-deserialization
  }

  /**
   * Constructs a new {@code IngestionProperties} instance with the specified parameters.
   *
   * @param pipeName The name of the pipe.
   * @param pipeId The ID of the pipe.
   * @param ingestionSourceType The type of ingestion source.
   * @param requestId The request ID.
   * @param notificationTimestamp The notification timestamp.
   */
  public IngestionProperties(
      String pipeName,
      String pipeId,
      String ingestionSourceType,
      String requestId,
      Long notificationTimestamp) {
    this.pipeName = pipeName;
    this.pipeId = pipeId;
    this.ingestionSourceType = ingestionSourceType;
    this.requestId = requestId;
    this.notificationTimestamp = notificationTimestamp;
  }

  public String getPipeName() {
    return pipeName;
  }

  public String getPipeId() {
    return pipeId;
  }

  public String getIngestionSourceType() {
    return ingestionSourceType;
  }

  public String getRequestId() {
    return requestId;
  }

  public Long getNotificationTimestamp() {
    return notificationTimestamp;
  }

  @Override
  public String toString() {
    return "PipeProperties{"
        + "pipeName='"
        + pipeName
        + '\''
        + ", pipeId='"
        + pipeId
        + '\''
        + ", ingestionSourceType='"
        + ingestionSourceType
        + '\''
        + ", requestId='"
        + requestId
        + '\''
        + ", notificationTimestamp="
        + notificationTimestamp
        + '}';
  }
}
