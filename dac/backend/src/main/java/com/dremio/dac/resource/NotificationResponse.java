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
package com.dremio.dac.resource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A notification response that is used for generic server confirmations
 */
public class NotificationResponse {

  /**
   * The type of response to return to the ui.
   */
  public static enum ResponseType {OK, INFO, WARN, ERROR}

  private final ResponseType type;
  private final String message;

  @JsonCreator
  public NotificationResponse(@JsonProperty("type") ResponseType type, @JsonProperty("name") String message) {
    super();
    this.type = type;
    this.message = message;
  }

  public ResponseType getType() {
    return type;
  }

  public String getMessage() {
    return message;
  }

}
