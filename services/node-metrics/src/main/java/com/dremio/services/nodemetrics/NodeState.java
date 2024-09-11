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
package com.dremio.services.nodemetrics;

public enum NodeState {
  NONE(null),
  NO_RESPONSE("NO_RESPONSE"),
  INVALID_VERSION("INVALID_VERSION");

  private final String value;

  NodeState(String value) {
    this.value = value;
  }

  public String toMessage(String message) {
    if (value == null || message == null) {
      return value;
    }
    return String.format("%s : %s", value, message);
  }

  public String getValue() {
    return value;
  }
}
