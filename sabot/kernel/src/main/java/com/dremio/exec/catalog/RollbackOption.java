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
package com.dremio.exec.catalog;

public class RollbackOption {
  public enum Type {
    SNAPSHOT, // Roll table back to previous SNAPSHOT
    TIME // Roll table back to previous timestamp
  }

  private final Type type;
  private final Long value; // Snapshot id or timestamp in Millis
  private final String literalValue; // The literal snapshot id or timestamp value.

  public RollbackOption(Type type, Long value, String literalValue) {
    this.type = type;
    this.value = value;
    this.literalValue = literalValue;
  }

  public Type getType() {
    return type;
  }

  public Long getValue() {
    return value;
  }

  public String getLiteralValue() {
    return literalValue;
  }
}
