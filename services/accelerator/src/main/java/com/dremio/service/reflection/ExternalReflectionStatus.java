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
package com.dremio.service.reflection;

/**
 * External reflection status
 */
public class ExternalReflectionStatus {

  /**
   *
   */
  public enum STATUS {
    OK,
    INVALID,    // reflection definition is no longer valid
    OUT_OF_SYNC  // something changed that may have caused this reflection to be invalid
  }

  private final STATUS configStatus;

  public ExternalReflectionStatus(STATUS configStatus) {
    this.configStatus = configStatus;
  }

  public STATUS getConfigStatus() {
    return configStatus;
  }
}
