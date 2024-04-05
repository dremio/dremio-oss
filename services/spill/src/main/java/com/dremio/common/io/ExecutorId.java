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
package com.dremio.common.io;

import java.util.Objects;

/** Uniquely identifies an executor. */
public final class ExecutorId {
  static final String PREFIX_DELIMITER = "_";
  private static final String PREFIX_FORMAT =
      "%s" + PREFIX_DELIMITER + "%s" + PREFIX_DELIMITER + "%d";

  private final String hostName;
  private final int port;

  public ExecutorId(String hostName, int port) {
    this.hostName = hostName.replaceFirst("^(http[s]?://www\\.|http[s]?://|www\\.)", "");
    this.port = port;
  }

  public String getHostName() {
    return hostName;
  }

  public int getPort() {
    return port;
  }

  public String toPrefix(String purpose) {
    return String.format(PREFIX_FORMAT, purpose, hostName, port);
  }

  @Override
  public String toString() {
    return "ExecutorId{" + "hostName='" + hostName + '\'' + ", port=" + port + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExecutorId that = (ExecutorId) o;
    return port == that.port && hostName.equals(that.hostName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostName, port);
  }
}
