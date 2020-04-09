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
package com.dremio.jdbc.impl;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.dremio.exec.rpc.ConnectionFailedException;
import com.dremio.exec.rpc.RpcException;
import com.google.common.base.Strings;

/**
 * Utility class for mapping RpcExceptions to SQLExceptions.
 */
public class DremioExceptionMapper {
  private static final String STATUS_PREFIX = " Status: ";
  private static final Map<String, String> errorStatusSQLStateMap = new HashMap<String, String>() {{
    put("AUTH_FAILED", "28000");
  }};

  /**
   * Private constructor to prevent instantiation.
   */
  private DremioExceptionMapper() {
  }

  /**
   * Map the given RpcException into an equivalent SQLException.
   *
   * The message used for the SQLException is the same as that used by the RpcException.
   * An appropriate SQLState will be chosen for the RpcException, if one is available.
   *
   * @param rpcException The remote exception to map.
   *
   * @return The equivalently mapped SQLException.
   */
  public static SQLException map(RpcException rpcException) {
    return map(rpcException, rpcException.getMessage());
  }

  /**
   * Map the given RpcException into an equivalent SQLException.
   *
   * An appropriate SQLState will be chosen for the RpcException, if one is available.
   *
   * @param rpcException The remote exception to map.
   * @param message The message format string to use for the SQLException.
   * @param args The arguments for the message.
   *
   * @return The equivalently mapped SQLException.
   */
  public static SQLException map(RpcException rpcException, String message, String... args) {
    if (message != null && args != null) {
      message = String.format(message, args);
    }

    if (rpcException instanceof ConnectionFailedException) {
      return new SQLException(message, "08001", rpcException);
    }

    return new SQLException(message, errorStatusSQLStateMap.get(getStatus(rpcException)), rpcException);
  }

  /**
   * Extract the status string from the RpcException.
   *
   * @param rpcException The exception to extract the status string from.
   *
   * @return The extracted status string, null if one cannot be extracted.
   */
  private static String getStatus(RpcException rpcException) {
    if (rpcException.getStatus() != null) {
      // If there's a status, get it directly.
      return rpcException.getStatus();
    }

    // As a fallback, attempt to get the status from the error message. This is ugly, so only do a best-effort here.
    String message = rpcException.getMessage();
    if (Strings.isNullOrEmpty(message)) {
      return null;
    }

    final int statusIndex = message.indexOf(STATUS_PREFIX);
    if (statusIndex == -1) {
      return null;
    }

    message = message.substring(statusIndex + STATUS_PREFIX.length());
    final int endIndex = message.indexOf(',');
    if (endIndex == -1) {
      return null;
    }
    return message.substring(0, endIndex);
  }
}
