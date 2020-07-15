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
import java.util.Locale;
import java.util.Map;

import com.dremio.exec.rpc.ConnectionFailedException;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcExceptionStatus;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

/**
 * Utility class for mapping RpcExceptions to SQLExceptions.
 */
public class DremioExceptionMapper {
  // For references of Sql State Code, please see http://www.wiscorp.com/sql200n.zip
  // or for a quick reference, please see https://en.wikipedia.org/wiki/SQLSTATE
  private static final String INVALID_AUTHORIZATION_SPECIFICATION = "28000";
  private static final String DISCONNECT_ERROR = "01002";
  private static final String UNABLE_TO_ESTABLISH_CONNECTION = "08001";

  private static final String STATUS_PREFIX = " Status: ";

  private static final Map<String, String> errorStatusSQLStateMap =
    new ImmutableMap.Builder<String, String>()
      .put(RpcExceptionStatus.AUTH_FAILED, INVALID_AUTHORIZATION_SPECIFICATION)
      .put(RpcExceptionStatus.CONNECTION_INVALID, DISCONNECT_ERROR)
      .build();
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
    return new SQLException(message, getSqlCode(rpcException), rpcException);
  }

  /**
   * Find the corresponding SQL State Code based on the RpcException
   *
   * @param rpcException the exception to evaluate.
   * @return a SQL State Code as a string.
   */
  private static String getSqlCode(RpcException rpcException) {
    String sqlCode = getSqlCodeFromRpcExceptionType(rpcException);
    if (sqlCode == null) {
      sqlCode = getSqlCodeFromRpcExceptionStatus(rpcException);
    }
    return sqlCode;
  }

  /**
   * Finds the corresponding SQL State Code based on the RpcExceptionType.
   *
   * @param rpcException the exception to evaluate.
   * @return a SQL State Code as a string.
   */
  private static String getSqlCodeFromRpcExceptionType(RpcException rpcException) {
    if (rpcException instanceof ConnectionFailedException) {
      return UNABLE_TO_ESTABLISH_CONNECTION;
    }
    return null;
  }

  /**
   * Finds the corresponding SQL State Code based on the Rpc Status of a RpcException.
   * If no RPC status is set in the exception, then it evaluates the exception message for a status.
   *
   * @param rpcException the exception to evaluate.
   * @return a SQL State Code as a string.
   */
  private static String getSqlCodeFromRpcExceptionStatus(RpcException rpcException) {
    if (rpcException.getStatus() != null) {
      // If there's a status, get it directly.
      return errorStatusSQLStateMap.get(rpcException.getStatus());
    }

    // As a fallback, attempt to get the status from the error message. This is ugly, so only do a best-effort here.
    return getSqlCodeFromMessage(rpcException.getMessage());
  }

  /**
   * Find the corresponding SQL State Code based on a given exception message.
   *
   * @param message the message to evaluate.
   * @return a SQL State Code as a string.
   */
  private static String getSqlCodeFromMessage(String message) {
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
    return errorStatusSQLStateMap.get(message.substring(0, endIndex).toUpperCase(Locale.getDefault()));
  }
}
