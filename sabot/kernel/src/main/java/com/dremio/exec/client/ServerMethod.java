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
package com.dremio.exec.client;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.dremio.common.Version;
import com.dremio.exec.proto.UserBitShared.RpcEndpointInfos;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.sabot.rpc.user.UserRpcUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A enumeration of server methods, and the version they were introduced
 *
 * it allows to introduce new methods without changing the protocol, with client
 * being able to gracefully handle cases were method is not handled by the server.
 */
public enum ServerMethod {
  /**
   * Submitting a query
   */
  RUN_QUERY(RpcType.RUN_QUERY, Constants.DREMIO_0_0_0, Constants.DRILL_0_0_0),

  /**
   * Plan a query without executing it
   */
  PLAN_QUERY(RpcType.QUERY_PLAN_FRAGMENTS, Constants.DREMIO_0_0_0, Constants.DRILL_0_0_0),

  /**
   * Cancel an existing query
   */
  CANCEL_QUERY(RpcType.CANCEL_QUERY, Constants.DREMIO_0_0_0, Constants.DRILL_0_0_0),

  /**
   * Resume a query
   */
  RESUME_PAUSED_QUERY(RpcType.RESUME_PAUSED_QUERY, Constants.DREMIO_0_0_0, Constants.DRILL_0_0_0),

  /**
   * Prepare a query for deferred execution
   */
  PREPARED_STATEMENT(RpcType.CREATE_PREPARED_STATEMENT, Constants.DREMIO_0_0_0, Constants.DRILL_1_8_0),

  /**
   * Get catalog metadata
   */
  GET_CATALOGS(RpcType.GET_CATALOGS, Constants.DREMIO_0_0_0, Constants.DRILL_1_8_0),

  /**
   * Get schemas metadata
   */
  GET_SCHEMAS(RpcType.GET_SCHEMAS, Constants.DREMIO_0_0_0, Constants.DRILL_1_8_0),

  /**
   * Get tables metadata
   */
  GET_TABLES(RpcType.GET_TABLES, Constants.DREMIO_0_0_0, Constants.DRILL_1_8_0),

  /**
   * Get columns metadata
   */
  GET_COLUMNS(RpcType.GET_COLUMNS, Constants.DREMIO_0_0_0, Constants.DRILL_1_8_0),

  /**
   * Get server metadata
   */
  GET_SERVER_META(RpcType.GET_SERVER_META, Constants.DREMIO_0_9_3, Constants.DRILL_1_10_0);

  private static class Constants {
    private static final Version DREMIO_0_0_0 = new Version("0.0.0", 0, 0, 0, 0, "");
    private static final Version DREMIO_0_9_3 = new Version("0.9.3", 0, 9, 3, 0, "");

    private static final Version DRILL_0_0_0 = new Version("0.0.0", 0, 0, 0, 0, "");
    private static final Version DRILL_1_8_0 = new Version("1.8.0", 1, 8, 0, 0, "");
    private static final Version DRILL_1_10_0 = new Version("1.10.0", 1, 8, 0, 0, "");
  }

  private static final Map<RpcType, ServerMethod> REVERSE_MAPPING;
  static {
    ImmutableMap.Builder<RpcType, ServerMethod> builder = ImmutableMap.builder();
    for(ServerMethod method: values()) {
      builder.put(method.rpcType, method);
    }
    REVERSE_MAPPING = Maps.immutableEnumMap(builder.build());
  }

  private final RpcType rpcType;
  private final Version minVersion;
  private final Version minDrillVersion;


  private ServerMethod(RpcType rpcType, Version minVersion, Version minDrillVersion) {
    this.rpcType = rpcType;
    this.minVersion = minVersion;
    this.minDrillVersion = minDrillVersion;
  }

  public Version getMinVersion() {
    return minVersion;
  }

  public Version getMinDrillVersion() {
    return minDrillVersion;
  }

  /**
   * Returns the list of methods supported by the server based on its advertised information.
   *
   * @param supported methods the list of supported rpc types
   * @return a immutable set of capabilities
   */
  static final Set<ServerMethod> getSupportedMethods(Iterable<RpcType> supportedMethods, RpcEndpointInfos serverInfos) {
    boolean supportedMethodsEmpty = true;
    ImmutableSet.Builder<ServerMethod> builder = ImmutableSet.builder();

    for(RpcType supportedMethod: supportedMethods) {
      supportedMethodsEmpty = false;
      ServerMethod method = REVERSE_MAPPING.get(supportedMethod);
      if (method == null) {
        // The server might have newer methods we don't know how to handle yet.
        continue;
      }
      builder.add(method);
    }

    if (!supportedMethodsEmpty) {
      return Sets.immutableEnumSet(builder.build());
    }

    final boolean isDremio;
    final Version serverVersion;
    if (serverInfos == null) {
      isDremio = false;
      serverVersion = Constants.DRILL_0_0_0;
    } else {
      String serverName = serverInfos.getName().toLowerCase(Locale.ROOT);
      isDremio = serverName.contains("dremio");
      serverVersion = UserRpcUtils.getVersion(serverInfos);
    }

    // Fallback to version detection if version is below Dremio 0.9.2
    // or Drill 1.10
    for(ServerMethod capability: ServerMethod.values()) {
      Version minVersion = isDremio ? capability.getMinVersion() : capability.getMinDrillVersion();
      if (serverVersion.compareTo(minVersion) >= 0) {
        builder.add(capability);
      }
    }

    return Sets.immutableEnumSet(builder.build());
  }
}
