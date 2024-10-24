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
package com.dremio.common.utils;

import com.dremio.common.nodes.EndpointHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;

public class ProfilesPathUtils {
  private static final String SEPARATOR = "/";
  private static final String INTERMEDIATE = "intermediate";
  private static final String FINAL = "final";

  private static final String PLANNING = "planning";
  private static final String TAIL = "tail";
  private static final String EXECUTOR = "executor";
  private static final String FULL = "Full";
  public static final String FINAL_EXECUTOR_PROFILE_SUFFIX = "_final";

  public static String buildProfilePrefix(UserBitShared.QueryId queryId) {
    return QueryIdHelper.getQueryId(queryId) + SEPARATOR;
  }

  public static String buildIntermediatePrefix(UserBitShared.QueryId queryId) {
    return buildProfilePrefix(queryId) + INTERMEDIATE + SEPARATOR;
  }

  public static String buildPlanningProfilePath(UserBitShared.QueryId queryId) {
    return buildIntermediatePrefix(queryId) + PLANNING;
  }

  public static String buildTailProfilePath(UserBitShared.QueryId queryId) {
    return buildIntermediatePrefix(queryId) + TAIL;
  }

  public static String buildExecutorProfilePrefix(UserBitShared.QueryId queryId) {
    return buildIntermediatePrefix(queryId) + EXECUTOR + SEPARATOR;
  }

  public static String buildExecutorProfilePath(
      UserBitShared.QueryId queryId, CoordinationProtos.NodeEndpoint endpoint, boolean isFinal) {
    String suffix = isFinal ? FINAL_EXECUTOR_PROFILE_SUFFIX : "";
    return buildExecutorProfilePrefix(queryId) + EndpointHelper.getMinimalString(endpoint) + suffix;
  }

  public static String buildFullProfilePath(UserBitShared.QueryId queryId) {
    return buildFinalPrefix(queryId) + FULL;
  }

  private static String buildFinalPrefix(UserBitShared.QueryId queryId) {
    return buildProfilePrefix(queryId) + FINAL + SEPARATOR;
  }
}
