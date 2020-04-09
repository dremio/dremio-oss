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
package com.dremio.service.jobtelemetry.server.store;

import java.util.Optional;
import java.util.stream.Stream;

import com.dremio.exec.proto.CoordExecRPC.ExecutorQueryProfile;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.service.Service;

/**
 * Store used to store and retrieve profile details
 */
public interface ProfileStore extends Service {

  /**
   * Put planning profile for a query.
   *
   * @param queryId queryId
   * @param planningProfile profile
   */
  void putPlanningProfile(UserBitShared.QueryId queryId, QueryProfile planningProfile);

  /**
   * Get planning profile for a query.
   *
   * @return profile
   */
  Optional<QueryProfile> getPlanningProfile(UserBitShared.QueryId queryId);

  /**
   * Put tail profile for a query.
   *
   * @param queryId queryId
   * @param profile profile
   */
  void putTailProfile(UserBitShared.QueryId queryId, QueryProfile profile);

  /**
   * Get planning profile for a query.
   *
   * @param queryId queryId
   * @return profile
   */
  Optional<QueryProfile> getTailProfile(UserBitShared.QueryId queryId);

  /**
   * Put planning profile for a query.
   *
   * @param queryId queryId
   * @param fullProfile profile
   */
  void putFullProfile(UserBitShared.QueryId queryId, QueryProfile fullProfile);

  /**
   * Get planning profile for a query.
   *
   * @return profile
   */
  Optional<QueryProfile> getFullProfile(UserBitShared.QueryId queryId);

  /**
  /**
   * Put executor profile for a given query.
   *
   * @param queryId queryId
   * @param endpoint executor endpoint
   * @param executorQueryProfile profile
   */
  void putExecutorProfile(UserBitShared.QueryId queryId,
                          CoordinationProtos.NodeEndpoint endpoint,
                          ExecutorQueryProfile executorQueryProfile);

  /**
   * Get all executor profiles for a given query.
   *
   * @param prefix prefix
   * @return stream of executor profiles.
   */
  Stream<ExecutorQueryProfile> getAllExecutorProfiles(UserBitShared.QueryId queryId);

  /**
   * Delete all profile entries except the full profile for a query.
   *
   * @param queryId queryId.
   */
  void deleteSubProfiles(UserBitShared.QueryId queryId);

  /**
   * Delete all profile entries for a query.
   *
   * @param queryId queryId.
   */
  void deleteProfile(UserBitShared.QueryId queryId);
}
