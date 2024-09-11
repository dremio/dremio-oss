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
package com.dremio.service.coordinator.zk;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.coordinator.CoordinatorLostHandle;
import java.util.function.Predicate;

/** ZK Configuration */
@Options
public interface ZKClusterConfig {
  TypeValidators.BooleanValidator COORDINATOR_ZK_SUPERVISOR =
      new TypeValidators.BooleanValidator("coordinator_zk_supervisor", false);

  int getConnectionTimeoutMilliSecs();

  int getSessionTimeoutMilliSecs();

  String getRoot();

  int getRetryBaseDelayMilliSecs();

  long getInitialTimeoutMilliSecs();

  int getRetryMaxDelayMilliSecs();

  boolean isRetryUnlimited();

  boolean isConnectionHandleEnabled();

  long getRetryLimit();

  long getElectionTimeoutMilliSecs();

  long getElectionPollingMilliSecs();

  long getElectionDelayForLeaderCallbackMilliSecs();

  CoordinatorLostHandle getConnectionLostHandler();

  String getClusterId();

  String getConnection();

  Predicate<String> getFeatureEvaluator();

  int getZkSupervisorIntervalMilliSec();

  int getZkSupervisorReadTimeoutMilliSec();

  int getZkSupervisorMaxFailures();
}
