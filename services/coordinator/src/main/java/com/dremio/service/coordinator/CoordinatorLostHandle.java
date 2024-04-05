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
package com.dremio.service.coordinator;

import org.apache.curator.framework.state.ConnectionState;

/** Handler class that handles zk connection lost and master down */
public interface CoordinatorLostHandle {
  CoordinatorLostHandle NO_OP =
      new CoordinatorLostHandle() {
        @Override
        public void handleConnectionState(ConnectionState state) {}

        @Override
        public void handleMasterDown(TaskLeaderStatusListener listener) {}

        @Override
        public boolean stateLoggingEnabled() {
          return true;
        }
      };

  void handleConnectionState(ConnectionState state);

  void handleMasterDown(TaskLeaderStatusListener listener);

  boolean stateLoggingEnabled();
}
