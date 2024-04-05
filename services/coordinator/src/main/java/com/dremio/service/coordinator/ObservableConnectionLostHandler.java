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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import org.apache.curator.framework.state.ConnectionState;

public interface ObservableConnectionLostHandler extends CoordinatorLostHandle {
  Supplier<CoordinatorLostHandle> OBSERVABLE_LOST_HANDLER =
      () ->
          new ObservableConnectionLostHandler() {
            private final List<LostConnectionObserver> connectionLostObservers =
                new CopyOnWriteArrayList<>();
            private volatile boolean sessionLost = false;

            @Override
            public void attachObserver(LostConnectionObserver observer) {
              connectionLostObservers.add(observer);
            }

            @Override
            public void handleConnectionState(ConnectionState state) {
              if (connectionLostObservers.isEmpty()) {
                return;
              }
              if (ConnectionState.LOST.equals(state)) {
                connectionLostObservers.forEach(LostConnectionObserver::notifyLostConnection);
                sessionLost = true;
              }
              if (sessionLost && state.isConnected()) {
                connectionLostObservers.forEach(
                    LostConnectionObserver::notifyConnectionRegainedAfterLost);
                sessionLost = false;
              }
            }

            @Override
            public void handleMasterDown(TaskLeaderStatusListener listener) {}

            @Override
            public boolean stateLoggingEnabled() {
              return true;
            }
          };

  void attachObserver(LostConnectionObserver observer);
}
