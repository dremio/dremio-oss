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
package com.dremio.dac.daemon;

import static com.dremio.dac.model.system.ServerStatus.MASTER_DOWN;
import static com.dremio.dac.model.system.ServerStatus.OK;

import javax.inject.Provider;

import com.dremio.dac.model.system.ServerStatus;
import com.dremio.dac.service.exec.MasterStatusListener;

/**
 * Monitor health of dremio daemon.
 * TODO: monitor errors and timeouts, after certain timeouts or fatal errors mark service unhealthy
 */
public class ServerHealthMonitor {
  private final Provider<MasterStatusListener> masterStatusListener;

  public ServerHealthMonitor(Provider<MasterStatusListener> masterStatusListener) {
    this.masterStatusListener = masterStatusListener;
  }

  public boolean isHealthy() {
    return masterStatusListener.get().isMasterUp();
  }

  public ServerStatus getStatus() {
    if (!masterStatusListener.get().isMasterUp()) {
      return MASTER_DOWN;
    }
    return OK;
  }
}
