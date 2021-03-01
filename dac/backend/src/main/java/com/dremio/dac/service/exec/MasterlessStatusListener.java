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
package com.dremio.dac.service.exec;

import javax.inject.Provider;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.coordinator.ClusterServiceSetManager;

/**
 * Implementation of global MasterListener in masterless cluster
 */
public class MasterlessStatusListener extends MasterStatusListener {

  public MasterlessStatusListener(Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider, boolean isMaster) {
    super(clusterServiceSetManagerProvider, isMaster);
  }

  @Override
  public boolean isMasterUp() {
    return true;
  }

  @Override
  public CoordinationProtos.NodeEndpoint getMasterNode() {
    // no global master
    return null;
  }

  @Override
  public void waitForMaster() {
    // nothing to wait for
    return;
  }
}
