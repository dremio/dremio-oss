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

import java.util.StringJoiner;

import com.dremio.config.DremioConfig;

/**
 * Small main method to determine the roles of a node
 * e.g. executor, coordinator, master, or any combination of the three.
 *
 * returns
 */
public class GetRolesCommand {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GetRolesCommand.class);

  public static void main(String[] args) throws Exception {
    DremioConfig config = DremioConfig.create();
    final Boolean isExecutor = config.getBoolean(DremioConfig.ENABLE_EXECUTOR_BOOL);
    final Boolean isCoordinator = config.getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL);
    final Boolean isMaster = config.getBoolean(DremioConfig.ENABLE_MASTER_BOOL);
    StringJoiner sj = new StringJoiner(",");
    if (isExecutor) {
      sj.add("executor");
    }
    if (isCoordinator) {
      sj.add("coordinator");
    }
    if (isMaster) {
      sj.add("master");
    }
    System.out.println(sj.toString());
  }
}
