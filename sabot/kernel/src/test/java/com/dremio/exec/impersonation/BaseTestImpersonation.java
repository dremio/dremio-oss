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
package com.dremio.exec.impersonation;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.exec.BaseTestMiniDFS;

/**
 * Base class for impersonation tests
 */
public class BaseTestImpersonation extends BaseTestMiniDFS {
  // Test users and groups
  protected static final String[] org1Users = { "user0_1", "user1_1", "user2_1", "user3_1", "user4_1", "user5_1" };
  protected static final String[] org1Groups = { "group0_1", "group1_1", "group2_1", "group3_1", "group4_1", "group5_1" };
  protected static final String[] org2Users = { "user0_2", "user1_2", "user2_2", "user3_2", "user4_2", "user5_2" };
  protected static final String[] org2Groups = { "group0_2", "group1_2", "group2_2", "group3_2", "group4_2", "group5_2" };

  static {
    // "user0_1" belongs to "groups0_1". From "user1_1" onwards each user belongs to corresponding group and the group
    // before it, i.e "user1_1" belongs to "group1_1" and "group0_1" and so on.
    UserGroupInformation.createUserForTesting(org1Users[0], new String[]{org1Groups[0]});
    for(int i=1; i<org1Users.length; i++) {
      UserGroupInformation.createUserForTesting(org1Users[i], new String[] { org1Groups[i], org1Groups[i-1] });
    }

    UserGroupInformation.createUserForTesting(org2Users[0], new String[] { org2Groups[0] });
    for(int i=1; i<org2Users.length; i++) {
      UserGroupInformation.createUserForTesting(org2Users[i], new String[] { org2Groups[i], org2Groups[i-1] });
    }
  }

  /**
   * Start a MiniDFS cluster backed SabotNode cluster with impersonation enabled.
   * @param testClass
   * @throws Exception
   * @return configuration
   */
  protected static Configuration startMiniDfsCluster(String testClass) throws Exception {
    Configuration configuration = new Configuration();
    // Set the proxyuser settings so that the user who is running the SabotNodes/MiniDfs can impersonate other users.
    configuration.set(String.format("hadoop.proxyuser.%s.hosts", processUser), "*");
    configuration.set(String.format("hadoop.proxyuser.%s.groups", processUser), "*");
    startMiniDfsCluster(testClass, configuration);
    return configuration;
  }
}
