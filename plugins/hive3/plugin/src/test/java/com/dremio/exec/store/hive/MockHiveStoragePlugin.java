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
package com.dremio.exec.store.hive;

import com.dremio.exec.server.SabotContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.mockito.Mockito;

/** Helper class for using HiveStoragePlugin that can be start()ed in tests. */
public class MockHiveStoragePlugin extends Hive3StoragePlugin {
  private final List<HiveClient> createdClients = new ArrayList<>();

  public MockHiveStoragePlugin(HiveConf hiveConf, SabotContext context, String name) {
    super(hiveConf, context, name);
  }

  public void verifyClosed() {
    createdClients.stream().forEach(client -> Mockito.verify(client).close());
  }

  public int getClientCount() {
    return createdClients.size();
  }

  @Override
  protected HiveClient createConnectedClient() {
    final HiveClient mockClient = Mockito.mock(HiveClient.class);
    createdClients.add(mockClient);
    return mockClient;
  }

  @Override
  protected HiveClient createConnectedClientWithAuthz(
      String userName, UserGroupInformation ugiForRpc) {
    return createConnectedClient();
  }
}
