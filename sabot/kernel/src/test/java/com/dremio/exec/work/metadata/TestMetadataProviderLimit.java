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
package com.dremio.exec.work.metadata;

import static org.junit.Assert.assertEquals;

import com.dremio.BaseTestQuery;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.proto.UserProtos.GetTablesResp;
import java.util.List;
import java.util.Properties;
import org.junit.Test;

/** Test metadata provider with limit. */
public class TestMetadataProviderLimit extends BaseTestQuery {

  @Test
  public void tables() throws Exception {
    final Properties properties = cloneDefaultTestConfigProperties();
    properties.setProperty("MaxMetadataCount", "4");
    updateClient(properties);

    GetTablesResp resp = client.getTables(null, null, null, null).get();

    assertEquals(UserProtos.RequestStatus.OK, resp.getStatus());
    List<UserProtos.TableMetadata> tables = resp.getTablesList();
    assertEquals(4, tables.size());
  }
}
