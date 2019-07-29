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
import static org.junit.Assert.assertNotNull;

import org.apache.calcite.avatica.util.Quoting;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.proto.UserProtos.GetServerMetaResp;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.proto.UserProtos.ServerMeta;

/**
 * Tests for server metadata provider APIs.
 */
public class TestServerMetaProvider extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestServerMetaProvider.class);

  @Test
  public void testServerMeta() throws Exception {
    GetServerMetaResp resp = client.getServerMeta().get();
    assertNotNull(resp);
    assertEquals(RequestStatus.OK, resp.getStatus());
    assertNotNull(resp.getServerMeta());

    ServerMeta serverMeta = resp.getServerMeta();
    logger.trace("Server metadata: {}", serverMeta);

    assertEquals(Quoting.DOUBLE_QUOTE.string, serverMeta.getIdentifierQuoteString());
  }
}
