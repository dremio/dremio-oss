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

package com.dremio.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.collect.Lists;
import java.util.Collection;
import org.junit.Test;

public class TestDremioVersionUtils {

  @Test
  public void testCompatibleNodeEndpoints() {
    NodeEndpoint nep1 =
        NodeEndpoint.newBuilder()
            .setAddress("localhost")
            .setDremioVersion(DremioVersionInfo.getVersion())
            .build();

    NodeEndpoint nep2 =
        NodeEndpoint.newBuilder()
            .setAddress("localhost")
            .setDremioVersion("incompatibleVersion")
            .build();

    Collection<NodeEndpoint> nodeEndpoints =
        DremioVersionUtils.getCompatibleNodeEndpoints(Lists.newArrayList(nep1, nep2));
    assertEquals(1, nodeEndpoints.size());
    assertSame(nep1, nodeEndpoints.toArray()[0]);
  }
}
