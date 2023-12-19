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
package com.dremio.exec.ops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.options.EagerCachingOptionManager;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.options.OptionManager;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.sabot.rpc.user.UserSession;

public class TestQueryContext extends BaseTestQuery {

  @Test
  public void testOptionManagerSetup() throws Exception {
    try (final QueryContext queryContext =
           new QueryContext(session(), getSabotContext(), UserBitShared.QueryId.getDefaultInstance());) {
      final OptionManagerWrapper  optionManager = (OptionManagerWrapper) queryContext.getOptions();
      final List<OptionManager> optionManagerList = optionManager.getOptionManagers();
      assertEquals(4, optionManagerList.size());
      assertTrue(optionManagerList.get(0) instanceof QueryOptionManager);
      assertTrue(optionManagerList.get(1) instanceof SessionOptionManager);
      assertTrue(optionManagerList.get(2) instanceof EagerCachingOptionManager);
      assertTrue(optionManagerList.get(3) instanceof DefaultOptionManager);
    }
  }

  private static UserSession session() {
    return UserSession.Builder.newBuilder()
      .withSessionOptionManager(
        new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
        getSabotContext().getOptionManager())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(UserServiceTestImpl.TEST_USER_1).build())
      .setSupportComplexTypes(true)
      .build();
  }
}
