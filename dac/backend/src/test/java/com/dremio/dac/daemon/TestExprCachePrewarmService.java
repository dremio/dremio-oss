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

import java.util.List;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.ExecConstants;
import com.google.common.io.Resources;

/**
 * TestExprsCachePrewarmService tests the cached projector expressions are built on exectuor startup
 */
public class TestExprCachePrewarmService extends BaseTestServer {
  private static ExprCachePrewarmService exprCachePrewarmService;

  @BeforeClass
  public static void init() throws Exception {
    System.setProperty(ExecConstants.CODE_CACHE_LOCATION_PROP, Resources.getResource("prewarmfiles/").getPath());
    System.setProperty(ExecConstants.CODE_CACHE_PREWARM_PROP, "true");
    BaseTestServer.init(true);
    System.clearProperty(ExecConstants.CODE_CACHE_LOCATION_PROP);
    System.clearProperty(ExecConstants.CODE_CACHE_PREWARM_PROP);
    exprCachePrewarmService = getExecutorDaemon().getBindingProvider().lookup(ExprCachePrewarmService.class);
  }

  @Test
  public void testProjectExpressionsAreBuilt() throws Exception {
    Assert.assertFalse(exprCachePrewarmService == null);
    List<Future<?>> futureList = exprCachePrewarmService.getProjectSetupFutures();
    Assert.assertTrue(futureList.size() == 1);
    futureList.get(0).get();
    Assert.assertEquals(1, exprCachePrewarmService.numCached());
  }
}
