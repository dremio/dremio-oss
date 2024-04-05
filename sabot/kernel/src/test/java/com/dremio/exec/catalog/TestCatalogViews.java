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
package com.dremio.exec.catalog;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import com.dremio.config.DremioConfig;
import com.dremio.test.TemporarySystemProperties;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestCatalogViews extends BaseTestQuery {
  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(20, TimeUnit.SECONDS);

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void before() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
  }

  @Test
  public void testQueryJsonView() throws Exception {
    test("use " + TEMP_SCHEMA);
    test(
        "create view json_view as \n"
            + "    SELECT \n"
            + "        convert_fromJSON('{\"x\": 17}') as obj");
    test("SELECT obj.x from json_view");
    test("drop view json_view");
  }
}
