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
package com.dremio.storage;

import static org.junit.Assert.assertEquals;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.logical.LogicalPlan;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.test.DremioTest;
import java.util.Collection;
import org.junit.Test;

public class CheckStorageConfig extends DremioTest {

  @Test
  public void ensureStorageEnginePickup() {
    Collection<?> engines = CLASSPATH_SCAN_RESULT.getImplementations(StoragePluginConfig.class);
    assertEquals(engines.size(), 1);
  }

  @Test
  public void checkPlanParsing() {
    LogicalPlan plan =
        LogicalPlan.parse(
            new LogicalPlanPersistence(CLASSPATH_SCAN_RESULT),
            readResourceAsString("/storage_engine_plan.json"));
  }
}
