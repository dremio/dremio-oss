/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.util.CancelFlag;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.SchemaConfig;

public class TestPlannerCallback extends BaseTestQuery {

  @BeforeClass
  public static void setupStoragePlugin() throws Exception {
    nodes[0].getContext().getStorage().addPlugin("fake", new FakeStoragePlugin());
  }

  @Test
  public void ensureCallbackIsRegistered() throws Exception {
    try{
      test("select * from cp.`employee.json`");
    }catch(Exception e){
      assertTrue(e.getMessage().contains("Statement preparation aborted"));
      return;
    }

    fail("Should have seen exception in planning due to planner initialization.");
  }

  public static class FakeStoragePlugin extends AbstractStoragePlugin {

    @Override
    public boolean folderExists(SchemaConfig schemaConfig, List folderPath) throws IOException {
      return true;
    }

    @Override
    public StoragePluginConfig getConfig() {
      return null;
    }

    @Override
    public PlannerCallback getPlannerCallback(QueryContext optimizerContext, PlannerPhase phase) {
      return new PlannerCallback(){
        @Override
        public void initializePlanner(RelOptPlanner planner) {
          CancelFlag f = planner.getContext().unwrap(CancelFlag.class);
          f.requestCancel();
        }
      };
    }
  }
}
