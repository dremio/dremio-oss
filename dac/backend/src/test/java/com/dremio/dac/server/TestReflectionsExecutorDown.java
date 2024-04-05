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
package com.dremio.dac.server;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.service.accelerator.BaseTestReflection;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.RefreshPolicyType;
import com.dremio.service.reflection.ReflectionMonitor;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionType;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test what happens when we try to refresh a reflection while an executor is down. */
public class TestReflectionsExecutorDown extends BaseTestReflection {

  private ReflectionMonitor monitor = newReflectionMonitor(100, 10_000);

  @BeforeClass
  public static void setup() throws ExecutionSetupException, IOException, NamespaceException {
    Assume.assumeTrue(isMultinode()); // only run in multinode
    populateInitialData();
  }

  @Test
  public void testRefreshWhileExecutorDown() throws Exception {
    // reflection manager wakeup interval 1s
    // without the fix, reflection will try to refresh 3 times and eventually be marked as failed
    // while the test is
    // waiting for it to materialize
    setManagerRefreshDelay(1);

    // add simple pds with never refresh acceleration settings
    final NamespaceKey dsKey = new NamespaceKey(ImmutableList.of("DG", "dsg1"));
    final DatasetConfig dataset = getNamespaceService().getDataset(dsKey);
    setDatasetAccelerationSettings(dsKey, 0, 0, true, true, RefreshPolicyType.NEVER);

    // bring executor down
    closeExecutorDaemon();

    // create raw reflection
    final ReflectionId rawId =
        getReflectionService()
            .create(
                new ReflectionGoal()
                    .setType(ReflectionType.RAW)
                    .setDatasetId(dataset.getId().getId())
                    .setName("raw")
                    .setDetails(
                        new ReflectionDetails()
                            .setDisplayFieldList(
                                ImmutableList.<ReflectionField>builder()
                                    .add(new ReflectionField("user"))
                                    .add(new ReflectionField("age"))
                                    .build())));

    // refresh should fail
    try {
      monitor.waitUntilMaterialized(rawId);
      Assert.fail("reflection should not be refreshed");
    } catch (ReflectionMonitor.TimeoutException e) {
      // expected
    }

    final ReflectionEntry entry = getReflectionService().getEntry(rawId).get();
    Assert.assertNotNull(
        "reflection entry lastSubmittedRefresh field not set", entry.getLastSubmittedRefresh());
    Assert.assertEquals(
        "reflection should only try to refresh once", 1, entry.getNumFailures().intValue());
  }
}
