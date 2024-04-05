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
package com.dremio.service.reflection.refresh;

import static com.dremio.service.reflection.ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.utils.protos.AttemptId;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class TestRefreshHandler {

  @Test
  public void testGetRefreshPath() {
    final Materialization materialization = new Materialization();
    final String reflectionIdStr = "reflectionID";
    final ReflectionId reflectionId = new ReflectionId(reflectionIdStr);
    materialization.setReflectionId(reflectionId);
    final String materilizatonIdStr = "materilizatonID";
    final MaterializationId materializationId = new MaterializationId(materilizatonIdStr);
    materialization.setId(materializationId);

    final RefreshDecision decision = new RefreshDecision();
    final int attemptIdInt = 5;
    final AttemptId attemptId = mock(AttemptId.class);
    when(attemptId.getAttemptNum()).thenReturn(attemptIdInt);

    // test with IcebergDataset, not initial refresh
    materialization.setIsIcebergDataset(true);
    decision.setInitialRefresh(false);
    final String materilizatonBasePathstr = "materilizatonBasePathstr";
    materialization.setBasePath(materilizatonBasePathstr);
    // notice that in this case there is no + attemptIdInt
    Assert.assertEquals(
        ImmutableList.of(ACCELERATOR_STORAGEPLUGIN_NAME, reflectionIdStr, materilizatonBasePathstr),
        RefreshHandler.getRefreshPath(reflectionId, materialization, decision, attemptId));

    // test with IcebergDataset, initial refresh
    decision.setInitialRefresh(true);
    Assert.assertEquals(
        ImmutableList.of(
            ACCELERATOR_STORAGEPLUGIN_NAME,
            reflectionIdStr,
            materilizatonIdStr + "_" + attemptIdInt),
        RefreshHandler.getRefreshPath(reflectionId, materialization, decision, attemptId));

    // test with non-Iceberg dataset
    materialization.setIsIcebergDataset(false);
    decision.setInitialRefresh(false);
    Assert.assertEquals(
        ImmutableList.of(
            ACCELERATOR_STORAGEPLUGIN_NAME,
            reflectionIdStr,
            materilizatonIdStr + "_" + attemptIdInt),
        RefreshHandler.getRefreshPath(reflectionId, materialization, decision, attemptId));
  }
}
