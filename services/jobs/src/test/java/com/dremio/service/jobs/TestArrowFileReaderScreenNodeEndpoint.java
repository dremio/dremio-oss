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
package com.dremio.service.jobs;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.dremio.exec.enginemanagement.proto.EngineManagementProtos;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.beans.NodeEndpoint;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.exec.store.easy.arrow.ArrowFileReader;
import com.dremio.sabot.exec.context.OperatorContext;
import javax.inject.Provider;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ArrowFileReader} with ScreenNodeEndpoint in ArrowFileMetaData. Tests
 * defined in base-class are also tested.
 */
public class TestArrowFileReaderScreenNodeEndpoint extends TestArrowFileReader {
  private static final String engineIdString = "testEngineId";
  private static final String hostname = "localhost-test";

  @Override
  public OperatorContext getOperatorContext() {
    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);

    EngineManagementProtos.EngineId engineId =
        EngineManagementProtos.EngineId.newBuilder().setId(engineIdString).build();
    CoordinationProtos.NodeEndpoint nodeEndpoint =
        CoordinationProtos.NodeEndpoint.newBuilder()
            .setAddress(hostname)
            .setEngineId(engineId)
            .build();
    Provider<CoordinationProtos.NodeEndpoint> nodeEndpointProvider = () -> nodeEndpoint;
    when(operatorContext.getNodeEndpointProvider()).thenReturn(nodeEndpointProvider);
    return operatorContext;
  }

  @Override
  public void assertArrowFileMetadata(ArrowFileMetadata arrowFileMetadata) {
    NodeEndpoint nodeEndpoint = arrowFileMetadata.getScreenNodeEndpoint();
    assertEquals(nodeEndpoint.getAddress(), hostname);
    assertEquals(nodeEndpoint.getEngineId().getId(), engineIdString);
  }
}
