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
package com.dremio.dac.resource;

import static org.junit.Assert.assertEquals;

import com.dremio.dac.model.system.Nodes;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.dremio.service.jobs.JobsService;
import com.dremio.services.fabric.api.FabricService;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Provider;
import javax.ws.rs.core.SecurityContext;
import org.junit.Test;
import org.mockito.Mockito;

/** Tests {@link SystemResource} API */
public class TestNodeActivityWithUnresponsiveExecutor extends BaseTestServer {
  @Test
  public void testNodeActivityWithUnresponsiveExecutor() throws Exception {
    Provider<SabotContext> context = Mockito.spy(p(SabotContext.class));
    SabotContext sabotContext = Mockito.spy(l(SabotContext.class));
    Mockito.when(context.get()).thenReturn(sabotContext);
    Provider<ExecutorServiceClientFactory> executorServiceClientFactoryProvider =
        Mockito.spy(p(ExecutorServiceClientFactory.class));
    SystemResource systemResource =
        new SystemResource(
            context,
            p(FabricService.class),
            p(JobsService.class),
            executorServiceClientFactoryProvider,
            l(SecurityContext.class),
            l(BufferAllocatorFactory.class),
            l(ProjectOptionManager.class));
    List<CoordinationProtos.NodeEndpoint> executors = new ArrayList<>(sabotContext.getExecutors());
    String fakeAddress = "0.0.0.0";
    CoordinationProtos.NodeEndpoint unresponsiveEp =
        CoordinationProtos.NodeEndpoint.newBuilder()
            .setAddress(fakeAddress)
            .setAvailableCores(1)
            .setFabricPort(1)
            .setConduitPort(1)
            .setDremioVersion(executors.get(0).getDremioVersion())
            .build();
    executors.add(unresponsiveEp);

    Mockito.when(sabotContext.getExecutors()).thenReturn(executors);

    List<Nodes.NodeInfo> nodes = systemResource.getNodes();
    assertEquals(
        1,
        nodes.stream()
            .filter(
                node -> {
                  if (fakeAddress.equals(node.getIp()) && "red".equals(node.getStatus())) {
                    if (node.getDetails() != null) {
                      return node.getDetails().contains(Nodes.NodeDetails.NO_RESPONSE.getValue());
                    }
                  }
                  return false;
                })
            .count());
  }
}
