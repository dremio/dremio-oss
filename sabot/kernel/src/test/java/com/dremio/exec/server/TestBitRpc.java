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
package com.dremio.exec.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.threads.SendingAccountor;
import com.dremio.sabot.threads.SendingMonitor;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceManager;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBitRpc extends ExecTest {

  @Test
  public void testSendingMonitorBackPressure() throws Exception {
    SharedResourceManager resourceManager =
        SharedResourceManager.newBuilder().addGroup("test").build();
    SharedResource resource =
        resourceManager
            .getGroup("test")
            .createResource("test-backpressure", SharedResourceType.TEST);

    int LIMIT = ExecConstants.OUTSTANDING_RPCS_PER_TUNNEL.getDefault().getNumVal().intValue();
    SendingMonitor sendingMonitor = new SendingMonitor(resource, new SendingAccountor(), LIMIT);
    RpcOutcomeListener<Ack> wrappedListener =
        sendingMonitor.wrap(Mockito.mock(RpcOutcomeListener.class));

    // all batches sent below the monitor limit should not cause the resource to become unavailable
    for (int i = 0; i < LIMIT - 1; i++) {
      sendingMonitor.increment();
      assertTrue(resourceManager.isAvailable());
    }

    // make sure sending a batch after that makes the resource unavailable
    // also make sure all calls to the wrapped listener will make the resource available again
    sendingMonitor.increment();
    assertFalse(resource.isAvailable());
    wrappedListener.success(null, null);
    assertTrue(resource.isAvailable());

    sendingMonitor.increment();
    assertFalse(resource.isAvailable());
    wrappedListener.failed(null);
    assertTrue(resource.isAvailable());

    sendingMonitor.increment();
    assertFalse(resource.isAvailable());
    wrappedListener.interrupted(null);
    assertTrue(resource.isAvailable());
  }
}
