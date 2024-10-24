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

package com.dremio.exec.work.protector;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.commandpool.CommandPoolFactory;
import org.junit.Test;

/** Tests {@link ForemenWorkManager} */
public class TestForemenWorkManager {

  @Test
  public void testSubmitWork() throws Exception {
    // Arrange - setup
    DremioConfig config = mock(DremioConfig.class);
    doReturn(false).when(config).getBoolean(any());

    CommandPool commandPool = CommandPoolFactory.INSTANCE.newPool(config, null);
    ForemenWorkManager foremenWorkManager =
        new ForemenWorkManager(null, null, () -> commandPool, null, null, null, null, null, null);

    foremenWorkManager = spy(foremenWorkManager);
    UserException userException =
        UserException.resourceError().message(UserException.QUERY_REJECTED_MSG).buildSilently();
    doThrow(userException)
        .when(foremenWorkManager)
        .submitWorkCommand(any(), any(), any(), any(), any(), any());
    final UserResult[] userResult = new UserResult[1];

    UserResponseHandler userResponseHandler =
        new UserResponseHandler() {
          @Override
          public void sendData(
              RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener,
              QueryWritableBatch result) {}

          @Override
          public void completed(UserResult result) {
            userResult[0] = result;
          }
        };

    // Act - make the request
    UserRequest userRequest = new UserRequest(UserProtos.RpcType.RUN_QUERY, new Object(), true);
    ExternalId externalId = ExternalId.newBuilder().setPart1(1L).setPart2(2L).build();
    foremenWorkManager.submitWork(externalId, null, userResponseHandler, userRequest, null, null);

    // Assert - Verify the results
    assertEquals(
        UserException.QUERY_REJECTED_MSG + ". Root cause: " + UserException.QUERY_REJECTED_MSG,
        userResult[0].getException().getMessage());

    commandPool.close();
    foremenWorkManager.close();
  }
}
