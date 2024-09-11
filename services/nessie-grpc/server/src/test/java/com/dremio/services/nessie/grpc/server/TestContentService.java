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
package com.dremio.services.nessie.grpc.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.services.nessie.grpc.client.GrpcClientBuilder;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.IcebergTable;

@ExtendWith(MockitoExtension.class)
class TestContentService {

  private static final Branch BRANCH = Branch.of("main", "1122334455667788");

  @Mock private static org.projectnessie.services.spi.ContentService bridge;
  private static final ContentService service = new ContentService(() -> bridge);

  private static Server server;
  private static ManagedChannel channel;
  private static NessieApiV2 api;

  @BeforeAll
  static void setup() throws IOException {
    String name = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
    channel = InProcessChannelBuilder.forName(name).directExecutor().build();

    api = GrpcClientBuilder.builder().withChannel(channel).build(NessieApiV2.class);
  }

  @AfterAll
  static void stopServer() {
    channel.shutdownNow();
    server.shutdownNow();
  }

  @BeforeEach
  void resetMocks() throws NessieNotFoundException {
    reset(bridge);

    when(bridge.getMultipleContents(any(), any(), any(), anyBoolean(), anyBoolean()))
        .thenReturn(
            GetMultipleContentsResponse.of(
                Collections.singletonList(
                    GetMultipleContentsResponse.ContentWithKey.of(
                        ContentKey.of("test"), IcebergTable.of("loc", 1, 2, 3, 4))),
                BRANCH));
  }

  @Test
  void testForWrite() throws NessieNotFoundException {
    api.getContent().refName("main").key(ContentKey.of("test")).forWrite(false).get();
    verify(bridge, times(1)).getMultipleContents(any(), any(), any(), anyBoolean(), eq(false));

    api.getContent().refName("main").key(ContentKey.of("test")).forWrite(true).get();
    verify(bridge, times(1)).getMultipleContents(any(), any(), any(), anyBoolean(), eq(true));
  }

  @Test
  void testForWriteSingle() throws NessieNotFoundException {
    api.getContent().refName("main").getSingle(ContentKey.of("test"));
    verify(bridge, times(1)).getMultipleContents(any(), any(), any(), anyBoolean(), eq(false));

    api.getContent().refName("main").forWrite(true).getSingle(ContentKey.of("test"));
    verify(bridge, times(1)).getMultipleContents(any(), any(), any(), anyBoolean(), eq(true));
  }
}
