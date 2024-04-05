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
package com.dremio.service.jobcounts.server;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.DirectProvider;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;
import com.dremio.service.grpc.GrpcServerBuilderFactory;
import com.dremio.service.grpc.SimpleGrpcChannelBuilderFactory;
import com.dremio.service.grpc.SimpleGrpcServerBuilderFactory;
import com.dremio.service.jobcounts.DeleteJobCountsRequest;
import com.dremio.service.jobcounts.GetJobCountsRequest;
import com.dremio.service.jobcounts.JobCountType;
import com.dremio.service.jobcounts.JobCountUpdate;
import com.dremio.service.jobcounts.JobCounts;
import com.dremio.service.jobcounts.JobCountsClient;
import com.dremio.service.jobcounts.UpdateJobCountsRequest;
import com.dremio.telemetry.utils.TracerFacade;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Tests LocalJobCountsServer. */
public class TestLocalJobCountsServer {
  public static final SabotConfig DEFAULT_SABOT_CONFIG = SabotConfig.forClient();
  public static final ScanResult CLASSPATH_SCAN_RESULT =
      ClassPathScanner.fromPrescan(DEFAULT_SABOT_CONFIG);
  private final GrpcChannelBuilderFactory grpcChannelBuilderFactory =
      new SimpleGrpcChannelBuilderFactory(TracerFacade.INSTANCE);
  private final GrpcServerBuilderFactory grpcServerBuilderFactory =
      new SimpleGrpcServerBuilderFactory(TracerFacade.INSTANCE);
  private LocalKVStoreProvider kvStoreProvider;
  private LocalJobCountsServer server;
  private JobCountsClient client;

  @Before
  public void setUp() throws Exception {
    final CoordinationProtos.NodeEndpoint node =
        CoordinationProtos.NodeEndpoint.newBuilder().setFabricPort(30).build();

    kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();

    server =
        new LocalJobCountsServer(
            grpcServerBuilderFactory, () -> kvStoreProvider, DirectProvider.wrap(node));
    server.start();

    client = new JobCountsClient(grpcChannelBuilderFactory, DirectProvider.wrap(node));
    client.start();
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(client, server, kvStoreProvider);
  }

  @Test
  public void testJobCountsUpdateAndDeletes() throws Exception {
    String catalogId = UUID.randomUUID().toString();
    String refId1 = UUID.randomUUID().toString();
    String refId2 = UUID.randomUUID().toString();
    String refId3 = UUID.randomUUID().toString();
    UpdateJobCountsRequest updateRequest =
        UpdateJobCountsRequest.newBuilder()
            .addCountUpdates(
                JobCountUpdate.newBuilder().setType(JobCountType.CATALOG).setId(catalogId).build())
            .addCountUpdates(
                JobCountUpdate.newBuilder().setType(JobCountType.CONSIDERED).setId(refId1).build())
            .addCountUpdates(
                JobCountUpdate.newBuilder().setType(JobCountType.MATCHED).setId(refId2).build())
            .build();
    client.getBlockingStub().updateJobCounts(updateRequest);

    UpdateJobCountsRequest updateRequest2 =
        UpdateJobCountsRequest.newBuilder()
            .addCountUpdates(
                JobCountUpdate.newBuilder().setType(JobCountType.CATALOG).setId(catalogId).build())
            .addCountUpdates(
                JobCountUpdate.newBuilder().setType(JobCountType.CONSIDERED).setId(refId1).build())
            .addCountUpdates(
                JobCountUpdate.newBuilder().setType(JobCountType.MATCHED).setId(refId2).build())
            .addCountUpdates(
                JobCountUpdate.newBuilder().setType(JobCountType.CHOSEN).setId(refId3).build())
            .build();
    client.getBlockingStub().updateJobCounts(updateRequest2);

    UpdateJobCountsRequest updateRequest3 =
        UpdateJobCountsRequest.newBuilder()
            .addCountUpdates(
                JobCountUpdate.newBuilder().setType(JobCountType.CONSIDERED).setId(refId2).build())
            .addCountUpdates(
                JobCountUpdate.newBuilder().setType(JobCountType.MATCHED).setId(refId3).build())
            .build();
    client.getBlockingStub().updateJobCounts(updateRequest3);

    List<String> ids = Arrays.asList(catalogId, refId1, refId2, refId3);
    verifyJobCount(JobCountType.CATALOG, ids, Arrays.asList(2, 0, 0, 0));
    verifyJobCount(JobCountType.CONSIDERED, ids, Arrays.asList(0, 2, 1, 0));
    verifyJobCount(JobCountType.MATCHED, ids, Arrays.asList(0, 0, 2, 1));
    verifyJobCount(JobCountType.CHOSEN, ids, Arrays.asList(0, 0, 0, 1));

    DeleteJobCountsRequest deleteJobCountsRequest =
        DeleteJobCountsRequest.newBuilder().addIds(catalogId).addIds(refId1).build();
    client.getBlockingStub().deleteJobCounts(deleteJobCountsRequest);

    verifyJobCount(JobCountType.CATALOG, ids, Arrays.asList(0, 0, 0, 0));
    verifyJobCount(JobCountType.CONSIDERED, ids, Arrays.asList(0, 0, 1, 0));
    verifyJobCount(JobCountType.MATCHED, ids, Arrays.asList(0, 0, 2, 1));
    verifyJobCount(JobCountType.CHOSEN, ids, Arrays.asList(0, 0, 0, 1));

    DeleteJobCountsRequest deleteJobCountsRequest2 =
        DeleteJobCountsRequest.newBuilder()
            .addIds(catalogId)
            .addIds(refId1)
            .addIds(refId2)
            .addIds(refId3)
            .build();
    client.getBlockingStub().deleteJobCounts(deleteJobCountsRequest2);

    verifyJobCount(JobCountType.CATALOG, ids, Arrays.asList(0, 0, 0, 0));
    verifyJobCount(JobCountType.CONSIDERED, ids, Arrays.asList(0, 0, 0, 0));
    verifyJobCount(JobCountType.MATCHED, ids, Arrays.asList(0, 0, 0, 0));
    verifyJobCount(JobCountType.CHOSEN, ids, Arrays.asList(0, 0, 0, 0));
  }

  private void verifyJobCount(JobCountType type, List<String> ids, List<Integer> expectedCounts)
      throws Exception {
    Preconditions.checkArgument(
        ids.size() == expectedCounts.size(), "Both lists should be of same size");
    GetJobCountsRequest getRequest =
        GetJobCountsRequest.newBuilder()
            .setType(type)
            .addAllIds(ids)
            .setJobCountsAgeInDays(30)
            .build();
    JobCounts jobCounts = client.getBlockingStub().getJobCounts(getRequest);
    for (int i = 0; i < expectedCounts.size(); i++) {
      Assert.assertEquals((int) expectedCounts.get(i), (int) jobCounts.getCountList().get(i));
    }
  }
}
