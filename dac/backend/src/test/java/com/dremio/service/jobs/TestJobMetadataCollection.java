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
import static org.junit.Assert.assertTrue;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.concurrent.DremioFutures;
import com.dremio.dac.model.job.JobDetailsUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.proto.UserProtos.GetCatalogsResp;
import com.dremio.exec.proto.UserProtos.LikeFilter;
import com.dremio.exec.rpc.ConnectionThrottle;
import com.dremio.exec.rpc.RpcException;
import com.dremio.proto.model.attempts.RequestType;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserResultsListener;
import com.dremio.service.Pointer;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.users.UserService;

/**
 * Make sure that job metadata is collected for various query types.
 */
public class TestJobMetadataCollection extends BaseTestServer {

  private final DremioClient rpc;
  private final JobsService jobs;

  public TestJobMetadataCollection() throws RpcException{
    rpc = getRpcClient();
    jobs = l(JobsService.class);
  }

  @Test
  public void getCatalogs() throws Exception {
    GetCatalogsResp resp = DremioFutures.getChecked(
      rpc.getCatalogs(LikeFilter.getDefaultInstance()),
      RpcException.class,
      RpcException::mapException
    );
    JobDetailsUI job = getDetails(resp.getQueryId());

    assertEquals(RequestType.GET_CATALOGS, job.getRequestType());
  }

  @Test
  public void prepare() throws Exception {
    CreatePreparedStatementResp resp = DremioFutures.getChecked(
      rpc.createPreparedStatement("select * from sys.options"),
      RpcException.class,
      RpcException::mapException
    );
    JobDetailsUI job = getDetails(resp.getQueryId());
    assertTrue(job.getTimeSpentInPlanning() > 0);
    assertTrue(job.getSql() != null);
    // expect no dataset path since sys.options isn't in the namespace.
    assertTrue(job.getDatasetPathList() == null);

    assertEquals("sys", job.getParentsList().get(0).getDatasetPathList().get(0));
    assertEquals(com.dremio.service.job.proto.QueryType.JDBC, job.getQueryType());

    final CountDownLatch latch = new CountDownLatch(1);
    final Pointer<QueryId> id = new Pointer<>();
    rpc.executePreparedStatement(resp.getPreparedStatement().getServerHandle(), new UserResultsListener() {

      @Override
      public void submissionFailed(UserException ex) {
        latch.countDown();
        Assert.fail(ex.toString());
      }

      @Override
      public void queryIdArrived(QueryId queryId) {
        id.value = queryId;
      }

      @Override
      public void queryCompleted(QueryState state) {
        latch.countDown();
      }

      @Override
      public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
        result.release();
      }
    });
    latch.await();

    JobDetailsUI job2 = getDetails(id.value);
    assertTrue(job2.getSql() != null);
  }



  private JobDetailsUI getDetails(QueryId id) throws JobNotFoundException {
    JobDetailsRequest request = JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(toId(id)))
      .build();
    return JobDetailsUI.of(jobs.getJobDetails(request));
  }

  private JobId toId(QueryId id){
    return new JobId(new UUID(id.getPart1(), id.getPart2()).toString());
  }

  @BeforeClass
  public static void init() throws Exception {
    enableDefaultUser(true);
    BaseTestServer.init();
    getPopulator().addDefaultFirstUser(l(UserService.class), newNamespaceService());
  }
}
