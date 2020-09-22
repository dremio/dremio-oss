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

import static com.dremio.service.job.proto.QueryType.UNKNOWN;
import static com.dremio.service.jobs.JobsProtoUtil.toBuf;
import static com.dremio.service.jobs.JobsProtoUtil.toStuff;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobCancellationInfo;
import com.dremio.service.job.proto.JobFailureInfo;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobProtobuf;

/**
 * Tests for {@code JobsProtoUtil}
 */
public class TestJobsProtoUtil {

  private static JobProtobuf.JobAttempt jobAttemptProtobuf;
  private static JobAttempt jobAttemptProtostuff;
  private static JobFailureInfo jobFailureInfoProtoStuff;
  private static JobProtobuf.JobFailureInfo jobFailureInfoProtoBuf;
  private static JobCancellationInfo jobCancellationInfoProtoStuff;
  private static JobProtobuf.JobCancellationInfo jobCancellationInfoProtobuf;

  @Before
  public void setUp() {
    final List<String> testDatasetPath = new ArrayList<>();
    testDatasetPath.add("first");
    testDatasetPath.add("second");
    testDatasetPath.add("third");

    final String jobIdString = UUID.randomUUID().toString();
    final JobId jobIdProtostuff = new JobId(jobIdString);
    final JobInfo jobInfoProtostuff = new JobInfo()
      .setJobId(jobIdProtostuff)
      .setSql("SQL")
      .setDatasetVersion("version")
      .setQueryType(UNKNOWN)
      .setDatasetPathList(testDatasetPath)
      .setUser(null); // test null conversion
    jobAttemptProtostuff = new JobAttempt()
      .setInfo(jobInfoProtostuff);

    final JobProtobuf.JobId jobIdProtobuf = JobProtobuf.JobId.newBuilder()
      .setId(jobIdString)
      .build();
    final JobProtobuf.JobInfo jobInfoProtobuf = JobProtobuf.JobInfo.newBuilder()
      .setJobId(jobIdProtobuf)
      .setSql("SQL")
      .setDatasetVersion("version")
      .setQueryType(JobProtobuf.QueryType.UNKNOWN)
      .addAllDatasetPath(testDatasetPath)
      .build();
    jobAttemptProtobuf = JobProtobuf.JobAttempt.newBuilder()
      .setInfo(jobInfoProtobuf)
      .build();

    //Instantiating objects for JobFailureInfo

    List<JobFailureInfo.Error> errors = new ArrayList<>();
    errors.add(new JobFailureInfo.Error()
      .setMessage("temp message")
      .setStartLine(1)
      .setEndLine(2)
      .setStartColumn(3)
      .setEndColumn(4));

    jobFailureInfoProtoStuff = new JobFailureInfo()
      .setErrorsList(errors)
      .setMessage("message")
      .setType(JobFailureInfo.Type.UNKNOWN);


    List<JobProtobuf.JobFailureInfo.Error> errorList = new ArrayList<>();
    errorList.add(JobProtobuf.JobFailureInfo.Error.newBuilder()
    .setMessage("temp message")
    .setStartLine(1)
    .setEndLine(2)
    .setStartColumn(3)
    .setEndColumn(4)
    .build());

    jobFailureInfoProtoBuf = JobProtobuf.JobFailureInfo.newBuilder()
      .addAllErrors(errorList)
      .setMessage("message")
      .setType(JobProtobuf.JobFailureInfo.Type.UNKNOWN)
      .build();

    //Instantiating objects for JobCancellationInfo

    jobCancellationInfoProtobuf = JobProtobuf.JobCancellationInfo.newBuilder()
      .setMessage("message")
      .build();

    jobCancellationInfoProtoStuff = new JobCancellationInfo().setMessage("messages");
  }

  @Test
  public void testToBuf() {
    JobProtobuf.JobAttempt resultJobAttempt = toBuf(jobAttemptProtostuff);
    assertEquals(resultJobAttempt, jobAttemptProtobuf);

    //test for jobFailureInfo
    JobProtobuf.JobFailureInfo jobFailureInfo = toBuf(jobFailureInfoProtoStuff);
    assertEquals(jobFailureInfoProtoBuf, jobFailureInfo);

    //test for jobCancellationInfo (-ve test case)
    JobProtobuf.JobCancellationInfo jobCancellationInfo = toBuf(jobCancellationInfoProtoStuff);
    assertNotEquals(jobCancellationInfoProtobuf, jobCancellationInfo);
  }

  @Test
  public void testToStuff() {
    JobAttempt resultJobAttempt = toStuff(jobAttemptProtobuf);
    assertEquals(resultJobAttempt, jobAttemptProtostuff);

    //test for jobFailureInfo
    JobFailureInfo jobFailureInfo = toStuff(jobFailureInfoProtoBuf);
    assertEquals(jobFailureInfoProtoStuff, jobFailureInfo);

    //test for jobCancellationinfo (-ve test case)
    JobCancellationInfo jobCancellationInfo = toStuff(jobCancellationInfoProtobuf);
    assertNotEquals(jobCancellationInfoProtoStuff, jobCancellationInfo);
  }
}
