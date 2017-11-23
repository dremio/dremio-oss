/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.UUID;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.beans.NodeEndpoint;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.google.protobuf.InvalidProtocolBufferException;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;

/**
 * utility class.
 */
// package private
final class JobsServiceUtil {

  private JobsServiceUtil() {
  }

  static NodeEndpoint toStuff(CoordinationProtos.NodeEndpoint pb) {
    // TODO use schemas to do this...
    NodeEndpoint ep = new NodeEndpoint();
    ProtobufIOUtil.mergeFrom(pb.toByteArray(), ep, NodeEndpoint.getSchema());
    return ep;
  }

  static CoordinationProtos.NodeEndpoint toPB(NodeEndpoint stuf) {
    // TODO use schemas to do this...
    LinkedBuffer buffer = LinkedBuffer.allocate();
    byte[] bytes = ProtobufIOUtil.toByteArray(stuf, stuf.cachedSchema(), buffer);
    try {
      return CoordinationProtos.NodeEndpoint.PARSER.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Cannot convert from protostuff to protobuf");
    }
  }

  /**
   * Translate job id to external id.
   *
   * @param jobId job id
   * @return external id
   */
  static ExternalId getJobIdAsExternalId(JobId jobId) {
    UUID id = UUID.fromString(jobId.getId());
    return ExternalId.newBuilder()
        .setPart1(id.getMostSignificantBits())
        .setPart2(id.getLeastSignificantBits())
        .build();
  }

  /**
   * Translate external id to job id.
   *
   * @param id external id
   * @return job id
   */
  static JobId getExternalIdAsJobId(ExternalId id) {
    return new JobId(new UUID(id.getPart1(), id.getPart2()).toString());
  }

  /**
   * Returns job status, given query state.
   *
   * @param state query state
   * @return job status
   */
  static JobState queryStatusToJobStatus(QueryState state) {
    switch (state) {
    case STARTING:
      return JobState.RUNNING;
    case RUNNING:
      return JobState.RUNNING;
    case ENQUEUED:
      return JobState.RUNNING;
    case COMPLETED:
      return JobState.COMPLETED;
    case CANCELED:
      return JobState.CANCELED;
    case FAILED:
      return JobState.FAILED;
    default:
      return JobState.NOT_SUBMITTED;
    }
  }
}
