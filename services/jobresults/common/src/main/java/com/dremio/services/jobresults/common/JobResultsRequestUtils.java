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
package com.dremio.services.jobresults.common;

import static io.grpc.MethodDescriptor.generateFullMethodName;

import com.dremio.service.jobresults.JobResultsResponse;
import com.dremio.service.jobresults.JobResultsServiceGrpc;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Utils class for customize handling of serializing/deserializing JobResultsRequest on-to/from wire
 * grpc stream.
 */
public class JobResultsRequestUtils {
  private static final String methodName = "jobResults";

  public static MethodDescriptor<JobResultsRequestWrapper, JobResultsResponse> getJobResultsMethod(
      BufferAllocator allocator) {
    return MethodDescriptor.<JobResultsRequestWrapper, JobResultsResponse>newBuilder()
        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
        .setFullMethodName(generateFullMethodName(JobResultsServiceGrpc.SERVICE_NAME, methodName))
        .setSampledToLocalTracing(true)
        .setRequestMarshaller(getJobResultsRequestMarshaller(allocator))
        .setResponseMarshaller(getJobResultsResponseMarshaller())
        .build();
  }

  private static MethodDescriptor.Marshaller<JobResultsResponse> getJobResultsResponseMarshaller() {
    return ProtoUtils.marshaller(JobResultsResponse.getDefaultInstance());
  }

  private static MethodDescriptor.Marshaller<JobResultsRequestWrapper>
      getJobResultsRequestMarshaller(BufferAllocator allocator) {
    return new JobResultsRequestWrapperMarshaller(allocator);
  }
}
