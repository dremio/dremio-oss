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
package com.dremio.service.grpc;

import static io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor;

import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;

/** Static method to wrap a stub with interceptors for attaching service name and release name */
public class HeaderUtils<T> {

  public static <T extends AbstractStub<T>> T attachHeaders(
      T stub, String serviceName, String releaseName) {
    Metadata extraHeaders = createRoutingHeaders(serviceName, releaseName);
    return stub.withInterceptors(newAttachHeadersInterceptor(extraHeaders));
  }

  public static ClientInterceptor createRoutingInterceptor(String serviceName, String releaseName) {
    return newAttachHeadersInterceptor(createRoutingHeaders(serviceName, releaseName));
  }

  private static Metadata createRoutingHeaders(String serviceName, String releaseName) {
    Metadata extraHeaders = new Metadata();
    extraHeaders.put(HeaderKeys.RELEASE_NAME_HEADER_KEY, releaseName);
    extraHeaders.put(HeaderKeys.SERVICE_NAME_HEADER_KEY, serviceName);
    return extraHeaders;
  }
}
