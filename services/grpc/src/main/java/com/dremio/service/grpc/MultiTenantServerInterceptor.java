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

import com.dremio.context.RequestContext;
import com.dremio.context.TenantContext;
import com.dremio.context.UserContext;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Interceptor that populates MT RequestContext entries from gRPC headers.
 */
public class MultiTenantServerInterceptor implements ServerInterceptor {
  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
    ServerCall<ReqT, RespT> call,
    final Metadata requestHeaders,
    ServerCallHandler<ReqT, RespT> next) {
    try {
      RequestContext context = RequestContext.empty()
        .with(TenantContext.CTX_KEY, new TenantContext(requestHeaders.get(HeaderKeys.TENANT_ID_HEADER_KEY)))
        .with(UserContext.CTX_KEY, new UserContext(requestHeaders.get(HeaderKeys.USER_HEADER_KEY)));

      return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(context.call(() -> next.startCall(call, requestHeaders))) {
        @Override
        public void onHalfClose() {
          context.run(super::onHalfClose);
        }

        @Override
        public void onCancel() {
          context.run(super::onCancel);
        }

        @Override
        public void onComplete() {
          context.run(super::onComplete);
        }

        @Override
        public void onReady() {
          context.run(super::onReady);
        }

        @Override
        public void onMessage(ReqT message) {
          context.run(() -> super.onMessage(message));
        }
      };
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
