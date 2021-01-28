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

import javax.inject.Provider;

import com.dremio.context.RequestContext;
import com.dremio.context.TenantContext;
import com.dremio.context.UserContext;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Interceptor for cases where the client is a per-tenant service. Use the defaults unless the caller overrides them.
 */
public class SingleTenantClientInterceptor implements ClientInterceptor {
  private Provider<RequestContext> defaultRequestContext;

  public SingleTenantClientInterceptor(Provider<RequestContext> defaultRequestContext) {
    this.defaultRequestContext = defaultRequestContext;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
      @Override
      public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {

        TenantContext tenantContext = RequestContext.current().get(TenantContext.CTX_KEY);
        if (tenantContext == null) {
          tenantContext = defaultRequestContext.get().get(TenantContext.CTX_KEY);
        }
        headers.put(HeaderKeys.PROJECT_ID_HEADER_KEY, tenantContext.getProjectId().toString());
        headers.put(HeaderKeys.ORG_ID_HEADER_KEY, tenantContext.getOrgId().toString());

        UserContext userContext = RequestContext.current().get(UserContext.CTX_KEY);
        if (userContext == null) {
          userContext = defaultRequestContext.get().get(UserContext.CTX_KEY);
        }
        headers.put(HeaderKeys.USER_HEADER_KEY, userContext.serialize());

        super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
          @Override
          public void onHeaders(Metadata headers) {
            super.onHeaders(headers);
          }
        }, headers);
      }
    };
  }
}
