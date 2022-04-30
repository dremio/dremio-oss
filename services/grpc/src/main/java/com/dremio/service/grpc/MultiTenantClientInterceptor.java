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

import static com.dremio.context.SupportContext.serializeSupportRoles;

import com.dremio.context.RequestContext;
import com.dremio.context.SupportContext;
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
 * Interceptor that populates MT gRPC headers from RequestContext.
 */
public class MultiTenantClientInterceptor implements ClientInterceptor {
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
      @Override
      public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {

        headers.put(HeaderKeys.PROJECT_ID_HEADER_KEY,
          RequestContext.current().get(TenantContext.CTX_KEY).getProjectId().toString());
        headers.put(HeaderKeys.ORG_ID_HEADER_KEY,
          RequestContext.current().get(TenantContext.CTX_KEY).getOrgId().toString());
        headers.put(HeaderKeys.USER_HEADER_KEY, RequestContext.current().get(UserContext.CTX_KEY).serialize());

        SupportContext supportContext = RequestContext.current().get(SupportContext.CTX_KEY);
        if (supportContext != null) {
          headers.put(HeaderKeys.SUPPORT_TICKET_HEADER_KEY, supportContext.getTicket());
          headers.put(HeaderKeys.SUPPORT_EMAIL_HEADER_KEY, supportContext.getEmail());
          headers.put(HeaderKeys.SUPPORT_ROLES_HEADER_KEY, serializeSupportRoles(supportContext.getRoles()));
        }

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
