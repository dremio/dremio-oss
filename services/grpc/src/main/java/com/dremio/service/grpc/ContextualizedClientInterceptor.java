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

import com.dremio.context.CatalogContext;
import com.dremio.context.RequestContext;
import com.dremio.context.SerializableContext;
import com.dremio.context.SupportContext;
import com.dremio.context.TenantContext;
import com.dremio.context.UserContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Interceptor that populates gRPC headers from RequestContext.
 */
public class ContextualizedClientInterceptor implements ClientInterceptor {
  public static final class ContextTransferBehavior {
    private final RequestContext.Key<? extends SerializableContext> key;
    private final boolean required;
    private final Provider<SerializableContext> fallback;

    /**
     * Describes how each context should be transferred:
     * @param key Key to the context within RequestContext being transferred
     * @param required Whether this key must be present (will raise an exception if not present when required)
     * @param fallback When the context is not present, this fallback can be used instead (null for no fallback)
     */
    public ContextTransferBehavior(
      RequestContext.Key<? extends SerializableContext> key,
      boolean required,
      Provider<SerializableContext> fallback)
    {
      this.key = key;
      this.required = required;
      this.fallback = fallback;
    }

    public RequestContext.Key<? extends SerializableContext> getKey() {
      return key;
    }

    public boolean getRequired() {
      return required;
    }

    public Provider<SerializableContext> getFallback() {
      return fallback;
    }
  }

  public static ContextualizedClientInterceptor buildSingleTenantClientInterceptor()
  {
    return new ContextualizedClientInterceptor(ImmutableList.of(
      new ContextTransferBehavior(TenantContext.CTX_KEY, false, null),
      new ContextTransferBehavior(UserContext.CTX_KEY, false, null),
      new ContextTransferBehavior(CatalogContext.CTX_KEY, false, null)
      // TODO: Copy SupportContext too?
    ));
  }

  public static ContextualizedClientInterceptor buildSingleTenantClientInterceptorWithDefaults(
    Provider<RequestContext> defaultRequestContext)
  {
    return new ContextualizedClientInterceptor(ImmutableList.of(
      new ContextTransferBehavior(TenantContext.CTX_KEY, false, () -> defaultRequestContext.get().get(TenantContext.CTX_KEY)),
      new ContextTransferBehavior(UserContext.CTX_KEY, false, () -> defaultRequestContext.get().get(UserContext.CTX_KEY)),
      new ContextTransferBehavior(CatalogContext.CTX_KEY, false, null)
    ));
  }

  public static ContextualizedClientInterceptor buildMultiTenantClientInterceptor() {
    return new ContextualizedClientInterceptor(ImmutableList.of(
      new ContextTransferBehavior(TenantContext.CTX_KEY, true, null),
      new ContextTransferBehavior(CatalogContext.CTX_KEY, false, null),
      new ContextTransferBehavior(UserContext.CTX_KEY, true, null),
      new ContextTransferBehavior(SupportContext.CTX_KEY, false, null)
    ));
  }

  private final ImmutableList<ContextTransferBehavior> actions;

  public ContextualizedClientInterceptor(ImmutableList<ContextTransferBehavior> actions) {
    this.actions = actions;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (ContextTransferBehavior action : actions) {
          final SerializableContext context = RequestContext.current().get(action.getKey());
          if (context != null) {
            context.serialize(builder);
          } else if (action.getFallback() != null) {
            action.getFallback().get().serialize(builder);
          } else if (action.getRequired()) {
            throw new RuntimeException("RequestContext for " + action.getKey().getName() + " is required but not present");
          }
        }

        builder.build().forEach(
          (key, value) -> headers.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value));

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
