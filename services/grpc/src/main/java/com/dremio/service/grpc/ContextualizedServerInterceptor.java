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

import java.util.Map;

import com.dremio.context.ExecutorToken;
import com.dremio.context.RequestContext;
import com.dremio.context.SerializableContextTransformer;
import com.dremio.context.SupportContext;
import com.dremio.context.TenantContext;
import com.dremio.context.UserContext;
import com.google.common.collect.ImmutableList;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Interceptor that populates RequestContext from gRPC headers.
 */
public class ContextualizedServerInterceptor implements ServerInterceptor {
  /**
   * Used to copy ExecutorToken so that it can be used for other gRPC calls in the context of the original API.
   */
  public static ContextualizedServerInterceptor buildExecutorTokenCopier() {
    return new ContextualizedServerInterceptor(ImmutableList.of(
      new ExecutorToken.Transformer()
    ));
  }

  public static ContextualizedServerInterceptor buildBasicContextualizedServerInterceptor() {
    return new ContextualizedServerInterceptor(ImmutableList.of(
      new TenantContext.Transformer(),
      new UserContext.Transformer(),
      new SupportContext.Transformer()
    ));
  }

  private final ImmutableList<SerializableContextTransformer> transformers;

  public ContextualizedServerInterceptor(ImmutableList<SerializableContextTransformer> transformers)
  {
    this.transformers = transformers;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
    ServerCall<ReqT, RespT> call,
    final Metadata requestHeaders,
    ServerCallHandler<ReqT, RespT> next) {
    try {
      final Map<String, String> headers = SerializableContextTransformer.convert(requestHeaders);
      RequestContext contextBuilder = RequestContext.current();
      for (SerializableContextTransformer transformer : transformers) {
        contextBuilder = transformer.deserialize(headers, contextBuilder);
      }

      final RequestContext context = contextBuilder;
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
