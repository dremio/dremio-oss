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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Predicate;

import com.google.common.io.ByteStreams;

import io.grpc.HandlerRegistry;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCallHandler;
import io.grpc.ServerMethodDefinition;

public class GrpcProxyHandlerRegistry extends HandlerRegistry {

  private final MethodDescriptor.Marshaller<byte[]> byteMarshaller = new ByteMarshaller();
  private final ServerCallHandler<byte[], byte[]> handler;
  private final Predicate<String> criteria;

  public GrpcProxyHandlerRegistry(ServerCallHandler<byte[], byte[]> handler, Predicate<String> criteria) {
    this.handler = handler;
    this.criteria = criteria;
  }

  @Override
  public ServerMethodDefinition<?, ?> lookupMethod(String methodName, String authority) {
    if (criteria.test(methodName)) {
      MethodDescriptor<byte[], byte[]> methodDescriptor
          = MethodDescriptor.newBuilder(byteMarshaller, byteMarshaller)
          .setFullMethodName(methodName)
          .setType(MethodDescriptor.MethodType.UNKNOWN)
          .build();
      return ServerMethodDefinition.create(methodDescriptor, handler);
    }
    return null;
  }

  private static final class ByteMarshaller implements MethodDescriptor.Marshaller<byte[]> {

    @Override
    public byte[] parse(InputStream stream) {
      try {
        return ByteStreams.toByteArray(stream);
      } catch (IOException ex) {
        throw new RuntimeException();
      }
    }

    @Override
    public InputStream stream(byte[] value) {
      return new ByteArrayInputStream(value);
    }
  }
}
