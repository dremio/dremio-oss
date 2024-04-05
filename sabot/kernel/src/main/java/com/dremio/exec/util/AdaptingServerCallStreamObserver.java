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
package com.dremio.exec.util;

import io.grpc.stub.ServerCallStreamObserver;
import java.util.function.Function;

/**
 * ServerCallStreamObserver that gets the data from grpc api, do the conversion using converter, and
 * send it to delegateServerCallStreamObserver. Can be used in cases where we need to make a simple
 * method call to a grpc api which is implementing flow control
 */
public class AdaptingServerCallStreamObserver<T, V> extends ServerCallStreamObserver<T> {
  private final ServerCallStreamObserver<V> delegateServerCallStreamObserver;
  private final Function<T, V> converter;

  protected ServerCallStreamObserver<V> getDelegateServerCallStreamObserver() {
    return delegateServerCallStreamObserver;
  }

  public AdaptingServerCallStreamObserver(
      ServerCallStreamObserver<V> delegateServerCallStreamObserver, Function<T, V> converter) {
    this.delegateServerCallStreamObserver = delegateServerCallStreamObserver;
    this.converter = converter;
  }

  @Override
  public boolean isCancelled() {
    return delegateServerCallStreamObserver.isCancelled();
  }

  @Override
  public void setOnCancelHandler(Runnable onCancelHandler) {
    delegateServerCallStreamObserver.setOnCancelHandler(onCancelHandler);
  }

  @Override
  public void setCompression(String compression) {
    delegateServerCallStreamObserver.setCompression(compression);
  }

  @Override
  public boolean isReady() {
    return delegateServerCallStreamObserver.isReady();
  }

  @Override
  public void setOnReadyHandler(Runnable onReadyHandler) {
    delegateServerCallStreamObserver.setOnReadyHandler(onReadyHandler);
  }

  @Override
  public void disableAutoInboundFlowControl() {
    delegateServerCallStreamObserver.disableAutoInboundFlowControl();
  }

  @Override
  public void request(int count) {
    delegateServerCallStreamObserver.request(count);
  }

  @Override
  public void setMessageCompression(boolean enable) {
    delegateServerCallStreamObserver.setMessageCompression(enable);
  }

  @Override
  public void onNext(T value) {
    delegateServerCallStreamObserver.onNext(converter.apply(value));
  }

  @Override
  public void onError(Throwable t) {
    delegateServerCallStreamObserver.onError(t);
  }

  @Override
  public void onCompleted() {
    delegateServerCallStreamObserver.onCompleted();
  }
}
