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
package com.dremio.service.jobs;

import io.grpc.stub.StreamObserver;

/**
 * Observer that ignores all notifications.
 *
 * @param <T> stream value type
 */
final class NoopStreamObserver<T> implements StreamObserver<T> {

  @SuppressWarnings({"rawtypes", "RawTypeCanBeGeneric"})
  public static final StreamObserver INSTANCE = new NoopStreamObserver<>();

  @SuppressWarnings("unchecked")
  public static <T> StreamObserver<T> instance() {
    return (StreamObserver<T>) INSTANCE;
  }

  private NoopStreamObserver() {}

  @Override
  public void onNext(T value) {}

  @Override
  public void onError(Throwable t) {}

  @Override
  public void onCompleted() {}
}
