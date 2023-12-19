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
package com.dremio.context;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

/**
 * This can be used to ensure Stream evaluation happens under a specific RequestContext
 */
final class RequestContextAwareSplitIterator<T> implements Spliterator<T> {

  private final RequestContext reqCtx;
  private final Spliterator<T> delegate;

  RequestContextAwareSplitIterator(RequestContext reqCtx, Spliterator<T> delegate) {
    this.reqCtx = Preconditions.checkNotNull(reqCtx);
    this.delegate = Preconditions.checkNotNull(delegate);
  }

  @Override
  public boolean tryAdvance(Consumer action) {
    return reqCtx.callUnchecked(() -> delegate.tryAdvance(action));
  }

  @Override
  public Spliterator<T> trySplit() {
    Spliterator<T> delegateSplit = reqCtx.callUnchecked(() -> delegate.trySplit());
    if (delegateSplit == null) {
      return null;
    }
    return new RequestContextAwareSplitIterator(reqCtx, delegateSplit);
  }

  @Override
  public long estimateSize() {
    return reqCtx.callUnchecked(() -> delegate.estimateSize());
  }

  @Override
  public int characteristics() {
    return reqCtx.callUnchecked(() -> delegate.characteristics());
  }

  @Override
  public Comparator<? super T> getComparator() {
    return reqCtx.callUnchecked(() -> delegate.getComparator());
  }
}
