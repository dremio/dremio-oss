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
package com.dremio.resource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Result of resource allocation call
 */
public class ResourceSchedulingResult {
  private final CompletableFuture<ResourceSet> resourceSetFuture;
  private final ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo;

  public ResourceSchedulingResult(
    ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo,
    CompletableFuture<ResourceSet> resourceSetFuture
  ) {
    this.resourceSchedulingDecisionInfo = resourceSchedulingDecisionInfo;
    this.resourceSetFuture = resourceSetFuture;
  }

  public ResourceSchedulingResult(
    ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo,
    ListenableFuture<ResourceSet> resourceSetFuture
  ) {
    this.resourceSchedulingDecisionInfo = resourceSchedulingDecisionInfo;
    this.resourceSetFuture = toCompletableFuture(resourceSetFuture);
  }

  public CompletableFuture<ResourceSet> getResourceSetFuture() {
    return resourceSetFuture;
  }

  public ResourceSchedulingDecisionInfo getResourceSchedulingDecisionInfo() {
    return resourceSchedulingDecisionInfo;
  }

  private <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> future) {
    CompletableFuture<T> completable = new CompletableFuture<T>() {
      @Override
      public boolean cancel(boolean mayInterrupt) {
        boolean result = future.cancel(mayInterrupt);
        super.cancel(mayInterrupt);
        return result;
      }
    };

    Futures.addCallback(future, new FutureCallback<T>() {
      @Override
      public void onSuccess(@Nullable T result) {
        completable.complete(result);
      }

      @Override
      public void onFailure(Throwable throwable) {
        completable.completeExceptionally(throwable);
      }
    }, ForkJoinPool.commonPool());
    return completable;
  }
}
