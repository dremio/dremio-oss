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
package com.dremio.common.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.opentracing.Tracer;

public class ScheduledContextMigratingExecutorService<E extends ScheduledExecutorService> extends ContextMigratingExecutorService implements ScheduledExecutorService {

  public ScheduledContextMigratingExecutorService(E delegate, Tracer tracer) {
    super(delegate, tracer);
  }


  @Override
  public ScheduledFuture<?> schedule(Runnable command,
                                     long delay, TimeUnit unit) {
    return ((ScheduledExecutorService)getDelegate()).schedule(decorate(command), delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable,
                                         long delay, TimeUnit unit) {
    return ((ScheduledExecutorService)getDelegate()).schedule(decorate(callable), delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                long initialDelay,
                                                long period,
                                                TimeUnit unit) {
    return ((ScheduledExecutorService)getDelegate()).scheduleAtFixedRate(decorate(command), initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return ((ScheduledExecutorService)getDelegate()).scheduleWithFixedDelay(decorate(command), initialDelay, delay, unit);
  }
}
