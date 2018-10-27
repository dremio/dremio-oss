/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.exec;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.server.SabotContext;
import com.dremio.sabot.task.TaskPool;
import com.dremio.sabot.task.TaskPoolFactory;
import com.dremio.sabot.task.TaskPools;
import com.dremio.service.BindingCreator;
import com.dremio.service.Service;
import com.google.common.base.Preconditions;

/**
 * Instantiates {@link TaskPool} and adds to the providers' registry
 */
public class TaskPoolInitializer implements Service {

  private final Provider<SabotContext> sContext;
  private final BindingCreator bindingCreator;

  private TaskPool pool;

  public TaskPoolInitializer(Provider<SabotContext> sContext, BindingCreator bindingCreator) {
    this.sContext = Preconditions.checkNotNull(sContext, "SabotContext provider required");
    this.bindingCreator = Preconditions.checkNotNull(bindingCreator, "BindingCreator required");
  }

  @Override
  public void start() throws Exception {
    final SabotContext context = sContext.get();
    final TaskPoolFactory factory = TaskPools.newFactory(context.getConfig());
    pool = factory.newInstance(context.getOptionManager(), context.getDremioConfig());

    bindingCreator.bind(TaskPool.class, pool);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(pool);
  }
}
