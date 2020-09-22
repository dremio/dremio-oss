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
package com.dremio.exec.store.sys;

import java.util.Iterator;

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.service.executor.ExecutorServiceImpl;
import com.dremio.sabot.exec.context.OperatorContext;

public class NodeIterator implements Iterator<Object> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NodeIterator.class);

  private boolean beforeFirst = true;
  private final OperatorContext context;
  private final SabotContext dbContext;

  public NodeIterator(final SabotContext dbContext, OperatorContext c) {
    this.dbContext = dbContext;
    this.context = c;
  }

  @Override
  public boolean hasNext() {
    return beforeFirst;
  }

  @Override
  public Object next() {
    if (!beforeFirst) {
      throw new IllegalStateException();
    }

    beforeFirst = false;
    return NodeInstance.fromStats(ExecutorServiceImpl.getNodeStatsFromContext(dbContext), dbContext.getEndpoint());
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
