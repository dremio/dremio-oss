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

import java.util.Collections;
import java.util.Iterator;

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.work.CacheManagerStoragePluginInfo;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Iterator which returns cache manager storage plugins information
 */
public class CacheManagerStoragePluginIterator implements Iterator<Object> {
  private final Iterator<CacheManagerStoragePluginInfo> iter;

  CacheManagerStoragePluginIterator(SabotContext sabotContext, OperatorContext operatorContext) {
    iter = (sabotContext.getFileSystemWrapper() instanceof CacheManagerStatsProvider) ?
        ((CacheManagerStatsProvider) sabotContext.getFileSystemWrapper()).getStoragePluginStats().iterator() :
        Collections.EMPTY_LIST.iterator();
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Object next() {
    return iter.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
