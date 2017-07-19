/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.sql.Timestamp;
import java.util.Iterator;

import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.server.SabotContext;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.collect.ImmutableList;

/**
 * Iterator which returns {@link QueryInfo} for each query foreman running in this node
 */
public class QueryIterator implements Iterator<Object> {
  private final Iterator<QueryProfile> iter;

  public QueryIterator(SabotContext dbContext, OperatorContext c) {
    iter = ImmutableList.copyOf(dbContext.getRunningQueryProvider().get().getRunningQueries()).iterator();
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Object next() {
    QueryProfile profile = iter.next();
    return new QueryInfo(profile.getForeman().getAddress(),
            profile.getUser(),
            QueryIdHelper.getQueryId(profile.getId()),
            profile.getQuery(),
            new Timestamp(profile.getStart()));
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public static class QueryInfo {
    /**
     * The host where foreman is running
     */
    public final String foreman;
    /**
     * User who submitted query
     */
    public final String user;
    public final String queryId;
    /**
     * Query sql string
     */
    public final String query;
    public final Timestamp startTime;

    public QueryInfo(String foreman, String user, String queryId, String query, Timestamp startTime) {
      this.foreman = foreman;
      this.user = user;
      this.queryId = queryId;
      this.query = query;
      this.startTime = startTime;
    }
  }
}
