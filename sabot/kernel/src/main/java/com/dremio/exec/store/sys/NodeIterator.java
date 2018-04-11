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
package com.dremio.exec.store.sys;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.work.WorkStats;
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

  public static class NodeInstance {
    public String hostname;
    public String ip_address;
    public int user_port;
    public int fabric_port;
    public double cluster_load;
    public int configured_max_width;
    public int actual_max_width;
    public boolean current;
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
    final NodeEndpoint ep = dbContext.getEndpoint();
    NodeInstance i = new NodeInstance();
    i.current = false; // disable current field for now.
    i.hostname = ep.getAddress();
    try {
      i.ip_address = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      // no op
    }
    i.user_port = ep.getUserPort();
    i.fabric_port = ep.getFabricPort();
    final WorkStats stats = dbContext.getWorkStatsProvider().get();
    i.cluster_load = stats.getClusterLoad();
    i.configured_max_width = (int)dbContext.getClusterResourceInformation().getAverageExecutorCores(dbContext.getOptionManager());

    i.actual_max_width = (int) Math.max(1, i.configured_max_width * stats.getMaxWidthFactor());
    return i;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
