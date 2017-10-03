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
package com.dremio.exec.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.junit.Test;

import com.dremio.exec.server.SabotContext;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.Lists;

public class TestRootSchemaAccess {

  private final NamespaceService ns = mock(NamespaceService.class);
  final SabotContext sabotContext = mock(SabotContext.class);

  @Test
  public void asNonSystemUser() throws Exception {
    when(ns.getSources()).thenReturn(Lists.newArrayList(new SourceConfig().setName("__internal"),
        new SourceConfig().setName("notinternal")));
    when(ns.getSpaces()).thenReturn(new ArrayList<SpaceConfig>());
    when(ns.getHomeSpaces()).thenReturn(new ArrayList<HomeConfig>());
    RootSchema schema = new RootSchema(ns, sabotContext, SchemaConfig.newBuilder("testuser").build(),
        new SchemaTreeProvider.MetadataStatsCollector());
    assertEquals(1, schema.getSubSchemaNames().size());
    assertTrue(schema.getSubSchemaNames().contains("notinternal"));
  }

  @Test
  public void asSystemUser() throws Exception {
    when(ns.getSources()).thenReturn(Lists.newArrayList(new SourceConfig().setName("__internal"),
        new SourceConfig().setName("notinternal")));
    when(ns.getSpaces()).thenReturn(new ArrayList<SpaceConfig>());
    when(ns.getHomeSpaces()).thenReturn(new ArrayList<HomeConfig>());
    RootSchema schema = new RootSchema(ns, sabotContext, SchemaConfig.newBuilder(SystemUser.SYSTEM_USERNAME).build(),
        new SchemaTreeProvider.MetadataStatsCollector());
    assertEquals(2, schema.getSubSchemaNames().size());
    assertTrue(schema.getSubSchemaNames().contains("notinternal"));
    assertTrue(schema.getSubSchemaNames().contains("__internal"));
  }

  @Test
  public void asNonSystemUserButExpose() throws Exception {
    when(ns.getSources()).thenReturn(Lists.newArrayList(new SourceConfig().setName("__internal"),
        new SourceConfig().setName("notinternal")));
    when(ns.getSpaces()).thenReturn(new ArrayList<SpaceConfig>());
    when(ns.getHomeSpaces()).thenReturn(new ArrayList<HomeConfig>());
    RootSchema schema = new RootSchema(ns, sabotContext,
        SchemaConfig.newBuilder("testuser").exposeInternalSources(true).build(),
        new SchemaTreeProvider.MetadataStatsCollector());
    assertEquals(2, schema.getSubSchemaNames().size());
    assertTrue(schema.getSubSchemaNames().contains("notinternal"));
    assertTrue(schema.getSubSchemaNames().contains("__internal"));
  }

  @Test
  public void homeIsSpecial() throws Exception {
    when(ns.getSources()).thenReturn(Lists.newArrayList(new SourceConfig().setName("__internal"),
        new SourceConfig().setName("notinternal"), new SourceConfig().setName("__home")));
    when(ns.getSpaces()).thenReturn(new ArrayList<SpaceConfig>());
    when(ns.getHomeSpaces()).thenReturn(new ArrayList<HomeConfig>());
    RootSchema schema = new RootSchema(ns, sabotContext,
        SchemaConfig.newBuilder("testuser").build(),
        new SchemaTreeProvider.MetadataStatsCollector());
    assertEquals(2, schema.getSubSchemaNames().size());
    assertTrue(schema.getSubSchemaNames().contains("notinternal"));
    assertTrue(schema.getSubSchemaNames().contains("__home"));
  }
}
