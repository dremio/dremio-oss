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
package com.dremio.exec.planner.acceleration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.tablefunctions.ExternalQueryScanCrel;
import com.dremio.exec.work.user.SubstitutionSettings;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableScan;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestMaterializationList {

  @Mock private SqlConverter converter;

  @Mock private UserSession session;

  @Mock private MaterializationDescriptorProvider provider;

  @Mock private DremioMaterialization relOptMat1;

  @Mock private DremioMaterialization relOptMat2;

  @Mock private MaterializationDescriptor desc1;

  @Mock private MaterializationDescriptor desc2;

  @Mock private RelNode node1;

  @Mock private RelNode node2;

  @Mock private ExternalMaterializationDescriptor externalMaterializationDescriptor1;

  @Mock private ExternalMaterializationDescriptor externalMaterializationDescriptor2;

  @Mock private FunctionContext functionContext;

  @Mock private OptionResolver optionResolver;

  @Before
  public void setup() {
    when(desc1.getMaterializationFor(converter)).thenReturn(relOptMat1);
    when(desc1.getLayoutId()).thenReturn("rid-1");
    when(desc1.getLayoutInfo())
        .thenReturn(Mockito.mock(MaterializationDescriptor.ReflectionInfo.class));
    when(desc1.getMaterializationId()).thenReturn("m-1");
    when(desc1.getPath()).thenReturn(ImmutableList.of());
    when(relOptMat1.getReflectionId()).thenReturn("rid-1");

    when(desc2.getMaterializationFor(converter)).thenReturn(relOptMat2);
    when(desc2.getLayoutId()).thenReturn("rid-2");
    when(desc2.getLayoutInfo())
        .thenReturn(Mockito.mock(MaterializationDescriptor.ReflectionInfo.class));
    when(desc2.getMaterializationId()).thenReturn("m-2");
    when(desc2.getPath()).thenReturn(ImmutableList.of());
    when(relOptMat2.getReflectionId()).thenReturn("rid-2");

    when(externalMaterializationDescriptor1.isApplicable(any(), any(), any())).thenCallRealMethod();
    when(externalMaterializationDescriptor2.isApplicable(any(), any(), any())).thenCallRealMethod();

    when(converter.getFunctionContext()).thenReturn(functionContext);
    when(functionContext.getOptions()).thenReturn(optionResolver);
    when(optionResolver.getOption(PlannerSettings.EXCLUDE_REFLECTIONS)).thenReturn("");
    when(optionResolver.getOption(PlannerSettings.CONSIDER_REFLECTIONS)).thenReturn("");
  }

  /** Verifies that reflections can be excluded through session level reflection hints */
  @Test
  public void testListDiscardsGivenHintExclusions() {
    when(desc1.isApplicable(any(), any(), any())).thenReturn(true);
    when(desc2.isApplicable(any(), any(), any())).thenReturn(true);
    RelNode relOptMat2QueryNode = createTableScan(Arrays.asList("schema", "t2"));
    when(relOptMat2.getQueryRel()).thenReturn(relOptMat2QueryNode);
    when(relOptMat2.accept(any(RelShuttle.class))).thenReturn(relOptMat2);

    SubstitutionSettings materializationSettings =
        new SubstitutionSettings(ImmutableList.of("rid-1"));

    when(session.getSubstitutionSettings()).thenReturn(materializationSettings);
    when(provider.get()).thenReturn(ImmutableList.of(desc1, desc2));

    when(optionResolver.getOption(PlannerSettings.EXCLUDE_REFLECTIONS)).thenReturn("rid-2");

    final MaterializationList materializations =
        new MaterializationList(converter, session, provider);
    RelNode userQuery = createTableScan(Arrays.asList("schema", "t2"));
    List<DremioMaterialization> dremioMaterializations =
        materializations.buildConsideredMaterializations(userQuery);

    verify(desc1, never()).getMaterializationFor(any(SqlConverter.class));
    verify(desc2, never()).getMaterializationFor(converter);
    assertEquals(0, dremioMaterializations.size());
  }

  /** Verifies that reflections can be included through session level reflection hints */
  @Test
  public void testListKeepsGivenHintIncludes() {
    when(desc1.isApplicable(any(), any(), any())).thenReturn(true);
    when(desc2.isApplicable(any(), any(), any())).thenReturn(true);
    RelNode relOptMat2QueryNode = createTableScan(Arrays.asList("schema", "t2"));
    when(relOptMat2.getQueryRel()).thenReturn(relOptMat2QueryNode);
    when(relOptMat2.accept(any(RelShuttle.class))).thenReturn(relOptMat2);

    SubstitutionSettings materializationSettings = new SubstitutionSettings(ImmutableList.of());

    when(session.getSubstitutionSettings()).thenReturn(materializationSettings);
    when(provider.get()).thenReturn(ImmutableList.of(desc1, desc2));

    when(optionResolver.getOption(PlannerSettings.CONSIDER_REFLECTIONS)).thenReturn("rid-2");

    final MaterializationList materializations =
        new MaterializationList(converter, session, provider);
    RelNode userQuery = createTableScan(Arrays.asList("schema", "t2"));
    List<DremioMaterialization> dremioMaterializations =
        materializations.buildConsideredMaterializations(userQuery);

    verify(desc1, never()).getMaterializationFor(any(SqlConverter.class));
    verify(desc2, atLeastOnce()).getMaterializationFor(converter);
    assertEquals(1, dremioMaterializations.size());
    assertEquals(relOptMat2, dremioMaterializations.stream().findFirst().get());
  }

  /**
   * Verifies that reflection excluded in substitution settings is not returned in materialization
   * list
   */
  @Test
  public void testListDiscardsGivenExclusions() {
    when(desc1.isApplicable(any(), any(), any())).thenReturn(true);
    when(desc2.isApplicable(any(), any(), any())).thenReturn(true);
    RelNode relOptMat2QueryNode = createTableScan(Arrays.asList("schema", "t2"));
    when(relOptMat2.getQueryRel()).thenReturn(relOptMat2QueryNode);
    when(relOptMat2.accept(any(RelShuttle.class))).thenReturn(relOptMat2);

    SubstitutionSettings materializationSettings =
        new SubstitutionSettings(ImmutableList.of("rid-1"));

    when(session.getSubstitutionSettings()).thenReturn(materializationSettings);
    when(provider.get()).thenReturn(ImmutableList.of(desc1, desc2));

    final MaterializationList materializations =
        new MaterializationList(converter, session, provider);
    RelNode userQuery = createTableScan(Arrays.asList("schema", "t2"));
    List<DremioMaterialization> dremioMaterializations =
        materializations.buildConsideredMaterializations(userQuery);

    verify(desc1, never()).getMaterializationFor(any(SqlConverter.class));
    verify(desc2, atLeastOnce()).getMaterializationFor(converter);
    assertEquals(1, dremioMaterializations.size());
    assertEquals(relOptMat2, dremioMaterializations.stream().findFirst().get());
  }

  /** Verifies materialization list only returns overlapping tables */
  @Test
  public void testQueryTableUsed() {

    CachedMaterializationDescriptor cachedDesc1 =
        new CachedMaterializationDescriptor(desc1, relOptMat1, Mockito.mock(CatalogService.class));
    RelNode relOptMat1QueryNode = createTableScan(Arrays.asList("schema", "t1"));
    when(relOptMat1.getQueryRel()).thenReturn(relOptMat1QueryNode);
    when(relOptMat1.accept(any(RelShuttle.class))).thenReturn(relOptMat1);

    CachedMaterializationDescriptor cachedDesc2 =
        new CachedMaterializationDescriptor(desc2, relOptMat2, Mockito.mock(CatalogService.class));
    RelNode relOptMat2QueryNode = createTableScan(Arrays.asList("schema", "t2"));
    when(relOptMat2.getQueryRel()).thenReturn(relOptMat2QueryNode);
    when(relOptMat2.accept(any(RelShuttle.class))).thenReturn(relOptMat2);

    SubstitutionSettings materializationSettings = SubstitutionSettings.of();
    when(session.getSubstitutionSettings()).thenReturn(materializationSettings);
    when(provider.get()).thenReturn(ImmutableList.of(cachedDesc1, cachedDesc2));

    final MaterializationList materializations =
        new MaterializationList(converter, session, provider);
    RelNode userQuery = createTableScan(Arrays.asList("schema", "t1"));
    List<DremioMaterialization> dremioMaterializations =
        materializations.buildConsideredMaterializations(userQuery);

    assertEquals(1, dremioMaterializations.size());
    assertEquals("rid-1", dremioMaterializations.stream().findFirst().get().getReflectionId());
  }

  private RelNode createTableScan(List<String> path) {
    RelNode node = Mockito.mock(TableScan.class);
    RelOptTable table = Mockito.mock(RelOptTable.class);
    when(node.getTable()).thenReturn(table);
    when(table.getQualifiedName()).thenReturn(path);
    when(node.accept(any(RelShuttle.class))).thenCallRealMethod();
    return node;
  }

  /** Verifies materialization list only returns overlapping VDS */
  @Test
  public void testQueryVdsUsed() {

    CachedMaterializationDescriptor cachedDesc1 =
        new CachedMaterializationDescriptor(desc1, relOptMat1, Mockito.mock(CatalogService.class));
    RelNode relOptMat1QueryNode = createExpansionNode(Arrays.asList("schema", "v1"));
    when(relOptMat1.getQueryRel()).thenReturn(relOptMat1QueryNode);
    when(relOptMat1.accept(any(RelShuttle.class))).thenReturn(relOptMat1);

    CachedMaterializationDescriptor cachedDesc2 =
        new CachedMaterializationDescriptor(desc2, relOptMat2, Mockito.mock(CatalogService.class));
    RelNode relOptMat2QueryNode = createExpansionNode(Arrays.asList("schema", "v2"));
    when(relOptMat2.getQueryRel()).thenReturn(relOptMat2QueryNode);
    when(relOptMat2.accept(any(RelShuttle.class))).thenReturn(relOptMat2);

    SubstitutionSettings materializationSettings = SubstitutionSettings.of();
    when(session.getSubstitutionSettings()).thenReturn(materializationSettings);
    when(provider.get()).thenReturn(ImmutableList.of(cachedDesc1, cachedDesc2));

    final MaterializationList materializations =
        new MaterializationList(converter, session, provider);
    RelNode userQuery = createExpansionNode(Arrays.asList("schema", "v1"));
    List<DremioMaterialization> dremioMaterializations =
        materializations.buildConsideredMaterializations(userQuery);

    assertEquals(1, dremioMaterializations.size());
    assertEquals("rid-1", dremioMaterializations.stream().findFirst().get().getReflectionId());
  }

  private RelNode createExpansionNode(List<String> path) {
    ExpansionNode node = Mockito.mock(ExpansionNode.class);
    when(node.getPath()).thenReturn(new NamespaceKey(path));
    when(node.accept(any(RelShuttle.class))).thenCallRealMethod();
    return node;
  }

  /** Verifies materialization list only returns overlapping external query */
  @Test
  public void testExternalQuery() {

    CachedMaterializationDescriptor cachedDesc1 =
        new CachedMaterializationDescriptor(desc1, relOptMat1, Mockito.mock(CatalogService.class));
    RelNode relOptMat1QueryNode = createExternalQueryScanCrel("plugin", "select 1");
    when(relOptMat1.getQueryRel()).thenReturn(relOptMat1QueryNode);
    when(relOptMat1.accept(any(RelShuttle.class))).thenReturn(relOptMat1);

    CachedMaterializationDescriptor cachedDesc2 =
        new CachedMaterializationDescriptor(desc2, relOptMat2, Mockito.mock(CatalogService.class));
    RelNode relOptMat2QueryNode = createExternalQueryScanCrel("plugin", "select 2");
    when(relOptMat2.getQueryRel()).thenReturn(relOptMat2QueryNode);
    when(relOptMat2.accept(any(RelShuttle.class))).thenReturn(relOptMat2);

    SubstitutionSettings materializationSettings = SubstitutionSettings.of();
    when(session.getSubstitutionSettings()).thenReturn(materializationSettings);
    when(provider.get()).thenReturn(ImmutableList.of(cachedDesc1, cachedDesc2));

    final MaterializationList materializations =
        new MaterializationList(converter, session, provider);
    RelNode userQuery = createExternalQueryScanCrel("plugin", "select 1");
    List<DremioMaterialization> dremioMaterializations =
        materializations.buildConsideredMaterializations(userQuery);

    assertEquals(1, dremioMaterializations.size());
    assertEquals("rid-1", dremioMaterializations.stream().findFirst().get().getReflectionId());
  }

  private RelNode createExternalQueryScanCrel(String pluginName, String sql) {
    ExternalQueryScanCrel node = Mockito.mock(ExternalQueryScanCrel.class);
    StoragePluginId pluginId = Mockito.mock(StoragePluginId.class);
    when(pluginId.getName()).thenReturn(pluginName);
    when(node.getPluginId()).thenReturn(pluginId);
    when(node.getSql()).thenReturn(sql);
    when(node.accept(any(RelShuttle.class))).thenCallRealMethod();
    return node;
  }

  /** Verifies external reflection without materialization cache is pruned */
  @Test
  public void testExternalReflection() {

    when(externalMaterializationDescriptor1.getMaterializationFor(any())).thenReturn(relOptMat1);
    RelNode relOptMat1QueryNode = createTableScan(Arrays.asList("schema", "t1"));
    when(relOptMat1.getQueryRel()).thenReturn(relOptMat1QueryNode);
    when(relOptMat1.accept(any(RelShuttle.class))).thenReturn(relOptMat1);

    when(externalMaterializationDescriptor2.getMaterializationFor(any())).thenReturn(relOptMat2);
    RelNode relOptMat2QueryNode = createTableScan(Arrays.asList("schema", "t2"));
    when(relOptMat2.getQueryRel()).thenReturn(relOptMat2QueryNode);
    when(relOptMat2.accept(any(RelShuttle.class))).thenReturn(relOptMat2);

    SubstitutionSettings materializationSettings = SubstitutionSettings.of();
    when(session.getSubstitutionSettings()).thenReturn(materializationSettings);
    when(provider.get())
        .thenReturn(
            ImmutableList.of(
                externalMaterializationDescriptor1, externalMaterializationDescriptor2));

    final MaterializationList materializations =
        new MaterializationList(converter, session, provider);
    RelNode userQuery = createTableScan(Arrays.asList("schema", "t1"));
    List<DremioMaterialization> dremioMaterializations =
        materializations.buildConsideredMaterializations(userQuery);

    assertEquals(1, dremioMaterializations.size());
    assertEquals("rid-1", dremioMaterializations.stream().findFirst().get().getReflectionId());
  }
}
