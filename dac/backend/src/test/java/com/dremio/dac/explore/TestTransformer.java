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
package com.dremio.dac.explore;

import static com.dremio.dac.proto.model.dataset.ActionForNonMatchingValue.DELETE_RECORDS;
import static com.dremio.dac.proto.model.dataset.ActionForNonMatchingValue.REPLACE_WITH_DEFAULT;
import static com.dremio.dac.proto.model.dataset.ActionForNonMatchingValue.REPLACE_WITH_NULL;
import static com.dremio.dac.proto.model.dataset.DataType.DATETIME;
import static com.dremio.dac.proto.model.dataset.DataType.FLOAT;
import static com.dremio.dac.proto.model.dataset.ExpressionType.CalculatedField;
import static com.dremio.dac.proto.model.dataset.ExpressionType.ColumnReference;
import static com.dremio.dac.proto.model.dataset.ExpressionType.FieldTransformation;
import static com.dremio.dac.proto.model.dataset.FieldTransformationType.ConvertFromJSON;
import static com.dremio.dac.proto.model.dataset.FieldTransformationType.ConvertNumberToDate;
import static com.dremio.dac.proto.model.dataset.FieldTransformationType.ConvertToTypeIfPossible;
import static com.dremio.dac.proto.model.dataset.FieldTransformationType.UnnestList;
import static com.dremio.dac.proto.model.dataset.FilterType.ConvertibleData;
import static com.dremio.dac.proto.model.dataset.FilterType.Custom;
import static com.dremio.dac.proto.model.dataset.MeasureType.Count;
import static com.dremio.dac.proto.model.dataset.MeasureType.Count_Star;
import static com.dremio.dac.proto.model.dataset.NumberToDateFormat.EXCEL;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.SecurityContext;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.FieldTransformationBase;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.ConvertCase;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.Dimension;
import com.dremio.dac.proto.model.dataset.Direction;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.ExpFieldTransformation;
import com.dremio.dac.proto.model.dataset.ExpMeasure;
import com.dremio.dac.proto.model.dataset.Expression;
import com.dremio.dac.proto.model.dataset.ExtractListRule;
import com.dremio.dac.proto.model.dataset.ExtractListRuleType;
import com.dremio.dac.proto.model.dataset.ExtractRule;
import com.dremio.dac.proto.model.dataset.ExtractRulePosition;
import com.dremio.dac.proto.model.dataset.ExtractRuleSingle;
import com.dremio.dac.proto.model.dataset.ExtractRuleType;
import com.dremio.dac.proto.model.dataset.FieldConvertFromJSON;
import com.dremio.dac.proto.model.dataset.FieldConvertNumberToDate;
import com.dremio.dac.proto.model.dataset.FieldConvertToTypeIfPossible;
import com.dremio.dac.proto.model.dataset.FieldExtractList;
import com.dremio.dac.proto.model.dataset.FieldReplacePattern;
import com.dremio.dac.proto.model.dataset.FieldTransformation;
import com.dremio.dac.proto.model.dataset.FieldTransformationType;
import com.dremio.dac.proto.model.dataset.FieldUnnestList;
import com.dremio.dac.proto.model.dataset.Filter;
import com.dremio.dac.proto.model.dataset.FilterConvertibleData;
import com.dremio.dac.proto.model.dataset.FilterCustom;
import com.dremio.dac.proto.model.dataset.FilterDefinition;
import com.dremio.dac.proto.model.dataset.FilterPattern;
import com.dremio.dac.proto.model.dataset.FilterRange;
import com.dremio.dac.proto.model.dataset.FilterType;
import com.dremio.dac.proto.model.dataset.FilterValue;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.FromType;
import com.dremio.dac.proto.model.dataset.Join;
import com.dremio.dac.proto.model.dataset.JoinCondition;
import com.dremio.dac.proto.model.dataset.JoinType;
import com.dremio.dac.proto.model.dataset.Measure;
import com.dremio.dac.proto.model.dataset.Offset;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.OrderDirection;
import com.dremio.dac.proto.model.dataset.ReplacePatternRule;
import com.dremio.dac.proto.model.dataset.ReplaceSelectionType;
import com.dremio.dac.proto.model.dataset.ReplaceType;
import com.dremio.dac.proto.model.dataset.Transform;
import com.dremio.dac.proto.model.dataset.TransformAddCalculatedField;
import com.dremio.dac.proto.model.dataset.TransformConvertCase;
import com.dremio.dac.proto.model.dataset.TransformConvertToSingleType;
import com.dremio.dac.proto.model.dataset.TransformCreateFromParent;
import com.dremio.dac.proto.model.dataset.TransformDrop;
import com.dremio.dac.proto.model.dataset.TransformExtract;
import com.dremio.dac.proto.model.dataset.TransformField;
import com.dremio.dac.proto.model.dataset.TransformFilter;
import com.dremio.dac.proto.model.dataset.TransformGroupBy;
import com.dremio.dac.proto.model.dataset.TransformJoin;
import com.dremio.dac.proto.model.dataset.TransformRename;
import com.dremio.dac.proto.model.dataset.TransformSort;
import com.dremio.dac.proto.model.dataset.TransformSorts;
import com.dremio.dac.proto.model.dataset.TransformSplitByDataType;
import com.dremio.dac.proto.model.dataset.TransformTrim;
import com.dremio.dac.proto.model.dataset.TransformType;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.proto.model.dataset.TrimType;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.dac.util.JSONUtil;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.jobs.metadata.QuerySemantics;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

/**
 * Transformer tests
 */
public class TestTransformer extends BaseTestServer { // needed for parsing queries

  private final DatasetPath datasetPath = new DatasetPath("myspace.parentDS");
  private final From parentDataset = new FromTable(datasetPath.toPathString()).wrap();

  private VirtualDatasetState state;

  private TransformResult transform(TransformBase tb) {
    return transform(tb, false);
  }

  private TransformResult transform(TransformBase tb, Boolean preview) {
    QueryExecutor executor = new QueryExecutor(null, null, null){
      @Override
      public List<String> getColumnList(String username, DatasetPath path) {
        return asList("bar", "baz");
      }
    };

    TransformActor actor = new TransformActor(state, preview, "test_user", executor){
      @Override
      protected com.dremio.service.jobs.metadata.proto.QueryMetadata getMetadata(SqlQuery query) {
        return Mockito.mock(com.dremio.service.jobs.metadata.proto.QueryMetadata.class);
      }

      @Override
      protected boolean hasMetadata() {
        return true;
      }

      @Override
      protected com.dremio.service.jobs.metadata.proto.QueryMetadata getMetadata() {
        return Mockito.mock(com.dremio.service.jobs.metadata.proto.QueryMetadata.class);
      }

      @Override
      protected Optional<BatchSchema> getBatchSchema() {
        return Optional.of(Mockito.mock(BatchSchema.class));
      }

      @Override
      protected Optional<List<ParentDatasetInfo>> getParents() {
        throw new IllegalStateException("not implemented");
      }

      @Override
      protected Optional<List<FieldOrigin>> getFieldOrigins() {
        throw new IllegalStateException("not implemented");
      }

      @Override
      protected Optional<List<ParentDataset>> getGrandParents() {
        throw new IllegalStateException("not implemented");
      }
    };

    return tb.accept(actor);
  }

  @Before
  public void before() {
    state = new VirtualDatasetState()
        .setFrom(parentDataset);
    state.setColumnsList(asList(new Column("foo", new ExpColumnReference("foo").wrap()), new Column("bar", new ExpColumnReference("bar").wrap())));
    state.setReferredTablesList(ImmutableList.of("parentDS"));
  }

  @Test
  public void testRenameAfterSort() {
    Expression value = new ExpColumnReference("parentFoo").wrap();
    state.setColumnsList(asList(new Column("foo", value), new Column("bar", value)));

    TransformResult result1 = transform(new TransformSort("foo", OrderDirection.ASC));

    state = result1.getNewState();
    assertEquals(state.getOrdersList().toString(), 1, state.getOrdersList().size());
    assertEquals(state.getOrdersList().toString(), new Order("foo", OrderDirection.ASC), state.getOrdersList().get(0));

    TransformResult result2 = transform(new TransformRename("foo", "foo2"));

    // rename after sort should not nest and just rename the order by
    VirtualDatasetState newState = result2.getNewState();

    assertEquals(2, newState.getColumnsList().size());
    assertEquals("foo2", newState.getColumnsList().get(0).getName());
    assertEquals("bar", newState.getColumnsList().get(1).getName());
    assertEquals(newState.getOrdersList().toString(), 1, newState.getOrdersList().size());
    assertEquals(new Order("foo2", OrderDirection.ASC), newState.getOrdersList().get(0));
  }

  @Test
  public void testDropAfterSort() {
    Expression value = new ExpColumnReference("parentFoo").wrap();
    state.setColumnsList(asList(new Column("foo", value), new Column("bar", value)));

    TransformResult result1 = transform(new TransformSort("foo", OrderDirection.ASC));

    state = result1.getNewState();
    assertEquals(state.getOrdersList().toString(), 1, state.getOrdersList().size());
    assertEquals(state.getOrdersList().toString(), new Order("foo", OrderDirection.ASC), state.getOrdersList().get(0));

    TransformResult result2 = transform(new TransformDrop("foo"));

    VirtualDatasetState newState = result2.getNewState();

    // drop after sort should nest and keep the order by
    assertEquals(1, newState.getColumnsList().size());
    assertEquals("bar", newState.getColumnsList().get(0).getName());
    assertNull(newState.getOrdersList());
    assertNotNull(newState.getFrom().toString(), newState.getFrom().getSubQuery());
    VirtualDatasetState suqQuery = newState.getFrom().getSubQuery().getSuqQuery();
    assertEquals(suqQuery.getOrdersList().toString(), 1, suqQuery.getOrdersList().size());
    assertEquals(new Order("foo", OrderDirection.ASC), suqQuery.getOrdersList().get(0));
  }

  @Test
  public void testDrop() {

    Expression value = new ExpColumnReference("parentFoo").wrap();
    state.setColumnsList(asList(new Column("foo", value), new Column("bar", value)));

    TransformResult result = transform(new TransformDrop("foo"));
    VirtualDatasetState newState = result.getNewState();

    assertEquals(1, newState.getColumnsList().size());
    assertEquals("bar", newState.getColumnsList().get(0).getName());
    assertEquals("foo", result.getRemovedColumns().iterator().next());
  }

  @Test
  public void testTitleCase() {
    Expression column = new ExpColumnReference("parentFoo").wrap();

    state.setColumnsList(asList(new Column("foo", column)));

    TransformResult result = transform(new TransformConvertCase("foo", ConvertCase.TITLE_CASE, "bar", true));
    VirtualDatasetState newState = result.getNewState();
    assertEquals("bar", newState.getColumnsList().get(0).getName());
  }

  @Test
  public void testExcludeText() {
    Expression column = new ExpColumnReference("parentFoo").wrap();

    state.setColumnsList(asList(new Column("foo", column)));

    ReplacePatternRule replacePatternRule = new ReplacePatternRule(ReplaceSelectionType.CONTAINS)
      .setIgnoreCase(false)
      .setSelectionPattern("bar");

    TransformResult result = transform(new TransformFilter("foo", new FilterDefinition(FilterType.Pattern)
        .setPattern(new FilterPattern(
          replacePatternRule
        ))).setExclude(true).setKeepNull(true));

    VirtualDatasetState newState = result.getNewState();
    assertEquals("foo", newState.getColumnsList().get(0).getName());
    assertEquals(1, newState.getFiltersList().size());
    assertEquals(0, newState.getFiltersList().get(0).getFieldNumber("foo"));
    assertEquals(FilterType.Pattern, newState.getFiltersList().get(0).getFilterDef().getType());
    assertEquals(replacePatternRule, newState.getFiltersList().get(0).getFilterDef().getPattern().getRule());
  }

  @Test
  public void testExcludeDate() {
    Expression column = new ExpColumnReference("parentFoo").wrap();

    state.setColumnsList(asList(new Column("foo", column)));

    FilterDefinition filterDefinition =
      new FilterDefinition(FilterType.Value)
        .setValue(new FilterValue(DataType.DATE).setValuesList(asList("foo", "bar")));
    TransformResult result = transform(new TransformFilter(
      "foo", filterDefinition).setExclude(false).setKeepNull(false));

    VirtualDatasetState newState = result.getNewState();
    assertEquals("foo", newState.getColumnsList().get(0).getName());
    assertEquals(1, newState.getFiltersList().size());
    assertEquals(0, newState.getFiltersList().get(0).getFieldNumber("foo"));
    assertEquals(filterDefinition, newState.getFiltersList().get(0).getFilterDef());
  }

  @Test
  public void testGroupBy() {
    Expression a = new ExpColumnReference("a").wrap();
    Expression b = new ExpColumnReference("b").wrap();

    state.setColumnsList(asList(new Column("a", a), new Column("b", b)));

    TransformResult result = transform(new TransformGroupBy()
        .setColumnsDimensionsList(asList(new Dimension("a")))
        .setColumnsMeasuresList(asList(new Measure(Count).setColumn("b"))));
    VirtualDatasetState newState = result.getNewState();
    assertEquals(asList(new Column("a", a), new Column("Count_b", new ExpMeasure(Count).setOperand(b).wrap())), newState.getColumnsList());
    assertEquals(asList(new Column("a", a)), newState.getGroupBysList());
  }

  @Test
  public void testJoin() {
    JoinCondition jc = new JoinCondition("bar", "bar");
    List<String> fullPathList = asList("space", "foo");
    TransformResult result;

    TransformJoin transformJoin = new TransformJoin(JoinType.Inner);
    result = transform(transformJoin.setRightTableFullPathList(fullPathList).setJoinConditionsList(asList(jc)));
    assertEquals(asList(new Join(JoinType.Inner, "space.foo", "join_foo")
        .setJoinConditionsList(asList(jc))), result.getNewState().getJoinsList());
    assertEquals(result.getNewState().getColumnsList().size(), 4);

    transformJoin = new TransformJoin(JoinType.LeftOuter);
    result = transform(transformJoin.setRightTableFullPathList(fullPathList).setJoinConditionsList(asList(jc)));
    assertEquals(asList(new Join(JoinType.LeftOuter, "space.foo", "join_foo")
        .setJoinConditionsList(asList(jc))), result.getNewState().getJoinsList());
    assertEquals(result.getNewState().getColumnsList().size(), 4);

    transformJoin = new TransformJoin(JoinType.RightOuter);
    result = transform(transformJoin.setRightTableFullPathList(fullPathList).setJoinConditionsList(asList(jc)));
    assertEquals(asList(new Join(JoinType.RightOuter, "space.foo", "join_foo")
        .setJoinConditionsList(asList(jc))), result.getNewState().getJoinsList());
    assertEquals(result.getNewState().getColumnsList().size(), 4);

    transformJoin = new TransformJoin(JoinType.FullOuter);
    result = transform(transformJoin.setRightTableFullPathList(fullPathList).setJoinConditionsList(asList(jc)));
    assertEquals(asList(new Join(JoinType.FullOuter, "space.foo", "join_foo")
        .setJoinConditionsList(asList(jc))), result.getNewState().getJoinsList());
    assertEquals(result.getNewState().getColumnsList().size(), 4);
  }

  @Test
  public void testJoinPreview() {
    JoinCondition jc = new JoinCondition("bar", "bar");
    List<String> fullPathList = asList("space", "foo");

    TransformResult result;

    result = transform(new TransformJoin(JoinType.Inner).setRightTableFullPathList(fullPathList).setJoinConditionsList(asList(jc)), true);
    assertEquals(asList("bar", "bar0"), result.getRowDeletionMarkerColumns());
    assertEquals(asList(new Join(JoinType.FullOuter, "space.foo", "join_foo")
        .setJoinConditionsList(asList(jc))), result.getNewState().getJoinsList());

    result = transform(new TransformJoin(JoinType.LeftOuter).setRightTableFullPathList(fullPathList).setJoinConditionsList(asList(jc)), true);
    assertEquals(asList("bar"), result.getRowDeletionMarkerColumns());
    assertEquals(asList(new Join(JoinType.FullOuter, "space.foo", "join_foo")
        .setJoinConditionsList(asList(jc))), result.getNewState().getJoinsList());

    result = transform(new TransformJoin(JoinType.RightOuter).setRightTableFullPathList(fullPathList).setJoinConditionsList(asList(jc)), true);
    assertEquals(asList("bar0"), result.getRowDeletionMarkerColumns());
    assertEquals(asList(new Join(JoinType.FullOuter, "space.foo", "join_foo")
        .setJoinConditionsList(asList(jc))), result.getNewState().getJoinsList());

    result = transform(new TransformJoin(JoinType.FullOuter).setRightTableFullPathList(fullPathList).setJoinConditionsList(asList(jc)), true);
    assertEquals(asList(), result.getRowDeletionMarkerColumns());
    assertEquals(asList(new Join(JoinType.FullOuter, "space.foo", "join_foo")
        .setJoinConditionsList(asList(jc))), result.getNewState().getJoinsList());
  }

  @Test
  public void testJoinColumnOrder() {
    List<String> fullPathList = asList("space", "foo");
    TransformResult result = transform(new TransformJoin(JoinType.Inner).setRightTableFullPathList(fullPathList).setJoinConditionsList(
        asList(new JoinCondition("foo", "baz"), new JoinCondition("bar", "bar"))));
    List<String> columns = new ArrayList<>();
    for (Column c : result.getNewState().getColumnsList()) {
      columns.add(c.getName());
    }
    assertEquals(columns, asList("foo", "bar", "bar0", "baz"));
  }

  @Test
  public void testGroupByTwice() {
    Expression a = new ExpColumnReference("a").wrap();
    Expression b = new ExpColumnReference("b").wrap();

    state.setColumnsList(asList(new Column("a", a), new Column("b", b)));

    TransformResult result = transform(new TransformGroupBy()
        .setColumnsDimensionsList(asList(new Dimension("a")))
        .setColumnsMeasuresList(asList(new Measure(Count).setColumn("b"))));
    state = result.getNewState();

    TransformResult result2 = transform(new TransformGroupBy()
        .setColumnsDimensionsList(asList(new Dimension("Count_b")))
        .setColumnsMeasuresList(asList(new Measure(Count).setColumn("a"))));

    VirtualDatasetState newState = result2.getNewState();
    System.out.println(JSONUtil.toString(newState));

    // nested query should match the intermediary state
    VirtualDatasetState intermediaryState = newState.getFrom().getSubQuery().getSuqQuery();
    assertEquals(asList(new Column("a", a), new Column("Count_b", new ExpMeasure(Count).setOperand(b).wrap())), intermediaryState.getColumnsList());
    assertEquals(asList(new Column("a", a)), intermediaryState.getGroupBysList());

    assertEquals(JSONUtil.toString(newState.getColumnsList()), asList(new Column("Count_b", new ExpColumnReference("Count_b").wrap()), new Column("Count_a", new ExpMeasure(Count).setOperand(a).wrap())), newState.getColumnsList());
    assertEquals(JSONUtil.toString(newState.getGroupBysList()), asList(new Column("Count_b", new ExpColumnReference("Count_b").wrap())), newState.getGroupBysList());

  }

  @Test
  public void testGroupByCountStar() {
    Expression a = new ExpColumnReference("a").wrap();
    Expression b = new ExpColumnReference("b").wrap();

    state.setColumnsList(asList(new Column("a", a), new Column("b", b)));

    TransformResult result = transform(new TransformGroupBy()
        .setColumnsDimensionsList(asList(new Dimension("a")))
        .setColumnsMeasuresList(asList(new Measure(Count_Star))));
    VirtualDatasetState newState = result.getNewState();
    assertEquals(asList(new Column("a", a), new Column("Count_Star", new ExpMeasure(Count_Star).wrap())), newState.getColumnsList());
    assertEquals(asList(new Column("a", a)), newState.getGroupBysList());
  }

  @Test
  public void testGroupByCountStarAndSort() {
    Expression a = new ExpColumnReference("a").wrap();
    Expression b = new ExpColumnReference("b").wrap();

    state.setColumnsList(asList(new Column("a", a), new Column("b", b)));

    TransformResult result = transform(new TransformGroupBy()
      .setColumnsDimensionsList(asList(new Dimension("a")))
      .setColumnsMeasuresList(asList(new Measure(Count_Star))));

    // Sort Count_Star
    state = result.getNewState();
    result = transform(new TransformSort("Count_Star", OrderDirection.ASC));

    VirtualDatasetState newState = result.getNewState();
    assertEquals(newState.getOrdersList().size() == 1, true);
    assertEquals(newState.getOrdersList().get(0),  new Order("Count_Star", OrderDirection.ASC));
  }

  @Test
  public void testConvertIfPossible() {
    Expression a = new ExpColumnReference("a").wrap();

    state.setColumnsList(asList(new Column("a", a)));

    FieldTransformation ft = new FieldConvertToTypeIfPossible(FLOAT, REPLACE_WITH_NULL).wrap();
    TransformResult result = transform(new TransformField("a", "a", true, ft));

    VirtualDatasetState newState = result.getNewState();

    assertEquals(1, newState.getColumnsList().size());
    Column column = newState.getColumnsList().get(0);
    assertEquals("a", column.getName());
    assertEquals(FieldTransformation, column.getValue().getType());
    ExpFieldTransformation fieldTransformation = column.getValue().getFieldTransformation();
    assertEquals(a.toString(), fieldTransformation.getOperand().toString());
    assertEquals(ConvertToTypeIfPossible, fieldTransformation.getTransformation().getType());
    assertEquals(ft, fieldTransformation.getTransformation());

    List<Filter> filtersList = newState.getFiltersList();
    assertTrue((filtersList == null) || (filtersList.size() == 0));
  }

  @Test
  public void testFilterCustomCondition() {
    Expression a = new ExpColumnReference("a").wrap();

    state.setColumnsList(asList(new Column("a", a)));

    TransformResult result = transform(new TransformFilter("a", new FilterDefinition(FilterType.Custom)
      .setCustom(new FilterCustom("a > 5"))));

    VirtualDatasetState newState = result.getNewState();

    assertEquals(1, newState.getColumnsList().size());
    Column col = newState.getColumnsList().get(0);
    assertEquals("a", col.getName());
    assertEquals(1, newState.getFiltersList().size());
    Filter filter = newState.getFiltersList().get(0);
    assertEquals(Custom, filter.getFilterDef().getType());
    assertEquals("a > 5", filter.getFilterDef().getCustom().getExpression());
    assertEquals(FromType.SubQuery, newState.getFrom().getType());
    assertEquals(datasetPath.toUnescapedString(), newState.getFrom().getSubQuery().getSuqQuery().getFrom().getTable().getDatasetPath());
  }

  @Test
  public void testCalculatedField() {
    Expression a = new ExpColumnReference("a").wrap();

    state.setColumnsList(asList(new Column("a", a)));

    TransformResult result = transform(new TransformAddCalculatedField("a", "a", "a+5", true));

    VirtualDatasetState newState = result.getNewState();

    assertEquals(1, newState.getColumnsList().size());
    Column col = newState.getColumnsList().get(0);
    assertEquals("a", col.getName());
    assertEquals(CalculatedField, col.getValue().getType());
    assertEquals("a+5", col.getValue().getCalculatedField().getExp());
    assertEquals(FromType.SubQuery, newState.getFrom().getType());
    assertEquals(datasetPath.toUnescapedString(), newState.getFrom().getSubQuery().getSuqQuery().getFrom().getTable().getDatasetPath());
  }

  @Test
  public void testRenameThenCalculatedField() {
    Expression b = new ExpColumnReference("b").wrap();
    state.setColumnsList(asList(new Column("b", b)));
    TransformResult result2 = transform(new TransformRename("b", "a"));

    state = result2.getNewState();

    TransformResult result = transform(new TransformAddCalculatedField("a", "a", "a+5", true));
    VirtualDatasetState newState = result.getNewState();

    // outside of the expected subquery there should be one expression in the form "a+5 as a"
    assertEquals(1, newState.getColumnsList().size());
    Column col = newState.getColumnsList().get(0);
    assertEquals("a", col.getName());
    assertEquals(CalculatedField, col.getValue().getType());
    assertEquals("a+5", col.getValue().getCalculatedField().getExp());

    // because of the calculated field, which is an opaque string from the user, we expect nesting until we
    // add parsing this string to find what kinds of columns are referenced by it (expressions or renames will need
    // special handling in this case)
    assertEquals(FromType.SubQuery, newState.getFrom().getType());
    VirtualDatasetState subQuery = newState.getFrom().getSubQuery().getSuqQuery();
    assertEquals(datasetPath.toUnescapedString(), subQuery.getFrom().getTable().getDatasetPath());
    // check in the subquery for a single expression "b as a"
    assertEquals(1, subQuery.getColumnsList().size());
    Column columnInSub = subQuery.getColumnsList().get(0);
    assertEquals("a", columnInSub.getName());
    assertEquals(ColumnReference, columnInSub.getValue().getType());
    assertEquals("b", columnInSub.getValue().getCol().getName());
  }

  @Test
  public void testUnnestThenExclude() {
    Expression b = new ExpColumnReference("b").wrap();
    state.setColumnsList(asList(new Column("b", b)));
    FieldTransformation transformation = new FieldTransformation(UnnestList).setUnnestList(new FieldUnnestList());
    TransformField transformField = new TransformField("b", "b", true, transformation);
    TransformResult result2 = transform(transformField);

    state = result2.getNewState();

    TransformResult result = transform(new TransformFilter("b", new FilterDefinition(FilterType.Range)
      .setRange(new FilterRange(DataType.INTEGER).setLowerBound("1").setUpperBound("5"))));

    VirtualDatasetState newState = result.getNewState();

    // outside of the expected subquery there should be one expression in the form "a+5 as a"
    assertEquals(1, newState.getColumnsList().size());
    assertEquals(newState.getFiltersList().size(), 1);
    Filter filter = newState.getFiltersList().get(0);
    FilterRange range = filter.getFilterDef().getRange();
    assertEquals("1", range.getLowerBound());
    assertEquals("5", range.getUpperBound());
    assertEquals(null, filter.getExclude());

    // because of the calculated field, which is an opaque string from the user, we expect nesting until we
    // add parsing this string to find what kinds of columns are referenced by it (expressions or renames will need
    // special handling in this case)
    assertEquals(FromType.SubQuery, newState.getFrom().getType());
    VirtualDatasetState subQuery = newState.getFrom().getSubQuery().getSuqQuery();
    assertEquals(datasetPath.toUnescapedString(), subQuery.getFrom().getTable().getDatasetPath());
    // check in the subquery for a single expression "b as a"
    assertEquals(1, subQuery.getColumnsList().size());
    Column columnInSub = subQuery.getColumnsList().get(0);
    assertEquals("b", columnInSub.getName());
    assertEquals(FieldTransformation, columnInSub.getValue().getType());
    assertEquals(UnnestList, columnInSub.getValue().getFieldTransformation().getTransformation().getType());
  }

  @Test
  public void testUnnestThenSort() {
    // Sort single column
    Expression a = new ExpColumnReference("a").wrap();
    state.setColumnsList(asList(new Column("a", a)));
    FieldTransformation unnestTransform = new FieldTransformation(UnnestList).setUnnestList(new FieldUnnestList());
    TransformField transformFieldA = new TransformField("a", "a2", true, unnestTransform);
    TransformResult unnestedResultA = transform(transformFieldA);

    state = unnestedResultA.getNewState();

    TransformResult result = transform(new TransformSort("a2", OrderDirection.ASC));
    VirtualDatasetState newState = result.getNewState();

    // Sorting a flattened field will result in a nested query
    assertEquals(FromType.SubQuery, newState.getFrom().getType());
    VirtualDatasetState subQuery = newState.getFrom().getSubQuery().getSuqQuery();
    assertEquals(datasetPath.toUnescapedString(), subQuery.getFrom().getTable().getDatasetPath());
    // check in the subquery for a single expression "a as a2"
    assertEquals(1, subQuery.getColumnsList().size());
    Column columnInSub = subQuery.getColumnsList().get(0);
    assertEquals("a2", columnInSub.getName());
    assertEquals(FieldTransformation, columnInSub.getValue().getType());
    assertEquals(UnnestList, columnInSub.getValue().getFieldTransformation().getTransformation().getType());

    // Sort multiple columns
    state = new VirtualDatasetState()
        .setFrom(parentDataset);
    Expression x = new ExpColumnReference("x").wrap();
    Expression y = new ExpColumnReference("y").wrap();
    state.setColumnsList(asList(new Column("x", x), new Column("y", y)));
    TransformField transformFieldX = new TransformField("x", "x2", true, unnestTransform);
    TransformResult unnestedResultX = transform(transformFieldX);
    state = unnestedResultX.getNewState();

    result = transform(new TransformSorts()
      .setColumnsList(asList(new Order("y", OrderDirection.ASC), new Order("x2", OrderDirection.DESC))));
    newState = result.getNewState();
    // Sorting by multiple fields that includes a flattened field
    assertEquals(FromType.SubQuery, newState.getFrom().getType());
    subQuery = newState.getFrom().getSubQuery().getSuqQuery();
    assertEquals(datasetPath.toUnescapedString(), subQuery.getFrom().getTable().getDatasetPath());
    // check in the subquery for the expression "x as x2, y"
    assertEquals(2, subQuery.getColumnsList().size());
    assertEquals("x2", subQuery.getColumnsList().get(0).getName());
    assertEquals("y", subQuery.getColumnsList().get(1).getName());
    assertEquals(FieldTransformation, columnInSub.getValue().getType());
    assertEquals(UnnestList, columnInSub.getValue().getFieldTransformation().getTransformation().getType());
  }

  // Verify that 'st' contains a single subquery with the name 'subName', which itself is a flatten (UnnestList)
  private void verifyNestedQuery(VirtualDatasetState st, String subName) {
    assertEquals(FromType.SubQuery, st.getFrom().getType());
    VirtualDatasetState subQuery = st.getFrom().getSubQuery().getSuqQuery();
    assertEquals(datasetPath.toUnescapedString(), subQuery.getFrom().getTable().getDatasetPath());
    // check in the subquery for a single expression "* as <subName>"
    assertEquals(1, subQuery.getColumnsList().size());
    Column columnInSub = subQuery.getColumnsList().get(0);
    assertEquals(subName, columnInSub.getName());
    assertEquals(FieldTransformation, columnInSub.getValue().getType());
    assertEquals(UnnestList, columnInSub.getValue().getFieldTransformation().getTransformation().getType());
  }

  @Test
  public void testUnnestThenConvert() {
    // Every one of the conversion transformations on a flattened column, below, results in a nested query

    // All conversions below start from the same state: "flatten(a) as a2"
    Expression a = new ExpColumnReference("a").wrap();
    state.setColumnsList(asList(new Column("a", a)));
    FieldTransformation unnestTransform = new FieldTransformation(UnnestList).setUnnestList(new FieldUnnestList());
    TransformField transformFieldA = new TransformField("a", "a2", true, unnestTransform);
    TransformResult unnestedResultA = transform(transformFieldA);

    // Rename column
    state = unnestedResultA.getNewState();
    TransformResult result = transform(new TransformRename("a2", "a3"));
    VirtualDatasetState newState = result.getNewState();
    verifyNestedQuery(newState, "a2");

    // Convert case
    state = unnestedResultA.getNewState();  // back to a single flatten(a) as a2
    result = transform(new TransformConvertCase("a2", ConvertCase.TITLE_CASE, "a4", true));
    newState = result.getNewState();
    verifyNestedQuery(newState, "a2");

    // Trim
    state = unnestedResultA.getNewState();  // back to a single flatten(a) as a2
    result = transform(new TransformTrim("a2", TrimType.LEFT, "a5", true));
    newState = result.getNewState();
    verifyNestedQuery(newState, "a2");

    // Extract
    state = unnestedResultA.getNewState();  // back to a single flatten(a) as a2
    result = transform(new TransformExtract("a2", "a6",
      new ExtractRule()
        .setType(ExtractRuleType.position)
        .setPosition(new ExtractRulePosition(new Offset(1, Direction.FROM_THE_END), new Offset(3, Direction.FROM_THE_END))),
      true));
    newState = result.getNewState();
    verifyNestedQuery(newState, "a2");

    // Add calculated field
    state = unnestedResultA.getNewState();  // back to a single flatten(a) as a2
    result = transform(new TransformAddCalculatedField("a7", "a2", "a2+7", true));
    newState = result.getNewState();
    verifyNestedQuery(newState, "a2");

    // Transform Field
    state = unnestedResultA.getNewState();  // back to a single flatten(a) as a2
    ReplacePatternRule rule =
      new ReplacePatternRule(ReplaceSelectionType.STARTS_WITH)
        .setIgnoreCase(false)
        .setSelectionPattern("foo");
    FieldReplacePattern replace =
      new FieldReplacePattern(rule, ReplaceType.SELECTION)
        .setReplacementValue("bar");
    FieldTransformation ft = replace.wrap();
    result = transform(new TransformField("a2", "a8", true, ft));
    newState = result.getNewState();
    verifyNestedQuery(newState, "a2");

    // Convert to single type
    state = unnestedResultA.getNewState();  // back to a single flatten(a) as a2
    result = transform(new TransformConvertToSingleType("a2", "a9", true, DataType.TEXT, true, REPLACE_WITH_NULL));
    newState = result.getNewState();
    verifyNestedQuery(newState, "a2");

    // Split by data type
    state = unnestedResultA.getNewState();  // back to a single flatten(a) as a2
    result = transform(new TransformSplitByDataType("a2", "a10_", false)
        .setSelectedTypesList(asList(DataType.TEXT, DataType.INTEGER)));
    newState = result.getNewState();
    verifyNestedQuery(newState, "a2");
  }

  @Test
  public void testConvertIfPossibleFilter() {
    Expression a = new ExpColumnReference("a").wrap();

    state.setColumnsList(asList(new Column("a", a)));

    FieldTransformation ft = new FieldConvertToTypeIfPossible(FLOAT,  DELETE_RECORDS).wrap();
    TransformResult result = transform(new TransformField("a", "a", true, ft));

    VirtualDatasetState newState = result.getNewState();

    assertEquals(1, newState.getColumnsList().size());
    Column column = newState.getColumnsList().get(0);
    assertEquals("a", column.getName());
    assertEquals(FieldTransformation, column.getValue().getType());
    ExpFieldTransformation fieldTransformation = column.getValue().getFieldTransformation();
    assertEquals(a.toString(), fieldTransformation.getOperand().toString());
    assertEquals(ConvertToTypeIfPossible, fieldTransformation.getTransformation().getType());
    assertEquals(ft, fieldTransformation.getTransformation());

    List<Filter> filtersList = newState.getFiltersList();
    assertEquals(1, filtersList.size());
    Filter filter = filtersList.get(0);
    assertEquals(ConvertibleData, filter.getFilterDef().getType());
    FilterConvertibleData convertible = filter.getFilterDef().getConvertible();
    assertEquals(FLOAT, convertible.getDesiredType());
    assertEquals(a.toString(), filter.getOperand().toString());
  }

  @Test
  public void testConvertIfPossibleWithDefaultValue() {
    Expression a = new ExpColumnReference("a").wrap();

    state.setColumnsList(asList(new Column("a", a)));

    FieldTransformation ft = new FieldConvertToTypeIfPossible(FLOAT, REPLACE_WITH_DEFAULT).setDefaultValue("0.00").wrap();
    TransformResult result = transform(new TransformField("a", "a", true, ft));

    VirtualDatasetState newState = result.getNewState();

    assertEquals(1, newState.getColumnsList().size());
    Column column = newState.getColumnsList().get(0);
    assertEquals("a", column.getName());
    assertEquals(FieldTransformation, column.getValue().getType());
    ExpFieldTransformation fieldTransformation = column.getValue().getFieldTransformation();
    assertEquals(a.toString(), fieldTransformation.getOperand().toString());
    assertEquals(ConvertToTypeIfPossible, fieldTransformation.getTransformation().getType());
    assertEquals(ft, fieldTransformation.getTransformation());

    List<Filter> filtersList = newState.getFiltersList();
    assertTrue((filtersList == null) || (filtersList.size() == 0));
  }

  @Test
  public void testConvertNumberToDate() {
    Expression a = new ExpColumnReference("a").wrap();

    state.setColumnsList(asList(new Column("a", a)));
    FieldTransformationBase ft = new FieldConvertNumberToDate()
            .setDesiredType(DATETIME)
            .setFormat(EXCEL);
    TransformResult result = transform(new TransformField("a", "a", true, ft.wrap()));

    VirtualDatasetState newState = result.getNewState();

    assertEquals(1, newState.getColumnsList().size());
    Column column = newState.getColumnsList().get(0);
    assertEquals("a", column.getName());
    assertEquals(FieldTransformation, column.getValue().getType());
    ExpFieldTransformation fieldTransformation = column.getValue().getFieldTransformation();
    assertEquals(ConvertNumberToDate, fieldTransformation.getTransformation().getType());

    FieldConvertNumberToDate fcnd = fieldTransformation.getTransformation().getNumberToDate();

    assertEquals(DATETIME, fcnd.getDesiredType());
    assertEquals(EXCEL, fcnd.getFormat());

  }

  @Test
  public void testConvertFromJSON() {
    Expression a = new ExpColumnReference("a").wrap();

    state.setColumnsList(asList(new Column("a", a)));

    FieldTransformation ft = new FieldConvertFromJSON().wrap();
    TransformResult result = transform(new TransformField("a", "a", true, ft));

    VirtualDatasetState newState = result.getNewState();

    assertEquals(1, newState.getColumnsList().size());
    Column column = newState.getColumnsList().get(0);
    assertEquals("a", column.getName());
    assertEquals(FieldTransformation, column.getValue().getType());
    ExpFieldTransformation fieldTransformation = column.getValue().getFieldTransformation();
    assertEquals(a.toString(), fieldTransformation.getOperand().toString());
    assertEquals(ConvertFromJSON, fieldTransformation.getTransformation().getType());
    assertEquals(ft, fieldTransformation.getTransformation());

  }

  @Test
  public void testConvertFromJsonThenExtractList() {
    Expression a = new ExpColumnReference("a").wrap();

    state.setColumnsList(asList(new Column("a", a)));

    FieldTransformation ft = new FieldConvertFromJSON().wrap();
    TransformResult result = transform(new TransformField("a", "a", true, ft));

    state = result.getNewState();

    TransformResult result2 = transform(new TransformField("a", "a", true,
      new FieldTransformation(FieldTransformationType.ExtractList)
        .setExtractList(new FieldExtractList(
            new ExtractListRule(ExtractListRuleType.single)
              .setSingle(new ExtractRuleSingle(0))))));
    VirtualDatasetState newState = result2.getNewState();

    // outside of the expected subquery there should be one expression in the form of a[0], extracting
    // the first element out of a list
    assertEquals(1, newState.getColumnsList().size());
    Column col = newState.getColumnsList().get(0);
    assertEquals("a", col.getName());
    assertEquals(FieldTransformation, col.getValue().getType());
    assertEquals(FieldTransformationType.ExtractList, col.getValue().getFieldTransformation().getTransformation().getType());

    // Dremio does not support referencing an array produced by a function, like convert_from(a_col,'JSON')[0]
    // so instead we move the function that produces the list into a subquery and make the array extraction happen
    // in an outer query
    assertEquals(FromType.SubQuery, newState.getFrom().getType());
    VirtualDatasetState subQuery = newState.getFrom().getSubQuery().getSuqQuery();
    assertEquals(datasetPath.toUnescapedString(), subQuery.getFrom().getTable().getDatasetPath());
    // check in the subquery for a single expression convert_from(a,'JSON') a
    assertEquals(1, subQuery.getColumnsList().size());
    Column columnInSub = subQuery.getColumnsList().get(0);
    assertEquals("a", columnInSub.getName());
    assertEquals(FieldTransformation, columnInSub.getValue().getType());
    assertEquals(FieldTransformationType.ConvertFromJSON, columnInSub.getValue().getFieldTransformation().getTransformation().getType());
    assertEquals(ColumnReference, columnInSub.getValue().getFieldTransformation().getOperand().getType());
  }

  @Test
  public void testUpdateSql() throws Exception {
    final String sql = "select foo, bar as b from tbl";
    SqlParser parser = SqlParser.create(sql, new ParserConfig(Quoting.DOUBLE_QUOTE, 128, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal()));
    final SqlNode sqlNode = parser.parseStmt();


    final JavaTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;
    final RelDataType rowType = new RelRecordType(Arrays.<RelDataTypeField>asList(
        new RelDataTypeFieldImpl("foo", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)),
        new RelDataTypeFieldImpl("b", 0, typeFactory.createSqlType(SqlTypeName.INTEGER))
    ));

    final QueryMetadata metadata = Mockito.mock(QueryMetadata.class);
    Mockito.when(metadata.getSqlNode()).thenReturn(Optional.of(sqlNode));
    Mockito.when(metadata.getRowType()).thenReturn(rowType);
    Mockito.when(metadata.getQuerySql()).thenReturn(sql);
    Mockito.when(metadata.getReferredTables()).thenReturn(Optional.absent());

    TransformActor actor = new TransformActor(state, false, "test_user", null) {
      @Override
      protected com.dremio.service.jobs.metadata.proto.QueryMetadata getMetadata(SqlQuery query) {
        return com.dremio.service.jobs.metadata.proto.QueryMetadata.newBuilder()
          .setState(QuerySemantics.extract(metadata).get())
          .build();
      }

      @Override
      protected boolean hasMetadata() {
        return true;
      }

      @Override
      protected com.dremio.service.jobs.metadata.proto.QueryMetadata getMetadata() {
        throw new IllegalStateException("not implemented");
      }

      @Override
      protected Optional<BatchSchema> getBatchSchema() {
        throw new IllegalStateException("not implemented");
      }

      @Override
      protected Optional<List<ParentDatasetInfo>> getParents() {
        throw new IllegalStateException("not implemented");
      }

      @Override
      protected Optional<List<FieldOrigin>> getFieldOrigins() {
        throw new IllegalStateException("not implemented");
      }

      @Override
      protected Optional<List<ParentDataset>> getGrandParents() {
        throw new IllegalStateException("not implemented");
      }
    };

    TransformResult result = new TransformUpdateSQL(sql).accept(actor);
    VirtualDatasetState newState = result.getNewState();

    assertEquals(2, newState.getColumnsList().size());
    assertEquals("foo", newState.getColumnsList().get(0).getName());
    assertEquals(new ExpColumnReference("foo").wrap(), newState.getColumnsList().get(0).getValue());
    assertEquals("b", newState.getColumnsList().get(1).getName());
    assertEquals(new ExpColumnReference("bar").wrap(), newState.getColumnsList().get(1).getValue());
  }

  @Test
  public void testTransformWithExtract() throws Exception {
    setSpace();
    DatasetPath myDatasetPath = new DatasetPath("spacefoo.folderbar.folderbaz.datasetbuzz");
    createDatasetFromParentAndSave(myDatasetPath, "cp.\"tpch/supplier.parquet\"");

    DatasetUI dataset = getDataset(myDatasetPath);

    Transformer testTransformer =
      new Transformer(l(SabotContext.class), l(JobsService.class), newNamespaceService(), newDatasetVersionMutator(),
        null, l(SecurityContext.class));

    VirtualDatasetUI vdsui = DatasetsUtil.getHeadVersion(myDatasetPath, newNamespaceService(),
      newDatasetVersionMutator());
    List<ViewFieldType> sqlFields = vdsui.getSqlFieldsList();
    boolean isContainOriginal = false;
    boolean isContainConverted = false;
    for (ViewFieldType sqlType : sqlFields) {
      if (sqlType.getName().equalsIgnoreCase("s_address")) {
        isContainOriginal = true;
        break;
      }
    }
    assertTrue(isContainOriginal);
    isContainOriginal = false;
    VirtualDatasetUI vdsuiTransformed = testTransformer.transformWithExtract(dataset.getDatasetVersion(), myDatasetPath,
      vdsui,
      new TransformRename("s_address", "s_addr"));

    List<ViewFieldType> sqlFieldsTransformed = vdsuiTransformed.getSqlFieldsList();
    for (ViewFieldType sqlType : sqlFieldsTransformed) {
      if (sqlType.getName().equalsIgnoreCase("s_addr")) {
        isContainConverted = true;
      }
      if (sqlType.getName().equalsIgnoreCase("s_address")) {
        isContainOriginal = true;
      }
    }
    assertTrue(isContainConverted);
    assertTrue(!isContainOriginal);
  }

  @Test // Test retrieval of metadata by transformWithExecute() through use of the QueryParser.
  public void testTransformMetadataRetrieval() throws Exception {
    // Setup the dataset and transformations.
    setSpace();
    final NamespaceService namespaceService = newNamespaceService();
    final DatasetVersionMutator datasetService = newDatasetVersionMutator();

    final String updatedSQL = "SELECT * FROM cp.\"tpch/supplier.parquet\" LIMIT 3";
    final DatasetPath datasetPath = new DatasetPath("spacefoo.folderbar.folderbaz.testTransform");
    final DatasetUI dataset =createDatasetFromSQLAndSave(datasetPath, updatedSQL, asList("cp"));

    final Transform firstTransform =
      new Transform().setType(TransformType.createFromParent).setTransformCreateFromParent(
        new TransformCreateFromParent().setCreateFrom(new From().setType(FromType.Table).setTable(
          new FromTable().setDatasetPath(datasetPath.toPathString()))));

    final TransformCreateFromParent transformCreateFromParent = firstTransform.getTransformCreateFromParent();
    final DatasetPath headPath = new DatasetPath(transformCreateFromParent.getCreateFrom().getTable().getDatasetPath());
    final DatasetConfig headConfig = namespaceService.getDataset(headPath.toNamespaceKey());
    final VirtualDatasetUI headVersion = datasetService.getVersion(headPath, headConfig.getVirtualDataset().getVersion());
    final QueryExecutor executor = new QueryExecutor(l(JobsService.class), null, null);

    final Transformer testTransformer =
      new Transformer(l(SabotContext.class), l(JobsService.class), namespaceService, datasetService,
        executor, l(SecurityContext.class));

    final TransformUpdateSQL transformUpdateSQL =
      new TransformUpdateSQL().setSql(updatedSQL).setSqlContextList(new ArrayList<>(Arrays.asList("cp")));

    // Setup the transform actor such that query executor will find the relevant job in the job store
    // when executing the query as part of the metadata retrieval.
    final Transformer.ExecuteTransformActor actor = testTransformer.
      new ExecuteTransformActor(
        QueryType.UI_PREVIEW,
        dataset.getDatasetVersion(),
        headVersion.getState(),
        false,
        l(SecurityContext.class).getUserPrincipal().getName(),
        new DatasetPath("tmp.UNTITLED"),
        executor);

    SqlQuery query =
      new SqlQuery(transformUpdateSQL.getSql(), transformUpdateSQL.getSqlContextList(),
        l(SecurityContext.class).getUserPrincipal().getName());

    // Force metadata to be regenerated.
    com.dremio.service.jobs.metadata.proto.QueryMetadata queryMetadata = actor.getMetadata(query);

    final JobId jobId = actor.getJobData().getJobId();
    final JobInfo jobInfo = JobsProtoUtil.getLastAttempt(l(JobsService.class).getJobDetails(JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(jobId))
      .build())).getInfo();

    List<ParentDatasetInfo> parents = jobInfo.getParentsList();


    // Check the generated QueryMetadata matches that of the updated SQL.
    assertNotNull(queryMetadata);
    assertEquals(7, queryMetadata.getFieldTypeCount());
    assertTrue(queryMetadata.getState().getFrom().getValue().contains("LIMIT 3"));
    assertEquals(new ArrayList<>(Arrays.asList("cp", "tpch/supplier.parquet")), parents.get(0).getDatasetPathList());
  }
}
