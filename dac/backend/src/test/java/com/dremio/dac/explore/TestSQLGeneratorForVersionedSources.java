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

import static com.dremio.dac.proto.model.dataset.DataType.DATE;
import static com.dremio.dac.proto.model.dataset.MeasureType.Sum;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.FieldTransformationBase;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.Dimension;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.ExpFieldTransformation;
import com.dremio.dac.proto.model.dataset.Expression;
import com.dremio.dac.proto.model.dataset.FieldReplaceValue;
import com.dremio.dac.proto.model.dataset.Filter;
import com.dremio.dac.proto.model.dataset.FilterDefinition;
import com.dremio.dac.proto.model.dataset.FilterRange;
import com.dremio.dac.proto.model.dataset.FilterType;
import com.dremio.dac.proto.model.dataset.FilterValue;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromSubQuery;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.Join;
import com.dremio.dac.proto.model.dataset.JoinCondition;
import com.dremio.dac.proto.model.dataset.JoinType;
import com.dremio.dac.proto.model.dataset.Measure;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.OrderDirection;
import com.dremio.dac.proto.model.dataset.ReplaceType;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.TransformFilter;
import com.dremio.dac.proto.model.dataset.TransformGroupBy;
import com.dremio.dac.proto.model.dataset.VersionContext;
import com.dremio.dac.proto.model.dataset.VersionContextType;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;

/**
 * SQLGenerator tests for Versioned sources
 */
public class TestSQLGeneratorForVersionedSources {
  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private CatalogService catalogService;

  @Mock
  private StoragePlugin storagePlugin;

  @Test
  public void testJoin() {
    List<SourceVersionReference> sourceVersionReferenceListForRight = new ArrayList<>();
    sourceVersionReferenceListForRight.add(new SourceVersionReference("versioned_source1", new VersionContext(VersionContextType.BRANCH, "main")));
    List<SourceVersionReference> sourceVersionReferenceListForLeft = new ArrayList<>();
    sourceVersionReferenceListForLeft.add(new SourceVersionReference("versioned_source2", new VersionContext(VersionContextType.BRANCH, "branch_test")));
    From fromTable = new FromTable("versioned_source2.parentDS").wrap();
    From ref = new FromSubQuery("versioned_source2.parentDS",
      new VirtualDatasetState().setFrom(fromTable).setReferenceList(sourceVersionReferenceListForLeft)).wrap();
    VirtualDatasetState state = new VirtualDatasetState()
      .setFrom(ref)
      .setReferenceList(sourceVersionReferenceListForRight);

    when(catalogService.getSource("versioned_source1")).thenReturn(mock(FakeVersionedPlugin.class));
    when(catalogService.getSource("versioned_source2")).thenReturn(mock(FakeVersionedPlugin.class));
    when(catalogService.getSource("non_versioned_source")).thenReturn(storagePlugin);

    //Both left and right tables are versioned sources
    validateForTranformRelatedCalls(
      "SELECT *\n" +
        "FROM (\n" +
        "  SELECT *\n" +
        "  FROM versioned_source2.parentDS AT BRANCH \"branch_test\"\n" +
        ") \"versioned_source2.parentDS\"\n" +
        " INNER JOIN versioned_source1.foo AT BRANCH \"main\" AS join_1 ON \"versioned_source2.parentDS\".bar = join_1.bar",
      state.setJoinsList(asList(new Join(JoinType.Inner, "versioned_source1.foo", "join_1")
        .setJoinConditionsList(asList(new JoinCondition("bar", "bar")))))
    );

    //Only left is versioned source
    state = new VirtualDatasetState()
      .setFrom(ref);
    validateForTranformRelatedCalls(
      "SELECT *\n" +
        "FROM (\n" +
        "  SELECT *\n" +
        "  FROM versioned_source2.parentDS AT BRANCH \"branch_test\"\n" +
        ") \"versioned_source2.parentDS\"\n" +
        " INNER JOIN non_versioned_source.foo AS join_1 ON \"versioned_source2.parentDS\".bar = join_1.bar",
      state.setJoinsList(asList(new Join(JoinType.Inner, "non_versioned_source.foo", "join_1")
        .setJoinConditionsList(asList(new JoinCondition("bar", "bar")))))
    );

    //Only right is versioned source
    fromTable = new FromTable("non_versioned_source.parentDS").wrap();
    ref = new FromSubQuery("non_versioned_source.parentDS",
      new VirtualDatasetState().setFrom(fromTable).setReferenceList(sourceVersionReferenceListForRight)).wrap();
    state = new VirtualDatasetState()
      .setFrom(ref)
      .setReferenceList(sourceVersionReferenceListForRight);
    validateForTranformRelatedCalls(
      "SELECT *\n" +
        "FROM (\n" +
        "  SELECT *\n" +
        "  FROM non_versioned_source.parentDS\n" +
        ") \"non_versioned_source.parentDS\"\n" +
        " INNER JOIN versioned_source1.foo AT BRANCH \"main\" AS join_1 ON \"non_versioned_source.parentDS\".bar = join_1.bar",
      state.setJoinsList(asList(new Join(JoinType.Inner, "versioned_source1.foo", "join_1")
        .setJoinConditionsList(asList(new JoinCondition("bar", "bar")))))
    );
  }

  @Test
  public void testOrderBy() {
    List<SourceVersionReference> sourceVersionReferenceList = new ArrayList<>();
    sourceVersionReferenceList.add(new SourceVersionReference("versioned_source", new VersionContext(VersionContextType.BRANCH, "branch")));
    when(catalogService.getSource("versioned_source")).thenReturn(mock(FakeVersionedPlugin.class));
    VirtualDatasetState state = new VirtualDatasetState()
      .setFrom(new FromTable("versioned_source.tables.1234.test").wrap())
      .setReferenceList(sourceVersionReferenceList)
      // user is a reserved keyword, make sure it is escaped in generated SQL
      .setOrdersList(asList(new Order("user", OrderDirection.ASC)));
    validateForTranformRelatedCalls("SELECT *\nFROM versioned_source.\"tables\".\"1234\".test AT BRANCH \"branch\"\nORDER BY \"user\" ASC", state);
  }

  @Test
  public void testFilters() {
    List<SourceVersionReference> sourceVersionReferenceList = new ArrayList<>();
    sourceVersionReferenceList.add(new SourceVersionReference("versioned_source", new VersionContext(VersionContextType.BRANCH, "branch")));
    From fromTable = new FromTable("versioned_source.parentDS").wrap();
    when(catalogService.getSource("versioned_source")).thenReturn(mock(FakeVersionedPlugin.class));
    VirtualDatasetState state = new VirtualDatasetState()
      .setFrom(fromTable)
      .setReferenceList(sourceVersionReferenceList);

    Expression column = new ExpColumnReference("count").wrap();
    validateForTranformRelatedCalls(
      "SELECT count\n" +
        "FROM versioned_source.parentDS AT BRANCH \"branch\"\n" +
        " WHERE (5 <= count AND 20 > count) AND (10 >= count OR 15 < count)",
      state
        .setColumnsList(asList(new Column("count", column)))
        .setFiltersList(asList(
          new Filter(column,
            new FilterDefinition(FilterType.Range)
              .setRange(new FilterRange().setLowerBound("5").setUpperBound("20").setDataType(DataType.INTEGER).setLowerBoundInclusive(true))
          ).setKeepNull(false),
          new Filter(column,
            new FilterDefinition(FilterType.Range)
              .setRange(new FilterRange().setLowerBound("10").setUpperBound("15").setDataType(DataType.INTEGER).setLowerBoundInclusive(true))
          ).setKeepNull(false).setExclude(true)
        ))
    );
  }

  @Test
  public void testReplaceValue() {
    List<SourceVersionReference> sourceVersionReferenceList = new ArrayList<>();
    sourceVersionReferenceList.add(new SourceVersionReference("versioned_source", new VersionContext(VersionContextType.BRANCH, "branch")));
    From fromTable = new FromTable("versioned_source.parentDS").wrap();
    when(catalogService.getSource("versioned_source")).thenReturn(mock(FakeVersionedPlugin.class));
    VirtualDatasetState state = new VirtualDatasetState()
      .setFrom(fromTable)
      .setReferenceList(sourceVersionReferenceList);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldTransformationBase transf1 =
      new FieldReplaceValue()
        .setReplacedValuesList(asList("1970-07-04", "1989-05-31"))
        .setReplacementType(DATE)
        .setReplacementValue("2016-11-05")
        .setReplaceNull(true)
        .setReplaceType(ReplaceType.VALUE);

    Expression exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    String expQuery = "SELECT CASE\n" +
      "  WHEN bar = DATE '1970-07-04' THEN DATE '2016-11-05'\n" +
      "  WHEN bar = DATE '1989-05-31' THEN DATE '2016-11-05'\n" +
      "  WHEN bar IS NULL THEN DATE '2016-11-05'\n" +
      "  ELSE bar\n" +
      "END AS foo\n" +
      "FROM versioned_source.parentDS AT BRANCH \"branch\"";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));
  }

  @Test
  public void testExcludeValue() {
    List<SourceVersionReference> sourceVersionReferenceList = new ArrayList<>();
    sourceVersionReferenceList.add(new SourceVersionReference("versioned_source", new VersionContext(VersionContextType.BRANCH, "branch")));
    From fromTable = new FromTable("versioned_source.parentDS").wrap();
    when(catalogService.getSource("versioned_source")).thenReturn(mock(FakeVersionedPlugin.class));
    VirtualDatasetState state = new VirtualDatasetState()
      .setFrom(fromTable)
      .setReferenceList(sourceVersionReferenceList);

    Expression column = new ExpColumnReference("user").wrap();

    validateForTranformRelatedCalls(
      "SELECT parentDS.\"user\" AS foo\nFROM versioned_source.parentDS AT BRANCH \"branch\"\n " +
        "WHERE parentDS.\"user\" NOT IN ('foo', 'bar')",
      state
        .setColumnsList(asList(new Column("foo", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
          .setValue(new FilterValue(DataType.DATE).setValuesList(asList("foo", "bar")))).setExclude(true).setKeepNull(false)
        ))
    );

    validateForTranformRelatedCalls(
      "SELECT parentDS.\"user\" AS foo\nFROM versioned_source.parentDS AT BRANCH \"branch\"\n " +
        "WHERE parentDS.\"user\" = 'foo'",
      state
        .setColumnsList(asList(new Column("foo", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
          .setValue(new FilterValue(DataType.DATE).setValuesList(asList("foo")))).setExclude(false).setKeepNull(false)
        ))
    );

    validateForTranformRelatedCalls(
      "SELECT parentDS.\"user\" AS foo\nFROM versioned_source.parentDS AT BRANCH \"branch\"\n " +
        "WHERE parentDS.\"user\" <> 'bar' OR parentDS.\"user\" IS NULL ",
      state
        .setColumnsList(asList(new Column("foo", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
          .setValue(new FilterValue(DataType.DATE).setValuesList(asList("bar")))).setExclude(true).setKeepNull(true)
        ))
    );
  }

  @Test
  public void testGroupBy() {
    List<SourceVersionReference> sourceVersionReferenceList = new ArrayList<>();
    sourceVersionReferenceList.add(new SourceVersionReference("versioned_source", new VersionContext(VersionContextType.BRANCH, "branch")));
    From fromTable = new FromTable("versioned_source.parentDS").wrap();
    when(catalogService.getSource("versioned_source")).thenReturn(mock(FakeVersionedPlugin.class));
    VirtualDatasetState state = new VirtualDatasetState()
      .setFrom(fromTable)
      .setReferenceList(sourceVersionReferenceList);

    Expression a = new ExpColumnReference("a").wrap();
    Expression b = new ExpColumnReference("b").wrap();

    state.setColumnsList(asList(new Column("a", a), new Column("b", b)));

    TransformResult sumTransform = transform(new TransformGroupBy()
      .setColumnsDimensionsList(asList(new Dimension("a")))
      .setColumnsMeasuresList(asList(new Measure(Sum).setColumn("b"))), state);
    state = sumTransform.getNewState();

    String expSumQuery =
      "SELECT a, SUM(b) AS Sum_b\n" +
        "FROM versioned_source.parentDS AT BRANCH \"branch\"\n" +
        "GROUP BY a";
    validateForTranformRelatedCalls(expSumQuery, state);

    TransformResult keeponlyTransform = transform( new TransformFilter("Sum_b", new FilterDefinition(FilterType.Range)
      .setRange(new FilterRange(DataType.INTEGER).setLowerBound("1"))), state);
    VirtualDatasetState filtState = keeponlyTransform.getNewState();

    String expQuery =
      "SELECT a, Sum_b\n" +
        "FROM (\n" +
        "  SELECT a, SUM(b) AS Sum_b\n" +
        "  FROM versioned_source.parentDS AT BRANCH \"branch\"\n" +
        "  GROUP BY a\n" +
        ") nested_0\n" +
        " WHERE 1 < Sum_b";
    validateForTranformRelatedCalls(expQuery, filtState);
  }

  private TransformResult transform(TransformBase tb, VirtualDatasetState state) {
    QueryExecutor executor = new QueryExecutor(null, null, null){
      @Override
      public List<String> getColumnList(DatasetPath path, List<SourceVersionReference> sourceVersionReferenceList) {
        return asList("bar", "baz");
      }
    };

    TransformActor actor = new TransformActor(state, false, "test_user", executor){
      @Override
      protected QueryMetadata getMetadata(SqlQuery query) {
        return mock(QueryMetadata.class);
      }

      @Override
      protected boolean hasMetadata() {
        return true;
      }

      @Override
      protected QueryMetadata getMetadata() {
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

    return tb.accept(actor);
  }

  private void validateForTranformRelatedCalls(String expectedSQL, VirtualDatasetState state) {
    String sql = SQLGenerator.generateSQL(state, true, catalogService);
    assertEquals(expectedSQL, sql);
  }

  /**
   * Fake Versioned Plugin interface for test
   */
  private interface FakeVersionedPlugin extends VersionedPlugin, StoragePlugin {
  }
}
