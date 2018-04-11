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
package com.dremio.sabot.join;

import static com.dremio.sabot.Fixtures.NULL_BIGINT;
import static com.dremio.sabot.Fixtures.NULL_VARCHAR;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Test;

import com.dremio.common.logical.data.JoinCondition;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.DataRow;
import com.dremio.sabot.Fixtures.HeaderRow;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.Generator;
import com.dremio.sabot.join.hash.EmptyGenerator;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.google.common.collect.ImmutableList;

import io.airlift.tpch.GenerationDefinition.TpchTable;
import io.airlift.tpch.TpchGenerator;

public abstract class BaseTestJoin extends BaseTestOperator {

  public static class JoinInfo {
    public final Class<? extends DualInputOperator> clazz;
    public final PhysicalOperator operator;

    public <T extends DualInputOperator> JoinInfo(Class<T> clazz, PhysicalOperator operator) {
      super();
      this.clazz = clazz;
      this.operator = operator;
    }
  }

  protected abstract JoinInfo getJoinInfo(List<JoinCondition> conditions, JoinRelType type);

  @Test
  public void emptyRight() throws Exception {
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("n_nationKey"), f("key"))),
      JoinRelType.INNER);

    final Table expected = t(
      th("key", "value", "n_nationKey"),
      true,
      tr(0L, 0L, 0L) // fake row to match types.
    );

    validateDual(joinInfo.operator, joinInfo.clazz,
      TpchGenerator.singleGenerator(TpchTable.NATION, 0.1, getTestAllocator(), "n_nationKey"),
      new EmptyGenerator(getTestAllocator()), DEFAULT_BATCH, expected, false);
  }

  @Test
  public void emptyRightWithLeftJoin() throws Exception {
    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("r_regionKey"), f("key"))),
      JoinRelType.LEFT);

    final Table expected = t(
      th("key", "value", "r_regionKey"),
      false,
      tr(NULL_BIGINT, NULL_BIGINT, 0L),
      tr(NULL_BIGINT, NULL_BIGINT, 1L),
      tr(NULL_BIGINT, NULL_BIGINT, 2L),
      tr(NULL_BIGINT, NULL_BIGINT, 3L),
      tr(NULL_BIGINT, NULL_BIGINT, 4L)
    );


    validateDual(joinInfo.operator, joinInfo.clazz,
      TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_regionKey"),
      new EmptyGenerator(getTestAllocator()), DEFAULT_BATCH, expected);
  }

  @Test
  public void isNotDistinctWithNulls() throws Exception{
    JoinInfo includeNullsInfo = getJoinInfo(Arrays.asList(new JoinCondition("IS_NOT_DISTINCT_FROM", f("id1"), f("id2"))), JoinRelType.INNER);
    final Table includeNulls = t(
        th("id2", "name2", "id1", "name1"),
        tr(Fixtures.NULL_BIGINT, "b1", Fixtures.NULL_BIGINT, "a1"),
        tr(4l, "b2", 4l, "a2")
        );
    nullKeys(includeNullsInfo, includeNulls);

  }

  @Test
  public void noNullEquivalenceWithNulls() throws Exception{
    JoinInfo noNullsInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("id1"), f("id2"))), JoinRelType.INNER);
    final Table noNulls = t(
        th("id2", "name2", "id1", "name1"),
        tr(4l, "b2", 4l, "a2")
        );
    nullKeys(noNullsInfo, noNulls);
  }

  @Test
  public void noNullEquivalenceWithNullsLeft() throws Exception{
    JoinInfo noNullsInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("id1"), f("id2"))), JoinRelType.LEFT);
    final Table noNulls = t(
        th("id2", "name2", "id1", "name1"),
        tr(Fixtures.NULL_BIGINT, Fixtures.NULL_VARCHAR, Fixtures.NULL_BIGINT, "a1"),
        tr(4l, "b2", 4l, "a2")
        );
    nullKeys(noNullsInfo, noNulls);
  }

  @Test
  public void noNullEquivalenceWithNullsRight() throws Exception{
    JoinInfo noNullsInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("id1"), f("id2"))), JoinRelType.RIGHT);
    final Table noNulls = t(
        th("id2", "name2", "id1", "name1"),
        tr(4l, "b2", 4l, "a2"),
        tr(Fixtures.NULL_BIGINT, "b1", Fixtures.NULL_BIGINT, Fixtures.NULL_VARCHAR)
        );
    nullKeys(noNullsInfo, noNulls);
  }

  private void nullKeys(JoinInfo info, Table expected) throws Exception {
    final Table left = t(
        th("id1", "name1"),
        tr(Fixtures.NULL_BIGINT, "a1"),
        tr(4l, "a2")
        );

    final Table right = t(
        th("id2", "name2"),
        tr(Fixtures.NULL_BIGINT, "b1"),
        tr(4l, "b2")
        );
    validateDual(
        info.operator, info.clazz,
        left.toGenerator(getTestAllocator()),
        right.toGenerator(getTestAllocator()),
        DEFAULT_BATCH, expected);
  }



  @Test
  public void regionNationInner() throws Exception {
    JoinInfo info = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("r_regionKey"), f("n_regionKey"))), JoinRelType.INNER);

    final Table expected = t(
        th("n_name", "n_regionKey", "r_regionKey", "r_name"),
        tr("ALGERIA", 0L, 0L, "AFRICA"),
        tr("MOZAMBIQUE", 0L, 0L, "AFRICA"),
        tr("MOROCCO", 0L, 0L, "AFRICA"),
        tr("KENYA", 0L, 0L, "AFRICA"),
        tr("ETHIOPIA", 0L, 0L, "AFRICA"),
        tr("ARGENTINA", 1L, 1L, "AMERICA"),
        tr("UNITED STATES", 1L, 1L, "AMERICA"),
        tr("PERU", 1L, 1L, "AMERICA"),
        tr("CANADA", 1L, 1L, "AMERICA"),
        tr("BRAZIL", 1L, 1L, "AMERICA"),
        tr("INDIA", 2L, 2L, "ASIA"),
        tr("VIETNAM", 2L, 2L, "ASIA"),
        tr("CHINA", 2L, 2L, "ASIA"),
        tr("JAPAN", 2L, 2L, "ASIA"),
        tr("INDONESIA", 2L, 2L, "ASIA"),
        tr("FRANCE", 3L, 3L, "EUROPE"),
        tr("UNITED KINGDOM", 3L, 3L, "EUROPE"),
        tr("RUSSIA", 3L, 3L, "EUROPE"),
        tr("ROMANIA", 3L, 3L, "EUROPE"),
        tr("GERMANY", 3L, 3L, "EUROPE"),
        tr("EGYPT", 4L, 4L, "MIDDLE EAST"),
        tr("SAUDI ARABIA", 4L, 4L, "MIDDLE EAST"),
        tr("JORDAN", 4L, 4L, "MIDDLE EAST"),
        tr("IRAQ", 4L, 4L, "MIDDLE EAST"),
        tr("IRAN", 4L, 4L, "MIDDLE EAST")
        );

    validateDual(
        info.operator, info.clazz,
        TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_regionKey", "r_name"),
        TpchGenerator.singleGenerator(TpchTable.NATION, 0.1, getTestAllocator(), "n_regionKey", "n_name"),
        DEFAULT_BATCH, expected);
  }

  @Test
  public void nationRegionPartialKeyRight() throws Exception {
    JoinInfo info = getJoinInfo( Arrays.asList(new JoinCondition("EQUALS", f("n_nationKey"), f("r_regionKey"))), JoinRelType.RIGHT);

    final Table expected = t(
        th("r_regionKey", "r_name", "n_nationKey", "n_name"),
        tr(0L, "AFRICA", 0L, "ALGERIA"),
        tr(1L, "AMERICA", 1L, "ARGENTINA"),
        tr(2L, "ASIA", 2L, "BRAZIL"),
        tr(3L, "EUROPE", 3L, "CANADA"),
        tr(4L, "MIDDLE EAST", 4L, "EGYPT")
        );

    validateDual(
        info.operator, info.clazz,
        TpchGenerator.singleGenerator(TpchTable.NATION, 0.1, getTestAllocator(), "n_nationKey", "n_name"),
        TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_regionKey", "r_name"),
        DEFAULT_BATCH, expected);
  }

  @Test
  public void regionNationPartialKeyRight() throws Exception {

    JoinInfo info = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("r_regionKey"), f("n_nationKey"))), JoinRelType.RIGHT);

    final Table expected = t(
        th("n_nationKey", "n_name", "r_regionKey", "r_name"),
        tr(0L, "ALGERIA", 0L, "AFRICA"),
        tr(1L, "ARGENTINA", 1L, "AMERICA"),
        tr(2L, "BRAZIL", 2L, "ASIA"),
        tr(3L, "CANADA", 3L, "EUROPE"),
        tr(4L, "EGYPT", 4L, "MIDDLE EAST"),
        tr(5L, "ETHIOPIA", NULL_BIGINT, NULL_VARCHAR),
        tr(6L, "FRANCE", NULL_BIGINT, NULL_VARCHAR),
        tr(7L, "GERMANY", NULL_BIGINT, NULL_VARCHAR),
        tr(8L, "INDIA", NULL_BIGINT, NULL_VARCHAR),
        tr(9L, "INDONESIA", NULL_BIGINT, NULL_VARCHAR),
        tr(10L, "IRAN", NULL_BIGINT, NULL_VARCHAR),
        tr(11L, "IRAQ", NULL_BIGINT, NULL_VARCHAR),
        tr(12L, "JAPAN", NULL_BIGINT, NULL_VARCHAR),
        tr(13L, "JORDAN", NULL_BIGINT, NULL_VARCHAR),
        tr(14L, "KENYA", NULL_BIGINT, NULL_VARCHAR),
        tr(15L, "MOROCCO", NULL_BIGINT, NULL_VARCHAR),
        tr(16L, "MOZAMBIQUE", NULL_BIGINT, NULL_VARCHAR),
        tr(17L, "PERU", NULL_BIGINT, NULL_VARCHAR),
        tr(18L, "CHINA", NULL_BIGINT, NULL_VARCHAR),
        tr(19L, "ROMANIA", NULL_BIGINT, NULL_VARCHAR),
        tr(20L, "SAUDI ARABIA", NULL_BIGINT, NULL_VARCHAR),
        tr(21L, "VIETNAM", NULL_BIGINT, NULL_VARCHAR),
        tr(22L, "RUSSIA", NULL_BIGINT, NULL_VARCHAR),
        tr(23L, "UNITED KINGDOM", NULL_BIGINT, NULL_VARCHAR),
        tr(24L, "UNITED STATES", NULL_BIGINT, NULL_VARCHAR)
        );

    validateDual(
        info.operator, info.clazz,
        TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_regionKey", "r_name"),
        TpchGenerator.singleGenerator(TpchTable.NATION, 0.1, getTestAllocator(), "n_nationKey", "n_name"),
        DEFAULT_BATCH, expected);
  }

  @Test
  public void nationRegionPartialKeyLeft() throws Exception {

    JoinInfo info = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("n_nationKey"), f("r_regionKey"))), JoinRelType.LEFT);

    final Table expected = t(
        th("r_regionKey", "r_name", "n_nationKey", "n_name"),
        tr(0L, "AFRICA", 0L, "ALGERIA"),
        tr(1L, "AMERICA", 1L, "ARGENTINA"),
        tr(2L, "ASIA", 2L, "BRAZIL"),
        tr(3L, "EUROPE", 3L, "CANADA"),
        tr(4L, "MIDDLE EAST", 4L, "EGYPT"),
        tr(NULL_BIGINT, NULL_VARCHAR, 5L, "ETHIOPIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 6L, "FRANCE"),
        tr(NULL_BIGINT, NULL_VARCHAR, 7L, "GERMANY"),
        tr(NULL_BIGINT, NULL_VARCHAR, 8L, "INDIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 9L, "INDONESIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 10L, "IRAN"),
        tr(NULL_BIGINT, NULL_VARCHAR, 11L, "IRAQ"),
        tr(NULL_BIGINT, NULL_VARCHAR, 12L, "JAPAN"),
        tr(NULL_BIGINT, NULL_VARCHAR, 13L, "JORDAN"),
        tr(NULL_BIGINT, NULL_VARCHAR, 14L, "KENYA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 15L, "MOROCCO"),
        tr(NULL_BIGINT, NULL_VARCHAR, 16L, "MOZAMBIQUE"),
        tr(NULL_BIGINT, NULL_VARCHAR, 17L, "PERU"),
        tr(NULL_BIGINT, NULL_VARCHAR, 18L, "CHINA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 19L, "ROMANIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 20L, "SAUDI ARABIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 21L, "VIETNAM"),
        tr(NULL_BIGINT, NULL_VARCHAR, 22L, "RUSSIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 23L, "UNITED KINGDOM"),
        tr(NULL_BIGINT, NULL_VARCHAR, 24L, "UNITED STATES")
        );

    validateDual(
        info.operator, info.clazz,
        TpchGenerator.singleGenerator(TpchTable.NATION, 0.1, getTestAllocator(), "n_nationKey", "n_name"),
        TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_regionKey", "r_name"),
        DEFAULT_BATCH, expected);
  }

  @Test
  public void nationRegionPartialKeyOuter() throws Exception {
    JoinInfo info = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("n_nationKey"), f("r_regionKey"))), JoinRelType.FULL);

    final Table expected = t(
        th("r_regionKey", "r_name", "n_nationKey", "n_name"),
        tr(0L, "AFRICA", 0L, "ALGERIA"),
        tr(1L, "AMERICA", 1L, "ARGENTINA"),
        tr(2L, "ASIA", 2L, "BRAZIL"),
        tr(3L, "EUROPE", 3L, "CANADA"),
        tr(4L, "MIDDLE EAST", 4L, "EGYPT"),
        tr(NULL_BIGINT, NULL_VARCHAR, 5L, "ETHIOPIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 6L, "FRANCE"),
        tr(NULL_BIGINT, NULL_VARCHAR, 7L, "GERMANY"),
        tr(NULL_BIGINT, NULL_VARCHAR, 8L, "INDIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 9L, "INDONESIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 10L, "IRAN"),
        tr(NULL_BIGINT, NULL_VARCHAR, 11L, "IRAQ"),
        tr(NULL_BIGINT, NULL_VARCHAR, 12L, "JAPAN"),
        tr(NULL_BIGINT, NULL_VARCHAR, 13L, "JORDAN"),
        tr(NULL_BIGINT, NULL_VARCHAR, 14L, "KENYA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 15L, "MOROCCO"),
        tr(NULL_BIGINT, NULL_VARCHAR, 16L, "MOZAMBIQUE"),
        tr(NULL_BIGINT, NULL_VARCHAR, 17L, "PERU"),
        tr(NULL_BIGINT, NULL_VARCHAR, 18L, "CHINA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 19L, "ROMANIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 20L, "SAUDI ARABIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 21L, "VIETNAM"),
        tr(NULL_BIGINT, NULL_VARCHAR, 22L, "RUSSIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 23L, "UNITED KINGDOM"),
        tr(NULL_BIGINT, NULL_VARCHAR, 24L, "UNITED STATES")
        );

    validateDual(
        info.operator, info.clazz,
        TpchGenerator.singleGenerator(TpchTable.NATION, 0.1, getTestAllocator(), "n_nationKey", "n_name"),
        TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_regionKey", "r_name"),
        DEFAULT_BATCH, expected);
  }



  @Test
  public void nationRegionPartialKeyOuterSmall() throws Exception {
    JoinInfo info = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("n_nationKey"), f("r_regionKey"))), JoinRelType.FULL);

    final Table expected = t(
        th("r_regionKey", "r_name", "n_nationKey", "n_name"),
        tr(0L, "AFRICA", 0L, "ALGERIA"),
        tr(1L, "AMERICA", 1L, "ARGENTINA"),
        tr(2L, "ASIA", 2L, "BRAZIL"),
        tr(3L, "EUROPE", 3L, "CANADA"),
        tr(4L, "MIDDLE EAST", 4L, "EGYPT"),
        tr(NULL_BIGINT, NULL_VARCHAR, 5L, "ETHIOPIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 6L, "FRANCE"),
        tr(NULL_BIGINT, NULL_VARCHAR, 7L, "GERMANY"),
        tr(NULL_BIGINT, NULL_VARCHAR, 8L, "INDIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 9L, "INDONESIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 10L, "IRAN"),
        tr(NULL_BIGINT, NULL_VARCHAR, 11L, "IRAQ"),
        tr(NULL_BIGINT, NULL_VARCHAR, 12L, "JAPAN"),
        tr(NULL_BIGINT, NULL_VARCHAR, 13L, "JORDAN"),
        tr(NULL_BIGINT, NULL_VARCHAR, 14L, "KENYA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 15L, "MOROCCO"),
        tr(NULL_BIGINT, NULL_VARCHAR, 16L, "MOZAMBIQUE"),
        tr(NULL_BIGINT, NULL_VARCHAR, 17L, "PERU"),
        tr(NULL_BIGINT, NULL_VARCHAR, 18L, "CHINA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 19L, "ROMANIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 20L, "SAUDI ARABIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 21L, "VIETNAM"),
        tr(NULL_BIGINT, NULL_VARCHAR, 22L, "RUSSIA"),
        tr(NULL_BIGINT, NULL_VARCHAR, 23L, "UNITED KINGDOM"),
        tr(NULL_BIGINT, NULL_VARCHAR, 24L, "UNITED STATES")
        );

    validateDual(
        info.operator, info.clazz,
        TpchGenerator.singleGenerator(TpchTable.NATION, 0.1, getTestAllocator(), "n_nationKey", "n_name"),
        TpchGenerator.singleGenerator(TpchTable.REGION, 0.1, getTestAllocator(), "r_regionKey", "r_name"),
        3, expected);
  }

  private static final class ManyColumnsGenerator implements Generator {
    private final int columns;
    private final int rows;
    private final VectorContainer result;
    private final List<NullableIntVector> vectors;

    private int offset = 0;

    public ManyColumnsGenerator(BufferAllocator allocator, String prefix, int columns, int rows) {
      this.columns = columns;
      this.rows = rows;
      result = new VectorContainer(allocator);

      ImmutableList.Builder<NullableIntVector> vectorsBuilder = ImmutableList.builder();
      for(int i = 0; i<columns; i++) {
        NullableIntVector vector = result.addOrGet(String.format("%s_%d", prefix, i + 1), Types.optional(MinorType.INT), NullableIntVector.class);
        vectorsBuilder.add(vector);
      }
      this.vectors = vectorsBuilder.build();

      result.buildSchema(SelectionVectorMode.NONE);
    }

    @Override
    public int next(int records) {
      int count = Math.min(rows - offset, records);
      if (count == 0) {
        return 0;
      }

      result.allocateNew();
      for(int i = 0; i<count; i++) {
        int col = 0;
        for(NullableIntVector vector: vectors) {
          vector.setSafe(i, (offset + i) * columns + col);
          col++;
        }
      }

      offset += count;
      result.setAllCount(count);

      return count;
    }

    @Override
    public VectorAccessible getOutput() {
      return result;
    }

    @Override
    public void close() throws Exception {
      result.close();
    }
  }

  private static HeaderRow getHeader(String leftPrefix, int leftColumns, String rightPrefix, int rightColumns) {
    String[] names = new String[leftColumns + rightColumns];
    for(int i = 0; i<leftColumns; i++) {
      names[i] = String.format("%s_%d", leftPrefix, i+1);
    }
    for(int i = 0; i<rightColumns; i++) {
      names[i + leftColumns] = String.format("%s_%d", rightPrefix, i+1);
    }
    return new HeaderRow(names);
  }

  private static DataRow[] getData( int leftColumns, int rightColumns, int count) {
    DataRow[] rows = new DataRow[count];

    for(int i = 0; i<count; i++) {
      Object[] objects = new Object[leftColumns + rightColumns];

      for(int j = 0; j<leftColumns; j++) {
        objects[j] = i * leftColumns + j;
      }
      for(int j = 0; j<rightColumns; j++) {
        objects[j + leftColumns] = i * rightColumns + j;
      }
      rows[i] = tr(objects);
    }

    return rows;
  }

  protected void baseManyColumns() throws Exception {
    int columns = 1000;
    int leftColumns = columns;
    int rightColumns = columns;

    JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("left_1"), f("right_1"))), JoinRelType.LEFT);

    Table expected = t(getHeader("right", rightColumns, "left", leftColumns), false, getData(columns, leftColumns, 1));
    validateDual(joinInfo.operator, joinInfo.clazz,
        new ManyColumnsGenerator(getTestAllocator(), "left", leftColumns, 1),
        new ManyColumnsGenerator(getTestAllocator(), "right", rightColumns, 1),
        DEFAULT_BATCH, expected);
  }
}
