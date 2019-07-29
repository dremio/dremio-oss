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

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.Expression;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;

/**
 * tests for DatasetStateMutator
 */
public class TestDatasetStateMutator {

  private final DatasetPath datasetPath = new DatasetPath("myspace.parentDS");
  private final From parentDataset = new FromTable(datasetPath.toPathString()).wrap();

  private final Expression value = new ExpColumnReference("parentFoo").wrap();
  private final Expression newValue = new ExpColumnReference("parentBar").wrap();

//  private Transformer t;
  private VirtualDatasetState state;

  @Before
  public void before() {
    state = new VirtualDatasetState()
        .setFrom(parentDataset);
    state.setColumnsList(asList(new Column("foo", value)));
  }

  private Expression getCol(String name, VirtualDatasetState dsState) {
    List<Column> columns = dsState.getColumnsList();
    for (Column column : columns) {
      if (column.getName().equals(name)) {
        return column.getValue();
      }
    }
    return null;
  }

  private void assertColIs(Expression expected, TransformResult result, String colName) {
    Expression col = getCol(colName, result.getNewState());
    if (expected == null) {
      assertNull(colName, col);
    } else {
      assertNotNull(colName, col);
      assertEquals(colName, expected.toString(), col.toString());
    }
  }

  private DatasetStateMutator mutator(boolean preview) {
    return new DatasetStateMutator("test_user", state, preview);
  }

  @Test
  public void testMutatorApplyNoDropNonPreview() {
    boolean preview = false;
    TransformResult result = mutator(preview).apply("foo", "foo2", newValue, false);
    assertEquals(newHashSet("foo2"), result.getAddedColumns());
    assertEquals(newHashSet(), result.getModifiedColumns());
    assertEquals(newHashSet(), result.getRemovedColumns());
    assertColIs(newValue, result, "foo2");
    assertColIs(value, result, "foo");
  }

  @Test
  public void testMutatorApplyReplaceNonPreview() {
    boolean preview = false;
    TransformResult result1 = mutator(preview).apply("foo", "foo", newValue, true);
    assertEquals(newHashSet(), result1.getAddedColumns());
    assertEquals(newHashSet("foo"), result1.getModifiedColumns());
    assertEquals(newHashSet(), result1.getRemovedColumns());
    assertColIs(null, result1, "foo2");
    assertColIs(newValue, result1, "foo");
  }

  @Test
  public void testMutatorApplyDropNonPreview() {
    boolean preview = false;
    TransformResult result2 = mutator(preview).apply("foo", "foo2", newValue, true);
    assertEquals(newHashSet("foo2"), result2.getAddedColumns());
    assertEquals(newHashSet(), result2.getModifiedColumns());
    assertEquals(newHashSet("foo"), result2.getRemovedColumns());
    assertColIs(null, result2, "foo");
    assertColIs(newValue, result2, "foo2");
  }

  @Test
  public void testMutatorApplyNoDropPreview() {
    boolean preview = true;
    TransformResult result = mutator(preview).apply("foo", "foo2", newValue, false);
    assertEquals(newHashSet("foo2"), result.getAddedColumns());
    assertEquals(newHashSet(), result.getModifiedColumns());
    assertEquals(newHashSet(), result.getRemovedColumns());
    assertColIs(newValue, result, "foo2");
    assertColIs(value, result, "foo");
  }

  @Test
  public void testMutatorApplyReplacePreview() {
    boolean preview = true;
    TransformResult result1 = mutator(preview).apply("foo", "foo", newValue, true);
    assertEquals(newHashSet("foo (new)"), result1.getAddedColumns());
    assertEquals(newHashSet(), result1.getModifiedColumns());
    assertEquals(newHashSet("foo"), result1.getRemovedColumns());
    assertColIs(null, result1, "foo2");
    assertColIs(value, result1, "foo");
    assertColIs(newValue, result1, "foo (new)");
  }

  @Test
  public void testMutatorApplyDropPreview() {
    boolean preview = true;
    TransformResult result2 = mutator(preview).apply("foo", "foo2", newValue, true);
    assertEquals(newHashSet("foo2"), result2.getAddedColumns());
    assertEquals(newHashSet(), result2.getModifiedColumns());
    assertEquals(newHashSet("foo"), result2.getRemovedColumns());
    assertColIs(value, result2, "foo");
    assertColIs(newValue, result2, "foo2");
  }

}
