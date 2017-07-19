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
package com.dremio.sabot.op.sort.external;

import static com.dremio.sabot.op.sort.external.CustomGenerator.ID;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.exec.context.BufferManagerImpl;

public class TestSortOp extends BaseTestOperator {

  private BufferAllocator allocator;
  private BufferManager bufferManager;
  private CustomGenerator generator;
  private ClassProducer producer;

  @Before
  public void prepare() {
    allocator = getTestAllocator().newChildAllocator("test-memory-run", 0, 1_000_000);
    bufferManager = new BufferManagerImpl(allocator);
    producer = testContext.newClassProducer(bufferManager);
    generator = new CustomGenerator(1_000_000, getTestAllocator());
  }

  @After
  public void cleanup() throws Exception {
    AutoCloseables.close(allocator, bufferManager, generator);
  }

  @Test
  public void testSpillSort() throws Exception {
    ExternalSort sort = new ExternalSort(null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
    sort.setInitialAllocation(1_000_000); // this can't go below sort's initialAllocation (20K)
    sort.setMaxAllocation(2_000_000); // this can't go below sort's initialAllocation (20K)
    Fixtures.Table table = generator.getExpectedSortedTable();
    validateSingle(sort, ExternalSortOperator.class, generator, table, 1000);
  }
}
