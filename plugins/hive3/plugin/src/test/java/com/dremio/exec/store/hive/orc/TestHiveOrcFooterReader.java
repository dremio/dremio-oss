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
package com.dremio.exec.store.hive.orc;

import static org.junit.Assert.assertEquals;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.metadatarefresh.footerread.Footer;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.google.common.io.Resources;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/** Test class for Hive ORC Footer Reader */
public class TestHiveOrcFooterReader extends BaseTestQuery {

  @Test
  public void testOrcFooterReaderWithFileRead() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.STORE_ACCURATE_PARTITION_STATS, true)) {
      BatchSchema tableSchema =
          BatchSchema.of(
              Field.nullablePrimitive("col1", new ArrowType.PrimitiveType.Utf8()),
              Field.nullablePrimitive("col2", new ArrowType.PrimitiveType.Int(32, true)),
              Field.nullablePrimitive("col3", new ArrowType.PrimitiveType.Bool()));
      FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
      OperatorContext opCtx = getOpCtx();
      HiveOrcFooterReader reader = new HiveOrcFooterReader(tableSchema, fs, opCtx);
      Path fileRoot = Path.of(Resources.getResource("orc/").toURI());
      String file1 = fileRoot.resolve("testsample1.orc").toString();
      Footer footer = reader.getFooter(file1, 280L);
      assertEquals(1, footer.getRowCount());

      String file2 = fileRoot.resolve("testsample2.orc").toString();
      footer = reader.getFooter(file2, 372L);
      assertEquals(5, footer.getRowCount());
    }
  }

  @Test
  public void testOrcFooterReaderWithEstimate() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.STORE_ACCURATE_PARTITION_STATS, false)) {
      BatchSchema tableSchema =
          BatchSchema.of(
              Field.nullablePrimitive("col1", new ArrowType.PrimitiveType.Int(32, true)),
              Field.nullablePrimitive("col2", new ArrowType.PrimitiveType.Utf8()));
      OperatorContext opCtx = getOpCtx();
      FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

      long fileSize = 280L;
      double compressionFactor = 30f;
      int recordSize =
          tableSchema.estimateRecordSize(
              (int) opCtx.getOptions().getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE),
              (int) opCtx.getOptions().getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE));
      HiveOrcFooterReader reader = new HiveOrcFooterReader(tableSchema, fs, opCtx);
      Path fileRoot = Path.of(Resources.getResource("orc/").toURI());
      String file1 = fileRoot.resolve("testsample1.orc").toString();
      Footer footer = reader.getFooter(file1, fileSize);
      assertEquals(
          (long) Math.ceil(fileSize * compressionFactor / recordSize), footer.getRowCount());

      tableSchema =
          BatchSchema.of(
              Field.nullablePrimitive("col1", new ArrowType.PrimitiveType.Utf8()),
              Field.nullablePrimitive("col2", new ArrowType.PrimitiveType.Int(32, true)),
              Field.nullablePrimitive("col3", new ArrowType.PrimitiveType.Bool()));
      fileSize = 372L;
      recordSize =
          tableSchema.estimateRecordSize(
              (int) opCtx.getOptions().getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE),
              (int) opCtx.getOptions().getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE));
      String file2 = fileRoot.resolve("testsample2.orc").toString();
      reader = new HiveOrcFooterReader(tableSchema, fs, opCtx);
      footer = reader.getFooter(file2, fileSize);
      assertEquals(
          (long) Math.ceil(fileSize * compressionFactor / recordSize), footer.getRowCount());
    }
  }

  private OperatorContext getOpCtx() {
    SabotContext sabotContext = getSabotContext();
    return new OperatorContextImpl(
        sabotContext.getConfig(),
        sabotContext.getDremioConfig(),
        getTestAllocator(),
        sabotContext.getOptionManager(),
        10,
        sabotContext.getExpressionSplitCache());
  }
}
