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
package com.dremio.exec.planner.sql;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.google.common.collect.Sets;

public class TestSQLRefreshDataset {
  private final ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  @Test
  public void testAlterTableRefreshMetadataAllFiles() {
    final String sql = "REFRESH DATASET tbl FOR ALL FILES";
    final  SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final  SqlRefreshDataset sqlRefreshDataset = (SqlRefreshDataset) sqlNode;
    Assert.assertTrue(sqlRefreshDataset.getAllFilesRefresh().booleanValue());
  }

  @Test
  public void testAlterTableRefreshMetadataAllPartitions() {
    final String sql = "REFRESH DATASET tbl FOR ALL PARTITIONS";
    final  SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final  SqlRefreshDataset sqlRefreshDataset = (SqlRefreshDataset) sqlNode;
    Assert.assertTrue(sqlRefreshDataset.getAllPartitionsRefresh().booleanValue());
  }

  @Test
  public void testAlterTableRefreshMetadataAllFilesLazyUpdate() {
    final String sql = "REFRESH DATASET tbl FOR ALL FILES LAZY UPDATE";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshDataset sqlRefreshDataset = (SqlRefreshDataset) sqlNode;
    Assert.assertTrue(sqlRefreshDataset.getAllFilesRefresh().booleanValue());
    Assert.assertFalse(sqlRefreshDataset.getForceUpdate().booleanValue());
  }

  @Test
  public void testAlterTableRefreshMetadataAllPartitionsLazyUpdate() {
    final String sql = "REFRESH DATASET tbl FOR ALL PARTITIONS LAZY UPDATE";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshDataset sqlRefreshDataset = (SqlRefreshDataset) sqlNode;
    Assert.assertTrue(sqlRefreshDataset.getAllPartitionsRefresh().booleanValue());
    Assert.assertFalse(sqlRefreshDataset.getForceUpdate().booleanValue());
  }

  @Test(expected = UserException.class)
  public void testAlterTableRefreshMetadataForFilesEmptyList() {
    final String sql = "REFRESH DATASET tbl FOR FILES ()";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
  }

  @Test(expected = UserException.class)
  public void testAlterTableRefreshMetadataForPartitionsEmptyList() {
    final String sql = "REFRESH DATASET tbl FOR PARTITIONS ()";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
  }

  @Test
  public void testAlterTableRefreshMetadataForFiles_OneFile() {
    final String sql = "REFRESH DATASET tbl FOR FILES ('file1.json')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshDataset sqlRefreshDataset = (SqlRefreshDataset) sqlNode;
    Assert.assertTrue(sqlRefreshDataset.getFileRefresh().booleanValue());
    Assert.assertArrayEquals(new String[]{"file1.json"}, sqlRefreshDataset.getFileNames().toArray(new String[0]));
  }

  @Test
  public void testAlterTableRefreshMetadataForParitions_OnePartitionKey() {
    final String sql = "REFRESH DATASET tbl FOR PARTITIONS (\"year\" = '2021')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshDataset sqlRefreshDataset = (SqlRefreshDataset) sqlNode;
    Assert.assertTrue(sqlRefreshDataset.getPartitionRefresh().booleanValue());

    final Map<String, String> expectedPartition = new HashMap<>();
    expectedPartition.put("year", "2021");
    Assert.assertEquals(expectedPartition, sqlRefreshDataset.getPartition());
  }

  @Test
  public void testAlterTableRefreshMetadataForParitions_OnePartitionKeyValueNull() {
    final String sql = "REFRESH DATASET tbl FOR PARTITIONS (\"year\" = NULL)";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshDataset sqlRefreshDataset = (SqlRefreshDataset) sqlNode;
    Assert.assertTrue(sqlRefreshDataset.getPartitionRefresh().booleanValue());

    final Map<String, String> expectedPartition = new HashMap<>();
    expectedPartition.put("year", null);
    Assert.assertEquals(expectedPartition, sqlRefreshDataset.getPartition());
  }

  @Test
  public void testAlterTableRefreshMetadataForFiles_MultipleFiles() {
    final String sql = "REFRESH DATASET tbl FOR FILES ('file1.json', 'file2.json')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshDataset sqlRefreshDataset = (SqlRefreshDataset) sqlNode;
    Assert.assertTrue(sqlRefreshDataset.getFileRefresh().booleanValue());
    Assert.assertArrayEquals(new String[]{"file1.json", "file2.json"}, sqlRefreshDataset.getFileNames().toArray(new String[0]));
  }

  @Test
  public void testAlterTableRefreshMetadataForPartitions_MultipleFiles() {
    final String sql = "REFRESH DATASET tbl FOR PARTITIONS (\"year\" = '2021', \"month\" = 'Jan')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshDataset sqlRefreshDataset = (SqlRefreshDataset) sqlNode;
    Assert.assertTrue(sqlRefreshDataset.getPartitionRefresh().booleanValue());
    Assert.assertEquals(2, sqlRefreshDataset.getPartition().size());

    final Iterator<Map.Entry<String, String>> pairIterator = sqlRefreshDataset.getPartition().entrySet().iterator();
    Map.Entry<String, String> entry = pairIterator.next();
    Assert.assertEquals("year", entry.getKey());
    Assert.assertEquals("2021", entry.getValue());

    entry = pairIterator.next();
    Assert.assertEquals("month", entry.getKey());
    Assert.assertEquals("Jan", entry.getValue());
  }
}
