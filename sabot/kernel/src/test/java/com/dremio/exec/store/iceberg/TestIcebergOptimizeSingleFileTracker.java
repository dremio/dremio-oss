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
package com.dremio.exec.store.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Tests for {@link IcebergOptimizeSingleFileTracker}
 */
public class TestIcebergOptimizeSingleFileTracker {

  private static final Schema TEST_SCHEMA = new Schema(
    Types.NestedField.optional(1, "n_nationkey", Types.IntegerType.get()),
    Types.NestedField.optional(2, "n_name", Types.StringType.get()),
    Types.NestedField.optional(3, "n_regionkey", Types.IntegerType.get()),
    Types.NestedField.optional(4, "n_comment", Types.StringType.get())
  );

  private static final String DATA_FILE_PATH_PREFIX = "s3://testdata/test_table/data/";

  private static final PartitionSpec SPEC_UNPARTITIONED = PartitionSpec.builderFor(TEST_SCHEMA).withSpecId(0).build();
  private static final PartitionSpec SPEC_PARTITIONED = PartitionSpec.builderFor(TEST_SCHEMA).withSpecId(1)
    .identity("n_nationkey").build();

  @Test
  public void testSingleDataFileInMultiplePartitions() {
    IcebergOptimizeSingleFileTracker tracker = new IcebergOptimizeSingleFileTracker();

    DataFile add1 = testDataFile(SPEC_PARTITIONED, 1);
    DataFile del1 = testDataFile(SPEC_PARTITIONED, 1);
    tracker.consumeAddDataFile(add1);
    tracker.consumeDeletedDataFile(del1);

    DataFile add2 = testDataFile(SPEC_PARTITIONED, 2);
    DataFile del2 = testDataFile(SPEC_PARTITIONED, 2);
    tracker.consumeAddDataFile(add2);
    tracker.consumeDeletedDataFile(del2);

    DataFile addUnpartitioned = testDataFile(SPEC_UNPARTITIONED, -1);
    DataFile delUnpartitioned = testDataFile(SPEC_UNPARTITIONED, -1);
    tracker.consumeAddDataFile(addUnpartitioned);
    tracker.consumeDeletedDataFile(delUnpartitioned);

    Set<DataFile> addDataFiles = Sets.newHashSet(add1, add2, addUnpartitioned);
    Set<DataFile> delDataFiles = Sets.newHashSet(del1, del2, delUnpartitioned);

    Set<String> removed = tracker.removeSingleFileChanges(addDataFiles, delDataFiles);

    assertThat(addDataFiles).isEmpty();
    assertThat(delDataFiles).isEmpty();

    assertThat(removed).contains(add1.path().toString(), add2.path().toString(), addUnpartitioned.path().toString());
  }

  @Test
  public void testSingleDataFilePartitionEvolved() {
    IcebergOptimizeSingleFileTracker tracker = new IcebergOptimizeSingleFileTracker();

    DataFile del1 = testDataFile(SPEC_UNPARTITIONED, -1);
    DataFile add1 = testDataFile(SPEC_PARTITIONED, 1);
    tracker.consumeAddDataFile(add1);
    tracker.consumeDeletedDataFile(del1);

    Set<DataFile> addDataFiles = Sets.newHashSet(add1);
    Set<DataFile> delDataFiles = Sets.newHashSet(del1);
    Set<String> removed = tracker.removeSingleFileChanges(addDataFiles, delDataFiles);

    assertThat(addDataFiles).contains(add1);
    assertThat(delDataFiles).contains(del1);
    assertThat(removed).isEmpty();
  }

  @Test
  public void testMultipleDataFilesRewrittenToSingle() {
    IcebergOptimizeSingleFileTracker tracker = new IcebergOptimizeSingleFileTracker();

    DataFile add1 = testDataFile(SPEC_PARTITIONED, 1);
    DataFile del1 = testDataFile(SPEC_PARTITIONED, 1);
    DataFile del2 = testDataFile(SPEC_PARTITIONED, 1);
    tracker.consumeAddDataFile(add1);
    tracker.consumeDeletedDataFile(del1);
    tracker.consumeDeletedDataFile(del2);

    Set<DataFile> addDataFiles = Sets.newHashSet(add1);
    Set<DataFile> delDataFiles = Sets.newHashSet(del1, del2);

    Set<String> removed = tracker.removeSingleFileChanges(addDataFiles, delDataFiles);

    assertThat(addDataFiles).contains(add1);
    assertThat(delDataFiles).contains(del1, del2);
    assertThat(removed).isEmpty();
  }

  @Test
  public void testSingleDataFileRewrittenToMultiple() {
    IcebergOptimizeSingleFileTracker tracker = new IcebergOptimizeSingleFileTracker();

    DataFile add1 = testDataFile(SPEC_PARTITIONED, 1);
    DataFile add2 = testDataFile(SPEC_PARTITIONED, 1);
    DataFile del1 = testDataFile(SPEC_PARTITIONED, 1);

    tracker.consumeAddDataFile(add1);
    tracker.consumeAddDataFile(add1);
    tracker.consumeDeletedDataFile(del1);

    Set<DataFile> addDataFiles = Sets.newHashSet(add1, add2);
    Set<DataFile> delDataFiles = Sets.newHashSet(del1);

    Set<String> removed = tracker.removeSingleFileChanges(addDataFiles, delDataFiles);

    assertThat(addDataFiles).contains(add1, add2);
    assertThat(delDataFiles).contains(del1);
    assertThat(removed).isEmpty();
  }

  @Test
  public void testMultipleDataFiles() {
    IcebergOptimizeSingleFileTracker tracker = new IcebergOptimizeSingleFileTracker();

    DataFile add1 = testDataFile(SPEC_PARTITIONED, 1);
    DataFile add2 = testDataFile(SPEC_PARTITIONED, 1);
    DataFile del1 = testDataFile(SPEC_PARTITIONED, 1);
    DataFile del2 = testDataFile(SPEC_PARTITIONED, 1);

    tracker.consumeAddDataFile(add1);
    tracker.consumeAddDataFile(add2);
    tracker.consumeDeletedDataFile(del1);
    tracker.consumeDeletedDataFile(del2);

    DataFile add3 = testDataFile(SPEC_PARTITIONED, 2);
    DataFile del3 = testDataFile(SPEC_PARTITIONED, 2);
    tracker.consumeAddDataFile(add3);
    tracker.consumeDeletedDataFile(del3);

    Set<DataFile> addDataFiles = Sets.newHashSet(add1, add2, add3);
    Set<DataFile> delDataFiles = Sets.newHashSet(del1, del2, del3);

    Set<String> removed = tracker.removeSingleFileChanges(addDataFiles, delDataFiles);

    assertThat(addDataFiles).contains(add1, add2).doesNotContain(add3);
    assertThat(delDataFiles).contains(del1, del2).doesNotContain(del3);
    assertThat(removed).containsExactly(add3.path().toString());
  }

  public DataFile testDataFile(PartitionSpec spec, int partitionVal) {
    DataFiles.Builder builder = DataFiles.builder(spec);
    if (spec.isPartitioned()) {
      builder.withPartitionPath("n_nationkey=" + partitionVal);
    }

    return builder
      .withInputFile(new MockInputFile())
      .withRecordCount(25)
      .withFormat(FileFormat.PARQUET)
      .build();
  }

  class MockInputFile implements InputFile {
    private final String location;

    public MockInputFile() {
      this.location = DATA_FILE_PATH_PREFIX + UUID.randomUUID() + ".parquet"; // give a random path
    }

    @Override
    public long getLength() {
      return 873;
    }

    @Override
    public SeekableInputStream newStream() {
      throw new NotImplementedException();
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return true;
    }
  }
}
