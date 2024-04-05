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
package com.dremio.exec.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.hadoop.HadoopFileSystem.FetchOnDemandDirectoryStream;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingRecordReader;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestDirListingRecordReader extends BaseTestQuery {

  static final long START_TIME = 10000;
  private static BufferAllocator testAllocator;
  private SampleMutator mutator;
  private RecordReader reader;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setup() throws Exception {
    testAllocator = allocatorRule.newAllocator("test-DirListReader-allocator", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() throws Exception {
    mutator.close();
    reader.close();
    testAllocator.close();
  }

  private OperatorContext getCtx() {
    return getCtx(true);
  }

  private OperatorContext getCtx(boolean excludeFutureModTimes) {
    OperatorContext operatorContext = mock(OperatorContext.class, RETURNS_DEEP_STUBS);
    when(operatorContext.getAllocator()).thenReturn(testAllocator);
    when(operatorContext.getTargetBatchSize()).thenReturn(4000);
    when(operatorContext.getMinorFragmentEndpoints())
        .thenReturn(ImmutableList.of(new MinorFragmentEndpoint(0, null)));
    when(operatorContext.getFunctionContext().getContextInformation().getQueryStartTime())
        .thenReturn(START_TIME);
    when(operatorContext.getOptions().getOption(ExecConstants.DIR_LISTING_EXCLUDE_FUTURE_MOD_TIMES))
        .thenReturn(excludeFutureModTimes);
    return operatorContext;
  }

  public static OperatorStats getOperatorStats(BufferAllocator testAllocator) {
    OpProfileDef prof = new OpProfileDef(1, 1, 1);
    final OperatorStats operatorStats = new OperatorStats(prof, testAllocator);
    return operatorStats;
  }

  private FileSystem setUpFs() throws RuntimeException, IOException {
    HadoopFileSystem fs = mock(HadoopFileSystem.class);
    return fs;
  }

  private interface RemoteIteratorWrapper extends RemoteIterator<LocatedFileStatus> {
    boolean isClosed();
  }

  private static FetchOnDemandDirectoryStream newRemoteIterator(
      Path path, BufferAllocator testAllocator, final FileStatus... statuses) {
    final Iterator<FileStatus> iterator = Arrays.asList(statuses).iterator();
    final AtomicBoolean closed = new AtomicBoolean(false);

    RemoteIteratorWrapper wrapper =
        new RemoteIteratorWrapper() {
          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public LocatedFileStatus next() throws IOException {
            return new LocatedFileStatus(iterator.next(), null);
          }

          @Override
          public boolean isClosed() {
            return closed.get();
          }
        };

    return new FetchOnDemandDirectoryStream(wrapper, path, getOperatorStats(testAllocator));
  }

  private static org.apache.hadoop.fs.Path toHadoopPath(Path path) {
    return new org.apache.hadoop.fs.Path(path.toURI());
  }

  private static DirListInputSplitProto.DirListInputSplit getDirListInputSplit(
      String operatingPath, String rootPath) {
    return DirListInputSplitProto.DirListInputSplit.newBuilder()
        .setOperatingPath(operatingPath)
        .setReadSignature(Long.MAX_VALUE)
        .setRootPath(rootPath)
        .build();
  }

  private void setupMutator() {
    mutator = new SampleMutator(testAllocator);

    List<Field> children =
        Arrays.asList(CompleteType.VARCHAR.toField("key"), CompleteType.VARCHAR.toField("value"));

    Field partInfo =
        CompleteType.VARBINARY.toField(
            MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.PARTITION_INFO);
    Field filePath =
        CompleteType.VARCHAR.toField(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    Field mtime =
        CompleteType.BIGINT.toField(
            MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.MODIFICATION_TIME);
    Field size =
        CompleteType.BIGINT.toField(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_SIZE);

    mutator.addField(filePath, VarCharVector.class);
    mutator.addField(partInfo, VarBinaryVector.class);
    mutator.addField(mtime, BigIntVector.class);
    mutator.addField(size, BigIntVector.class);

    mutator.getContainer().buildSchema();
  }

  protected static void setupFsListIteratorMock(HadoopFileSystem fs, Path inputPath)
      throws IOException {

    /*FS Structure

    // foo.parquet
    // bar/
    //   file1.parquet
    //   subBar1/
    //     file2.parquet
    //     file3.parquet
    //     file4.parquet
    //     subBar2/
    //        file5.parquet
    //
    */

    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(Path.of(inputPath.resolve("foo.parquet").toString())), // File
      toHadoopPath(Path.of(inputPath.resolve("bar").toString())), // Dir
      toHadoopPath(Path.of(inputPath.resolve("bar/file1.parquet").toString())), // File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1").toString())), // Dir1/Dir2
      toHadoopPath(
          Path.of(inputPath.resolve("bar/subBar1/file2.parquet").toString())), // Dir1/Dir2/File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/subBar2").toString())), // Dir1/Dir2/Dir3
      toHadoopPath(
          Path.of(inputPath.resolve("bar/subBar1/file3.parquet").toString())), // Dir1/Dir2/File
      toHadoopPath(
          Path.of(inputPath.resolve("bar/subBar1/file4.parquet").toString())), // Dir1/Dir2/File
      toHadoopPath(
          Path.of(
              inputPath
                  .resolve("bar/subBar1/subBar2/file5.parquet")
                  .toString())), // Dir1/Dir2/Dir3/File
    };

    org.apache.hadoop.fs.Path[] relativePathToSubBar = {
      toHadoopPath(
          Path.of(inputPath.resolve("bar/subBar1/file2.parquet").toString())), // Dir1/Dir2/File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/subBar2").toString())), // Dir1/Dir2/Dir3
      toHadoopPath(
          Path.of(inputPath.resolve("bar/subBar1/file3.parquet").toString())), // Dir1/Dir2/File
      toHadoopPath(
          Path.of(inputPath.resolve("bar/subBar1/file4.parquet").toString())), // Dir1/Dir2/File
      toHadoopPath(
          Path.of(
              inputPath
                  .resolve("bar/subBar1/subBar2/file5.parquet")
                  .toString())), // Dir1/Dir2/Dir3/File
    };

    FetchOnDemandDirectoryStream statusesIterator1 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]),
            new FileStatus(
                40,
                true,
                0,
                0,
                32,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[1]),
            new FileStatus(
                70,
                false,
                0,
                0,
                31,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[2]),
            new FileStatus(
                1010,
                true,
                0,
                0,
                33,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[3]),
            new FileStatus(
                1200,
                false,
                0,
                0,
                32,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[4]),
            new FileStatus(
                400,
                true,
                0,
                0,
                13,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[5]),
            new FileStatus(
                1200,
                false,
                0,
                0,
                312,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[6]),
            new FileStatus(
                1400,
                false,
                0,
                0,
                331,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[7]),
            new FileStatus(
                1320,
                false,
                0,
                0,
                START_TIME + 1,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[8]));

    FetchOnDemandDirectoryStream statusesIterator2 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                relativePathToSubBar[0]),
            new FileStatus(
                40,
                true,
                0,
                0,
                32,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                relativePathToSubBar[1]),
            new FileStatus(
                70,
                false,
                0,
                0,
                31,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                relativePathToSubBar[2]),
            new FileStatus(
                1010,
                false,
                0,
                0,
                33,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                relativePathToSubBar[3]),
            new FileStatus(
                1200,
                false,
                0,
                0,
                32,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                relativePathToSubBar[4]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator1);

    Path relativeToBar = inputPath.resolve(Path.of("bar"));
    when(fs.listFiles(relativeToBar, false)).thenReturn(statusesIterator2);
  }

  private void setupFsListIteratorMockWithHiddenFiles(HadoopFileSystem fs, Path inputPath)
      throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(
          Path.of(inputPath.resolve("bar/subBar1/file2.parquet").toString())), // Dir1/Dir2/File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/.subBar2").toString())), // Dir1/Dir2/Dir3
      toHadoopPath(
          Path.of(inputPath.resolve("bar/subBar1/file3.parquet").toString())), // Dir1/Dir2/File
      toHadoopPath(
          Path.of(inputPath.resolve("bar/subBar1/_file4.parquet").toString())), // Dir1/Dir2/File
      toHadoopPath(
          Path.of(
              inputPath
                  .resolve("bar/subBar1/.subBar2/file5.parquet")
                  .toString())), // Dir1/Dir2/Dir3/File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/_subBar3").toString())), // Dir1/Dir2/Dir3
      toHadoopPath(
          Path.of(
              inputPath
                  .resolve("bar/subBar1/_subBar3/file5.parquet")
                  .toString())), // Dir1/Dir2/Dir3/File
    };

    FetchOnDemandDirectoryStream statusesIterator1 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]),
            new FileStatus(
                40,
                true,
                0,
                0,
                32,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[1]),
            new FileStatus(
                70,
                false,
                0,
                0,
                31,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[2]),
            new FileStatus(
                1010,
                false,
                0,
                0,
                33,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[3]),
            new FileStatus(
                1200,
                false,
                0,
                0,
                32,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[4]),
            new FileStatus(
                40,
                true,
                0,
                0,
                32,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[5]),
            new FileStatus(
                70,
                false,
                0,
                0,
                31,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[6]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator1);
  }

  public static void setupFsListIteratorMockWithPartitions(HadoopFileSystem fs, Path inputPath)
      throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(Path.of(inputPath.resolve("id=1/data=name/file1.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=1/data=name/file2.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=1/data=name/file3.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=2/data=value/file4.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=2/data=value/file5.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=2/data=value/file6.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=2/data=1234/file7.parquet").toString())),
    };

    FetchOnDemandDirectoryStream statusesIterator1 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]),
            new FileStatus(
                40,
                false,
                0,
                0,
                2,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[1]),
            new FileStatus(
                70,
                false,
                0,
                0,
                3,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[2]),
            new FileStatus(
                1010,
                false,
                0,
                0,
                4,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[3]),
            new FileStatus(
                1200,
                false,
                0,
                0,
                5,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[4]),
            new FileStatus(
                40,
                false,
                0,
                0,
                6,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[5]),
            new FileStatus(
                70,
                false,
                0,
                0,
                7,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[6]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator1);
  }

  private void setupFsListIteratorMockInvalidPartitions(HadoopFileSystem fs, Path inputPath)
      throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(Path.of(inputPath.resolve("id=1/data=name/file1.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id/data=name/file2.parquet").toString())),
    };

    FetchOnDemandDirectoryStream statusesIterator1 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]),
            new FileStatus(
                40,
                false,
                0,
                0,
                2,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[1]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator1);
  }

  private void setupFsListIteratorMockForNullPartitions(HadoopFileSystem fs, Path inputPath)
      throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(Path.of(inputPath.resolve("id=1/data=name/file1.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=/data=/file2.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=/data=null/file2.parquet").toString())),
      toHadoopPath(
          Path.of(
              inputPath.resolve("id=/data=__HIVE_DEFAULT_PARTITION__/file2.parquet").toString())),
    };

    FetchOnDemandDirectoryStream statusesIterator1 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]),
            new FileStatus(
                40,
                false,
                0,
                0,
                2,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[1]),
            new FileStatus(
                40,
                false,
                0,
                0,
                3,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[2]),
            new FileStatus(
                40,
                false,
                0,
                0,
                4,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[3]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator1);
  }

  private void setupFsListIteratorMockWithLargeFiles(HadoopFileSystem fs, Path inputPath)
      throws IOException {

    /*FS Structure
    // bar/
    //   file1.parquet
    //   file2.parquet
    //   ...
    //   file200.parquet
    //   subBar/
    //     file201.parquet
    //     file202.parquet
    //     ...
    //     file400.parquet
    //
    */

    FileStatus[] statuses = new FileStatus[404];

    statuses[0] =
        new FileStatus(
            20,
            true,
            1,
            4096,
            1,
            2,
            FsPermission.getFileDefault(),
            "testowner",
            "testgroup",
            toHadoopPath(Path.of(inputPath.resolve("bar").toString())));

    for (int i = 1; i <= 200; i++) {
      org.apache.hadoop.fs.Path hadoopPath =
          toHadoopPath(Path.of(inputPath.resolve("bar/file" + i + ".parquet").toString()));
      statuses[i] =
          new FileStatus(
              20,
              false,
              1,
              4096,
              1,
              2,
              FsPermission.getFileDefault(),
              "testowner",
              "testgroup",
              hadoopPath);
    }

    statuses[201] =
        new FileStatus(
            20,
            true,
            1,
            4096,
            1,
            2,
            FsPermission.getFileDefault(),
            "testowner",
            "testgroup",
            toHadoopPath(Path.of(inputPath.resolve("bar/subBar").toString())));

    statuses[202] =
        new FileStatus(
            20,
            true,
            1,
            4096,
            1,
            2,
            FsPermission.getFileDefault(),
            "testowner",
            "testgroup",
            toHadoopPath(Path.of(inputPath.resolve("subBar").toString())));

    for (int i = 203; i <= 403; i++) {
      org.apache.hadoop.fs.Path hadoopPath =
          toHadoopPath(
              Path.of(inputPath.resolve("bar/subBar/file" + (i - 2) + ".parquet").toString()));
      statuses[i] =
          new FileStatus(
              20,
              false,
              1,
              4096,
              1,
              2,
              FsPermission.getFileDefault(),
              "testowner",
              "testgroup",
              hadoopPath);
    }

    when(fs.listFiles(inputPath, true))
        .thenReturn(newRemoteIterator(inputPath, testAllocator, statuses));
  }

  @Test
  public void testDirListReaderForFileSystemPartitionIsRecursive() throws Exception {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();

    setupMutator();
    setupFsListIteratorMock(fs, inputPath);
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());
    reader = new DirListingRecordReader(getCtx(), fs, split, true, null, null, true, false);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    int generatedRecords = reader.next();
    assertEquals(generatedRecords, 5);

    Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
    VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");
    VarBinaryVector outputPartInfo = (VarBinaryVector) fieldVectorMap.get("partitioninfo");
    BigIntVector mtime = (BigIntVector) fieldVectorMap.get("modificationtime");
    BigIntVector size = (BigIntVector) fieldVectorMap.get("filesize");

    assertEquals(outputpaths.getObject(0).toString(), "/randompath/foo.parquet?version=1");
    assertEquals(extractPartitionData(outputPartInfo.getObject(0)), "PartitionData{}");
    assertEquals(mtime.get(0), 1);
    assertEquals(size.get(0), 20);

    assertEquals(outputpaths.getObject(1).toString(), "/randompath/bar/file1.parquet?version=31");
    assertEquals(extractPartitionData(outputPartInfo.getObject(1)), "PartitionData{dir0=bar}");
    assertEquals(mtime.get(1), 31);
    assertEquals(size.get(1), 70);

    assertEquals(
        outputpaths.getObject(2).toString(), "/randompath/bar/subBar1/file2.parquet?version=32");
    assertEquals(
        extractPartitionData(outputPartInfo.getObject(2)), "PartitionData{dir0=bar, dir1=subBar1}");
    assertEquals(mtime.get(2), 32);
    assertEquals(size.get(2), 1200);

    assertEquals(
        outputpaths.getObject(3).toString(), "/randompath/bar/subBar1/file3.parquet?version=312");
    assertEquals(
        extractPartitionData(outputPartInfo.getObject(3)), "PartitionData{dir0=bar, dir1=subBar1}");
    assertEquals(mtime.get(3), 312);
    assertEquals(size.get(3), 1200);

    assertEquals(
        outputpaths.getObject(4).toString(), "/randompath/bar/subBar1/file4.parquet?version=331");
    assertEquals(
        extractPartitionData(outputPartInfo.getObject(4)), "PartitionData{dir0=bar, dir1=subBar1}");
    assertEquals(mtime.get(4), 331);
    assertEquals(size.get(4), 1400);
  }

  @Test
  public void testDirListReaderForHivePartitionIsRecursive()
      throws IOException, ExecutionSetupException, ClassNotFoundException {
    Path inputPath = Path.of("/hivePath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMock(fs, inputPath);

    setupMutator();

    BatchSchema tableSchema =
        BatchSchema.of(
            CompleteType.INT.toField("integerCol"),
            CompleteType.DOUBLE.toField("doubleCol"),
            CompleteType.BIT.toField("bitField"),
            CompleteType.VARCHAR.toField("varCharField"));

    List<PartitionProtobuf.PartitionValue> partitionValues = new ArrayList<>();
    partitionValues.add(
        PartitionProtobuf.PartitionValue.newBuilder()
            .setColumn("integerCol")
            .setIntValue(20)
            .build());
    partitionValues.add(
        PartitionProtobuf.PartitionValue.newBuilder()
            .setColumn("doubleCol")
            .setDoubleValue(20.0D)
            .build());
    partitionValues.add(
        PartitionProtobuf.PartitionValue.newBuilder()
            .setColumn("bitField")
            .setBitValue(true)
            .build());
    partitionValues.add(
        PartitionProtobuf.PartitionValue.newBuilder()
            .setColumn("varCharField")
            .setStringValue("tempVarCharValue")
            .build());

    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());
    reader =
        new DirListingRecordReader(
            getCtx(), fs, split, true, tableSchema, partitionValues, false, false);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    int generatedRecords = reader.next();

    Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
    VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");
    VarBinaryVector outputPartInfo = (VarBinaryVector) fieldVectorMap.get("partitioninfo");
    BigIntVector mtime = (BigIntVector) fieldVectorMap.get("modificationtime");
    BigIntVector size = (BigIntVector) fieldVectorMap.get("filesize");

    assertEquals(generatedRecords, 5);

    String partInfo =
        "PartitionData{integerCol=20, doubleCol=20.0, bitField=true, varCharField=tempVarCharValue}";

    assertEquals(outputpaths.getObject(0).toString(), "/hivePath/foo.parquet?version=1");
    assertEquals(extractPartitionData(outputPartInfo.get(0)), partInfo);
    assertEquals(mtime.get(0), 1);
    assertEquals(size.get(0), 20);

    assertEquals(outputpaths.getObject(1).toString(), "/hivePath/bar/file1.parquet?version=31");
    assertEquals(extractPartitionData(outputPartInfo.get(1)), partInfo);
    assertEquals(mtime.get(1), 31);
    assertEquals(size.get(1), 70);

    assertEquals(
        outputpaths.getObject(2).toString(), "/hivePath/bar/subBar1/file2.parquet?version=32");
    assertEquals(extractPartitionData(outputPartInfo.get(2)), partInfo);
    assertEquals(mtime.get(2), 32);
    assertEquals(size.get(2), 1200);

    assertEquals(
        outputpaths.getObject(3).toString(), "/hivePath/bar/subBar1/file3.parquet?version=312");
    assertEquals(extractPartitionData(outputPartInfo.get(3)), partInfo);
    assertEquals(mtime.get(3), 312);
    assertEquals(size.get(3), 1200);

    assertEquals(
        outputpaths.getObject(4).toString(), "/hivePath/bar/subBar1/file4.parquet?version=331");
    assertEquals(extractPartitionData(outputPartInfo.get(4)), partInfo);
    assertEquals(mtime.get(4), 331);
    assertEquals(size.get(4), 1400);
  }

  @Test
  public void testDirListReaderWithLargeFilesInDirectory()
      throws IOException, ExecutionSetupException {
    Path inputPath = Path.of("/hivePath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockWithLargeFiles(fs, inputPath);

    OperatorContext context = getCtx();
    when(context.getTargetBatchSize()).thenReturn(500);

    setupMutator();
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());
    reader = new DirListingRecordReader(context, fs, split, true, null, null, true, false);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    ((DirListingRecordReader) reader).initIncrementalBatchSize(50);

    int noRecordsRead = reader.next();

    assertEquals(noRecordsRead, 50);
    Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
    VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");

    assertEquals(outputpaths.getValueCount(), 50);
    assertEquals(outputpaths.getObject(0).toString(), "/hivePath/bar/file1.parquet?version=1");
    assertEquals(outputpaths.getObject(1).toString(), "/hivePath/bar/file2.parquet?version=1");
    assertEquals(outputpaths.getObject(9).toString(), "/hivePath/bar/file10.parquet?version=1");
    assertEquals(outputpaths.getObject(49).toString(), "/hivePath/bar/file50.parquet?version=1");

    // Reset the vectors for the second batch
    mutator.close();
    setupMutator();

    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    noRecordsRead = reader.next();
    assertEquals(noRecordsRead, 100);

    fieldVectorMap = mutator.getFieldVectorMap();
    outputpaths = (VarCharVector) fieldVectorMap.get("filepath");

    assertEquals(outputpaths.getValueCount(), 100);
    assertEquals(outputpaths.getObject(0).toString(), "/hivePath/bar/file51.parquet?version=1");
    assertEquals(outputpaths.getObject(1).toString(), "/hivePath/bar/file52.parquet?version=1");
    assertEquals(outputpaths.getObject(99).toString(), "/hivePath/bar/file150.parquet?version=1");

    // Reset the vectors for the third batch
    mutator.close();
    setupMutator();

    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    noRecordsRead = reader.next();
    assertEquals(noRecordsRead, 200);

    fieldVectorMap = mutator.getFieldVectorMap();
    outputpaths = (VarCharVector) fieldVectorMap.get("filepath");

    assertEquals(outputpaths.getValueCount(), 200);
    assertEquals(outputpaths.getObject(0).toString(), "/hivePath/bar/file151.parquet?version=1");
    assertEquals(outputpaths.getObject(1).toString(), "/hivePath/bar/file152.parquet?version=1");
    assertEquals(
        outputpaths.getObject(50).toString(), "/hivePath/bar/subBar/file201.parquet?version=1");
    assertEquals(
        outputpaths.getObject(199).toString(), "/hivePath/bar/subBar/file350.parquet?version=1");
  }

  @Test
  public void testDirListReaderWithDifferentOperatingAndRootPath() throws Exception {
    Path rootPath = Path.of("/randompath/");
    Path operatingPath = Path.of("/randompath/bar");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();

    try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      setupMutator();
      setupFsListIteratorMock(fs, rootPath);

      DirListInputSplitProto.DirListInputSplit split =
          getDirListInputSplit(operatingPath.toString(), rootPath.toString());
      reader = new DirListingRecordReader(getCtx(), fs, split, false, null, null, true, false);
      reader.allocate(mutator.getFieldVectorMap());
      reader.setup(mutator);

      closer.addAll(reader);
      closer.addAll(mutator);

      int generatedRecords = reader.next();
      assertEquals(generatedRecords, 4);

      Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
      VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");
      VarBinaryVector outputPartInfo = (VarBinaryVector) fieldVectorMap.get("partitioninfo");
      BigIntVector mtime = (BigIntVector) fieldVectorMap.get("modificationtime");
      BigIntVector size = (BigIntVector) fieldVectorMap.get("filesize");

      assertEquals(
          outputpaths.getObject(0).toString(), "/randompath/bar/subBar1/file2.parquet?version=1");
      assertEquals(
          extractPartitionData(outputPartInfo.getObject(0)),
          "PartitionData{dir0=bar, dir1=subBar1}");
      assertEquals(mtime.get(0), 1);
      assertEquals(size.get(0), 20);

      assertEquals(
          outputpaths.getObject(1).toString(), "/randompath/bar/subBar1/file3.parquet?version=31");
      assertEquals(
          extractPartitionData(outputPartInfo.getObject(1)),
          "PartitionData{dir0=bar, dir1=subBar1}");
      assertEquals(mtime.get(1), 31);
      assertEquals(size.get(1), 70);

      assertEquals(
          outputpaths.getObject(2).toString(), "/randompath/bar/subBar1/file4.parquet?version=33");
      assertEquals(
          extractPartitionData(outputPartInfo.getObject(2)),
          "PartitionData{dir0=bar, dir1=subBar1}");
      assertEquals(mtime.get(2), 33);
      assertEquals(size.get(2), 1010);
    }
  }

  @Test
  public void testHiddenFiles() throws Exception {
    Path inputPath = Path.of("/hivePath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockWithHiddenFiles(fs, inputPath);

    OperatorContext context = getCtx();
    when(context.getTargetBatchSize()).thenReturn(100);

    setupMutator();
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());
    reader = new DirListingRecordReader(context, fs, split, true, null, null, true, false);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    int noRecordsRead = reader.next();

    assertEquals(2, noRecordsRead);
    Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
    VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");

    assertEquals(2, outputpaths.getValueCount());
    assertEquals(
        outputpaths.getObject(0).toString(), "/hivePath/bar/subBar1/file2.parquet?version=1");
    assertEquals(
        outputpaths.getObject(1).toString(), "/hivePath/bar/subBar1/file3.parquet?version=31");

    // Reset the vectors for the second batch
    mutator.close();
  }

  private String extractPartitionData(byte[] partitionInfoBytes)
      throws IOException, ClassNotFoundException {
    java.io.ByteArrayInputStream fis = new java.io.ByteArrayInputStream(partitionInfoBytes);
    java.io.ObjectInputStream ois = new java.io.ObjectInputStream(fis);
    IcebergPartitionData partitionData = (IcebergPartitionData) ois.readObject();
    return partitionData.toString();
  }

  @Test
  public void testDirListingParserPartitionPaths() throws Exception {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockWithPartitions(fs, inputPath);

    OperatorContext context = getCtx();
    setupMutator();
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());

    reader = new DirListingRecordReader(context, fs, split, true, null, null, true, true);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    int noRecordsRead = reader.next();

    assertEquals(7, noRecordsRead);
    Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
    VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");
    VarBinaryVector outputPartInfo = (VarBinaryVector) fieldVectorMap.get("partitioninfo");

    assertEquals(7, outputpaths.getValueCount());

    String partInfo = "";

    assertEquals(
        outputpaths.getObject(0).toString(), "/randompath/id=1/data=name/file1.parquet?version=1");
    partInfo = "PartitionData{id=1, data=name, dir0=id=1, dir1=data=name}";
    assertEquals(extractPartitionData(outputPartInfo.get(0)), partInfo);

    assertEquals(
        outputpaths.getObject(1).toString(), "/randompath/id=1/data=name/file2.parquet?version=2");
    partInfo = "PartitionData{id=1, data=name, dir0=id=1, dir1=data=name}";
    assertEquals(extractPartitionData(outputPartInfo.get(1)), partInfo);

    assertEquals(
        outputpaths.getObject(2).toString(), "/randompath/id=1/data=name/file3.parquet?version=3");
    partInfo = "PartitionData{id=1, data=name, dir0=id=1, dir1=data=name}";
    assertEquals(extractPartitionData(outputPartInfo.get(2)), partInfo);

    assertEquals(
        outputpaths.getObject(5).toString(), "/randompath/id=2/data=value/file6.parquet?version=6");
    partInfo = "PartitionData{id=2, data=value, dir0=id=2, dir1=data=value}";
    assertEquals(extractPartitionData(outputPartInfo.get(5)), partInfo);

    assertEquals(
        outputpaths.getObject(6).toString(), "/randompath/id=2/data=1234/file7.parquet?version=7");
    partInfo = "PartitionData{id=2, data=1234, dir0=id=2, dir1=data=1234}";
    assertEquals(extractPartitionData(outputPartInfo.get(6)), partInfo);

    // Reset the vectors for the second batch
    mutator.close();
  }

  @Test
  public void testDirListingParserPartitionPathsWithNull()
      throws IOException, ExecutionSetupException, ClassNotFoundException {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockForNullPartitions(fs, inputPath);

    OperatorContext context = getCtx();
    setupMutator();
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());

    reader = new DirListingRecordReader(context, fs, split, true, null, null, true, true);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    int noRecordsRead = reader.next();

    assertEquals(4, noRecordsRead);
    Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
    VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");
    VarBinaryVector outputPartInfo = (VarBinaryVector) fieldVectorMap.get("partitioninfo");

    assertEquals(4, outputpaths.getValueCount());

    String partInfo = "";

    assertEquals(
        outputpaths.getObject(0).toString(), "/randompath/id=1/data=name/file1.parquet?version=1");
    partInfo = "PartitionData{id=1, data=name, dir0=id=1, dir1=data=name}";
    assertEquals(extractPartitionData(outputPartInfo.get(0)), partInfo);

    assertEquals(
        outputpaths.getObject(1).toString(), "/randompath/id=/data=/file2.parquet?version=2");
    partInfo = "PartitionData{id=null, data=null, dir0=id=, dir1=data=}";
    assertEquals(extractPartitionData(outputPartInfo.get(1)), partInfo);

    assertEquals(
        outputpaths.getObject(2).toString(), "/randompath/id=/data=null/file2.parquet?version=3");
    partInfo = "PartitionData{id=null, data=null, dir0=id=, dir1=data=null}";
    assertEquals(extractPartitionData(outputPartInfo.get(2)), partInfo);

    assertEquals(
        outputpaths.getObject(3).toString(),
        "/randompath/id=/data=__HIVE_DEFAULT_PARTITION__/file2.parquet?version=4");
    partInfo = "PartitionData{id=null, data=null, dir0=id=, dir1=data=__HIVE_DEFAULT_PARTITION__}";
    assertEquals(extractPartitionData(outputPartInfo.get(3)), partInfo);

    // Reset the vectors for the second batch
    mutator.close();
  }

  private void setupFsListIteratorMockForInvalidPartitions1(HadoopFileSystem fs, Path inputPath)
      throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(Path.of(inputPath.resolve("id=1/data=name/file1.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id/data=value/file2.parquet").toString())),
    };

    FetchOnDemandDirectoryStream statusesIterator1 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]),
            new FileStatus(
                40,
                false,
                0,
                0,
                2,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[1]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator1);
  }

  @Test(expected = UserException.class)
  // First path has id=1/data=name and second path has id/data=value so the second path doesn't have
  // id= so error
  public void testDirListingParserPartitionPathsError1()
      throws IOException, ExecutionSetupException {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockForInvalidPartitions1(fs, inputPath);

    OperatorContext context = getCtx();
    setupMutator();
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());

    reader = new DirListingRecordReader(context, fs, split, true, null, null, true, true);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    try {
      reader.next();
    } catch (Exception e) {
      assertTrue(e instanceof UserException);
      assertEquals(e.getMessage(), "Failed to list files of directory /randompath/");
      assertEquals(
          e.getCause().getMessage(),
          "All the directories should have = in the partition structure. Path /randompath/id/data=value/file2.parquet");
      throw e;
    }
    // Reset the vectors for the second batch
    mutator.close();
  }

  @Test
  public void testDirListReaderCanIncludeFilesWithFutureModTimes() throws Exception {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();

    setupMutator();
    setupFsListIteratorMock(fs, inputPath);
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());
    reader = new DirListingRecordReader(getCtx(false), fs, split, true, null, null, true, false);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    int generatedRecords = reader.next();
    assertEquals(6, generatedRecords);

    Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
    VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");

    assertEquals("/randompath/foo.parquet?version=1", outputpaths.getObject(0).toString());
    assertEquals("/randompath/bar/file1.parquet?version=31", outputpaths.getObject(1).toString());
    assertEquals(
        "/randompath/bar/subBar1/file2.parquet?version=32", outputpaths.getObject(2).toString());
    assertEquals(
        "/randompath/bar/subBar1/file3.parquet?version=312", outputpaths.getObject(3).toString());
    assertEquals(
        "/randompath/bar/subBar1/file4.parquet?version=331", outputpaths.getObject(4).toString());
    assertEquals(
        "/randompath/bar/subBar1/subBar2/file5.parquet?version=10001",
        outputpaths.getObject(5).toString());
  }

  private void setupFsListIteratorMockForInvalidPartitions2(HadoopFileSystem fs, Path inputPath)
      throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(Path.of(inputPath.resolve("id=1/data=name/file1.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("otherColumn=1/data=value/file2.parquet").toString())),
    };

    FetchOnDemandDirectoryStream statusesIterator1 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]),
            new FileStatus(
                40,
                false,
                0,
                0,
                2,
                4,
                FsPermission.getDirDefault(),
                "testowner",
                "testgroup",
                testPaths[1]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator1);
  }

  @Test(expected = UserException.class)
  // First path has id=1/data=name and second path has otherColumn=1/data=value so the column name
  // changed
  public void testDirListingParserPartitionPathsError2()
      throws IOException, ExecutionSetupException {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockForInvalidPartitions2(fs, inputPath);

    OperatorContext context = getCtx();
    setupMutator();
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());

    reader = new DirListingRecordReader(context, fs, split, true, null, null, true, true);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    try {
      reader.next();
    } catch (Exception e) {
      assertTrue(e instanceof UserException);
      assertEquals(e.getMessage(), "Failed to list files of directory /randompath/");
      assertEquals(
          e.getCause().getMessage(),
          "Invalid partition structure was specified. Earlier inferred partition names were [id, data]. Parsing current path resulted in partition names [otherColumn, data]. Path /randompath/otherColumn=1/data=value/file2.parquet. Please correct directory structure if possible.");
      throw e;
    }
    // Reset the vectors for the second batch
    mutator.close();
  }

  private void setupFsListIteratorMockForInvalidPartitions3(HadoopFileSystem fs, Path inputPath)
      throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(Path.of(inputPath.resolve("id=1/file1.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=1/data=value/file1.parquet").toString()))
    };

    FetchOnDemandDirectoryStream statusesIterator3 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]),
            new FileStatus(
                20,
                false,
                1,
                4096,
                2,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[1]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator3);
  }

  @Test(expected = UserException.class)
  // Test to verify change in partition depth. First path has id=1 and second has id=1/data=value/
  public void testDirListingParserPartitionPathsError3()
      throws IOException, ExecutionSetupException {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockForInvalidPartitions3(fs, inputPath);

    OperatorContext context = getCtx();
    setupMutator();
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());

    reader = new DirListingRecordReader(context, fs, split, true, null, null, true, true);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    try {
      reader.next();
    } catch (Exception e) {
      assertTrue(e instanceof UserException);
      assertEquals(e.getMessage(), "Failed to list files of directory /randompath/");
      assertEquals(
          e.getCause().getMessage(),
          "Invalid partition structure was specified. Earlier inferred partition names were [id]. Parsing current path "
              + "resulted in partition names [id, data]. Path /randompath/id=1/data=value/file1.parquet. Please correct directory structure if possible.");
      throw e;
    }
    // Reset the vectors for the second batch
    mutator.close();
  }

  private void setupFsListIteratorMockForParsingPartitionPaths4(HadoopFileSystem fs, Path inputPath)
      throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(Path.of(inputPath.resolve("tmp1/tmp2/file1.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("temp1/temp2/file1.parquet").toString()))
    };

    FetchOnDemandDirectoryStream statusesIterator4 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]),
            new FileStatus(
                20,
                false,
                1,
                4096,
                2,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[1]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator4);
  }

  // Should error out when directory names are not in required formats
  @Test(expected = UserException.class)
  public void testDirListingParserPartitionPathsOldStructure()
      throws IOException, ExecutionSetupException, ClassNotFoundException {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockForParsingPartitionPaths4(fs, inputPath);

    OperatorContext context = getCtx();
    setupMutator();
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());

    reader = new DirListingRecordReader(context, fs, split, true, null, null, true, true);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    try {
      reader.next();
    } catch (Exception e) {
      assertTrue(e instanceof UserException);
      assertEquals(e.getMessage(), "Failed to list files of directory /randompath/");
      assertEquals(
          e.getCause().getMessage(),
          "All the directories should have = in the partition structure. Path /randompath/tmp1/tmp2/file1.parquet");
      throw e;
    }
    // Reset the vectors for the second batch
    mutator.close();
  }

  private void setupFsListIteratorMockForInvalidPartitions4(HadoopFileSystem fs, Path inputPath)
      throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(Path.of(inputPath.resolve("file1.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=1/file1.parquet").toString())),
      toHadoopPath(Path.of(inputPath.resolve("id=1/data=value/file1.parquet").toString()))
    };

    FetchOnDemandDirectoryStream statusesIterator3 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]),
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[1]),
            new FileStatus(
                20,
                false,
                1,
                4096,
                2,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[2]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator3);
  }

  // Should error out when directory names are not in required formats
  @Test(expected = UserException.class)
  public void testDirListingParserPartitionPathsInconsistentStructure()
      throws IOException, ExecutionSetupException, ClassNotFoundException {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockForInvalidPartitions4(fs, inputPath);

    OperatorContext context = getCtx();
    setupMutator();
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());

    reader = new DirListingRecordReader(context, fs, split, true, null, null, true, true);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    try {
      reader.next();
    } catch (Exception e) {
      assertTrue(e instanceof UserException);
      assertEquals(e.getMessage(), "Failed to list files of directory /randompath/");
      assertEquals(
          e.getCause().getMessage(),
          "Invalid partition structure was specified. Earlier inferred partition names were []. Parsing current path resulted in partition names [id]. Path /randompath/id=1/file1.parquet. Please correct directory structure if possible.");
      throw e;
    }
    // Reset the vectors for the second batch
    mutator.close();
  }

  private void setupFsListIteratorMockForInvalidPartitions5(HadoopFileSystem fs, Path inputPath)
      throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
      toHadoopPath(
          Path.of(
              inputPath
                  .resolve("id=1/da=ta=value/file1.parquet")
                  .toString())) // directories with more
    };

    FetchOnDemandDirectoryStream statusesIterator3 =
        newRemoteIterator(
            inputPath,
            testAllocator,
            new FileStatus(
                20,
                false,
                1,
                4096,
                1,
                2,
                FsPermission.getFileDefault(),
                "testowner",
                "testgroup",
                testPaths[0]));

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator3);
  }

  @Test(expected = UserException.class)
  public void testDirListingParserPartitionPathsInvalidName()
      throws IOException, ExecutionSetupException, ClassNotFoundException {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockForInvalidPartitions5(fs, inputPath);

    OperatorContext context = getCtx();
    setupMutator();
    DirListInputSplitProto.DirListInputSplit split =
        getDirListInputSplit(inputPath.toString(), inputPath.toString());

    reader = new DirListingRecordReader(context, fs, split, true, null, null, true, true);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    try {
      reader.next();
    } catch (Exception e) {
      assertTrue(e instanceof UserException);
      assertEquals(e.getMessage(), "Failed to list files of directory /randompath/");
      assertEquals(
          e.getCause().getMessage(),
          "All the directories should have = in the partition structure. Path /randompath/id=1/da=ta=value/file1.parquet");
      throw e;
    }

    // Reset the vectors for the second batch
    mutator.close();
  }
}
