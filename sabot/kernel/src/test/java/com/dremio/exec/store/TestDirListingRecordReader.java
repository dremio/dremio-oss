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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
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

public class TestDirListingRecordReader extends BaseTestQuery {

  private BufferAllocator testAllocator;
  private SampleMutator mutator;
  private RecordReader reader;

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

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
    OperatorContext operatorContext = mock(OperatorContext.class);
    when(operatorContext.getAllocator()).thenReturn(testAllocator);
    when(operatorContext.getTargetBatchSize()).thenReturn(4000);
    when(operatorContext.getMinorFragmentEndpoints()).thenReturn(ImmutableList.of(new MinorFragmentEndpoint(0,null)));
    return operatorContext;
  }

  private OperatorStats getOperatorStats() {
    OpProfileDef prof = new OpProfileDef(1, 1, 1);
    final OperatorStats operatorStats = new OperatorStats(prof, testAllocator);
    return operatorStats;
  }


  private FileSystem setUpFs() throws RuntimeException, IOException {
    HadoopFileSystem fs = mock(HadoopFileSystem.class);
    return fs;
  }

  private interface RemoteIteratorWrapper extends RemoteIterator<LocatedFileStatus>{
    boolean isClosed();
  }


  private FetchOnDemandDirectoryStream newRemoteIterator(Path path, final FileStatus... statuses) {
    final Iterator<FileStatus> iterator = Arrays.asList(statuses).iterator();
    final AtomicBoolean closed = new AtomicBoolean(false);

    RemoteIteratorWrapper wrapper = new RemoteIteratorWrapper() {
      @Override
      public boolean hasNext(){
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

    return new FetchOnDemandDirectoryStream(wrapper, path, getOperatorStats());
  }

  private static org.apache.hadoop.fs.Path toHadoopPath(Path path) {
    return new org.apache.hadoop.fs.Path(path.toURI());
  }

  private static DirListInputSplitProto.DirListInputSplit getDirListInputSplit(String operatingPath, String rootPath) {
    return DirListInputSplitProto.DirListInputSplit.newBuilder()
      .setOperatingPath(operatingPath)
      .setRootPath(rootPath)
      .setReadSignature(System.currentTimeMillis())
      .build();
  }

  private void setupMutator() {
    mutator = new SampleMutator(testAllocator);

    List<Field> children = Arrays.asList(CompleteType.VARCHAR.toField("key"),
      CompleteType.VARCHAR.toField("value"));

    Field partInfo = CompleteType.VARBINARY.toField(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.PARTITION_INFO);
    Field filePath = CompleteType.VARCHAR.toField(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    Field mtime = CompleteType.BIGINT.toField(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.MODIFICATION_TIME);
    Field size = CompleteType.BIGINT.toField(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_SIZE);

    mutator.addField(filePath, VarCharVector.class);
    mutator.addField(partInfo, VarBinaryVector.class);
    mutator.addField(mtime, BigIntVector.class);
    mutator.addField(size, BigIntVector.class);

    mutator.getContainer().buildSchema();
  }

  private void setupFsListIteratorMock(HadoopFileSystem fs, Path inputPath) throws IOException {

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
      toHadoopPath(Path.of(inputPath.resolve("foo.parquet").toString())),     //File
      toHadoopPath(Path.of(inputPath.resolve("bar").toString())),             //Dir
      toHadoopPath(Path.of(inputPath.resolve("bar/file1.parquet").toString())), //File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1").toString())),       //Dir1/Dir2
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/file2.parquet").toString())),   //Dir1/Dir2/File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/subBar2").toString())),         //Dir1/Dir2/Dir3
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/file3.parquet").toString())),   //Dir1/Dir2/File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/file4.parquet").toString())),   //Dir1/Dir2/File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/subBar2/file5.parquet").toString())),   //Dir1/Dir2/Dir3/File
    };

    org.apache.hadoop.fs.Path[] relativePathToSubBar = {
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/file2.parquet").toString())),   //Dir1/Dir2/File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/subBar2").toString())),         //Dir1/Dir2/Dir3
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/file3.parquet").toString())),   //Dir1/Dir2/File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/file4.parquet").toString())),   //Dir1/Dir2/File
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/subBar2/file5.parquet").toString())),   //Dir1/Dir2/Dir3/File
    };

    FetchOnDemandDirectoryStream statusesIterator1 = newRemoteIterator(inputPath,
      new FileStatus(20, false, 1, 4096, 1, 2, FsPermission.getFileDefault(), "testowner", "testgroup", testPaths[0]),
      new FileStatus(40, true, 0, 0, 32, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[1]),
      new FileStatus(70, false, 0, 0, 31, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[2]),
      new FileStatus(1010, true, 0, 0, 33, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[3]),
      new FileStatus(1200, false, 0, 0, 32, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[4]),
      new FileStatus(400, true, 0, 0, 13, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[5]),
      new FileStatus(1200, false, 0, 0, 312, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[6]),
      new FileStatus(1400, false, 0, 0, 331, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[7]),
      new FileStatus(1320, false, 0, 0, System.currentTimeMillis() + 1000000, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[8])
    );

    FetchOnDemandDirectoryStream statusesIterator2 = newRemoteIterator(inputPath,
      new FileStatus(20, false, 1, 4096, 1, 2, FsPermission.getFileDefault(), "testowner", "testgroup", relativePathToSubBar[0]),
      new FileStatus(40, true, 0, 0, 32, 4, FsPermission.getDirDefault(), "testowner", "testgroup", relativePathToSubBar[1]),
      new FileStatus(70, false, 0, 0, 31, 4, FsPermission.getDirDefault(), "testowner", "testgroup", relativePathToSubBar[2]),
      new FileStatus(1010, false, 0, 0, 33, 4, FsPermission.getDirDefault(), "testowner", "testgroup", relativePathToSubBar[3]),
      new FileStatus(1200, false, 0, 0, 32, 4, FsPermission.getDirDefault(), "testowner", "testgroup", relativePathToSubBar[4])
    );

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator1);

    Path relativeToBar = inputPath.resolve(Path.of("bar"));
    when(fs.listFiles(relativeToBar, false)).thenReturn(statusesIterator2);
  }

  private void setupFsListIteratorMockWithHiddenFiles(HadoopFileSystem fs, Path inputPath) throws IOException {
    org.apache.hadoop.fs.Path[] testPaths = {
            toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/file2.parquet").toString())),   //Dir1/Dir2/File
            toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/.subBar2").toString())),         //Dir1/Dir2/Dir3
            toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/file3.parquet").toString())),   //Dir1/Dir2/File
            toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/_file4.parquet").toString())),   //Dir1/Dir2/File
            toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/.subBar2/file5.parquet").toString())),   //Dir1/Dir2/Dir3/File
            toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/_subBar3").toString())),   //Dir1/Dir2/Dir3
            toHadoopPath(Path.of(inputPath.resolve("bar/subBar1/_subBar3/file5.parquet").toString())),   //Dir1/Dir2/Dir3/File
    };

    FetchOnDemandDirectoryStream statusesIterator1 = newRemoteIterator(inputPath,
            new FileStatus(20, false, 1, 4096, 1, 2, FsPermission.getFileDefault(), "testowner", "testgroup", testPaths[0]),
            new FileStatus(40, true, 0, 0, 32, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[1]),
            new FileStatus(70, false, 0, 0, 31, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[2]),
            new FileStatus(1010, false, 0, 0, 33, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[3]),
            new FileStatus(1200, false, 0, 0, 32, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[4]),
            new FileStatus(40, true, 0, 0, 32, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[5]),
            new FileStatus(70, false, 0, 0, 31, 4, FsPermission.getDirDefault(), "testowner", "testgroup", testPaths[6])
    );

    when(fs.listFiles(inputPath, true)).thenReturn(statusesIterator1);
  }

  private void setupFsListIteratorMockWithLargeFiles(HadoopFileSystem fs, Path inputPath) throws IOException {

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

    statuses[0] = new FileStatus(20, true, 1, 4096, 1, 2, FsPermission.getFileDefault(), "testowner", "testgroup", toHadoopPath(Path.of(inputPath.resolve("bar").toString())));

    for(int i = 1; i <= 200; i++) {
      org.apache.hadoop.fs.Path hadoopPath = toHadoopPath(Path.of(inputPath.resolve("bar/file" + i + ".parquet").toString()));
      statuses[i] = new FileStatus(20, false, 1, 4096, 1, 2, FsPermission.getFileDefault(), "testowner", "testgroup", hadoopPath);
    }

    statuses[201] = new FileStatus(20, true, 1, 4096, 1, 2, FsPermission.getFileDefault(), "testowner", "testgroup",
      toHadoopPath(Path.of(inputPath.resolve("bar/subBar").toString())));

    statuses[202] = new FileStatus(20, true, 1, 4096, 1, 2, FsPermission.getFileDefault(), "testowner", "testgroup", toHadoopPath(Path.of(inputPath.resolve("subBar").toString())));

    for(int i = 203; i <= 403; i++) {
      org.apache.hadoop.fs.Path hadoopPath = toHadoopPath(Path.of(inputPath.resolve("bar/subBar/file" + (i - 2) + ".parquet").toString()));
      statuses[i] = new FileStatus(20, false, 1, 4096, 1, 2, FsPermission.getFileDefault(), "testowner", "testgroup", hadoopPath);
    }

    when(fs.listFiles(inputPath, true)).thenReturn(newRemoteIterator(inputPath, statuses));
  }

  @Test
  public void testDirListReaderForFileSystemPartitionIsRecursive() throws Exception {
    Path inputPath = Path.of("/randompath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();

    setupMutator();
    setupFsListIteratorMock(fs, inputPath);
    DirListInputSplitProto.DirListInputSplit split = getDirListInputSplit(inputPath.toString(), inputPath.toString());
    reader = new DirListingRecordReader(getCtx(), fs, split, true, null, null,true);
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

    assertEquals(outputpaths.getObject(2).toString(), "/randompath/bar/subBar1/file2.parquet?version=32");
    assertEquals(extractPartitionData(outputPartInfo.getObject(2)), "PartitionData{dir0=bar, dir1=subBar1}");
    assertEquals(mtime.get(2), 32);
    assertEquals(size.get(2), 1200);

    assertEquals(outputpaths.getObject(3).toString(), "/randompath/bar/subBar1/file3.parquet?version=312");
    assertEquals(extractPartitionData(outputPartInfo.getObject(3)), "PartitionData{dir0=bar, dir1=subBar1}");
    assertEquals(mtime.get(3), 312);
    assertEquals(size.get(3), 1200);

    assertEquals(outputpaths.getObject(4).toString(), "/randompath/bar/subBar1/file4.parquet?version=331");
    assertEquals(extractPartitionData(outputPartInfo.getObject(4)), "PartitionData{dir0=bar, dir1=subBar1}");
    assertEquals(mtime.get(4), 331);
    assertEquals(size.get(4), 1400);
  }

  @Test
  public void testDirListReaderForHivePartitionIsRecursive() throws IOException, ExecutionSetupException, ClassNotFoundException {
    Path inputPath = Path.of("/hivePath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMock(fs, inputPath);

    setupMutator();

    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitField"),
        CompleteType.VARCHAR.toField("varCharField")
      );

    List<PartitionProtobuf.PartitionValue> partitionValues = new ArrayList<>();
    partitionValues.add(PartitionProtobuf.PartitionValue.newBuilder().setColumn("integerCol").setIntValue(20).build());
    partitionValues.add(PartitionProtobuf.PartitionValue.newBuilder().setColumn("doubleCol").setDoubleValue(new Double("20")).build());
    partitionValues.add(PartitionProtobuf.PartitionValue.newBuilder().setColumn("bitField").setBitValue(true).build());
    partitionValues.add(PartitionProtobuf.PartitionValue.newBuilder().setColumn("varCharField").setStringValue("tempVarCharValue").build());

    DirListInputSplitProto.DirListInputSplit split = getDirListInputSplit(inputPath.toString(), inputPath.toString());
    reader = new DirListingRecordReader(getCtx(), fs, split, true, tableSchema, partitionValues, false);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    int generatedRecords = reader.next();

    Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
    VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");
    VarBinaryVector outputPartInfo = (VarBinaryVector) fieldVectorMap.get("partitioninfo");
    BigIntVector mtime = (BigIntVector) fieldVectorMap.get("modificationtime");
    BigIntVector size = (BigIntVector) fieldVectorMap.get("filesize");

    assertEquals(generatedRecords, 5);

    String partInfo = "PartitionData{integerCol=20, doubleCol=20.0, bitField=true, varCharField=tempVarCharValue}";

    assertEquals(outputpaths.getObject(0).toString(), "/hivePath/foo.parquet?version=1");
    assertEquals(extractPartitionData(outputPartInfo.get(0)), partInfo);
    assertEquals(mtime.get(0), 1);
    assertEquals(size.get(0), 20);

    assertEquals(outputpaths.getObject(1).toString(), "/hivePath/bar/file1.parquet?version=31");
    assertEquals(extractPartitionData(outputPartInfo.get(1)), partInfo);
    assertEquals(mtime.get(1), 31);
    assertEquals(size.get(1), 70);

    assertEquals(outputpaths.getObject(2).toString(), "/hivePath/bar/subBar1/file2.parquet?version=32");
    assertEquals(extractPartitionData(outputPartInfo.get(2)), partInfo);
    assertEquals(mtime.get(2), 32);
    assertEquals(size.get(2), 1200);

    assertEquals(outputpaths.getObject(3).toString(), "/hivePath/bar/subBar1/file3.parquet?version=312");
    assertEquals(extractPartitionData(outputPartInfo.get(3)), partInfo);
    assertEquals(mtime.get(3), 312);
    assertEquals(size.get(3), 1200);

    assertEquals(outputpaths.getObject(4).toString(), "/hivePath/bar/subBar1/file4.parquet?version=331");
    assertEquals(extractPartitionData(outputPartInfo.get(4)), partInfo);
    assertEquals(mtime.get(4), 331);
    assertEquals(size.get(4), 1400);
  }

  @Test
  public void testDirListReaderWithLargeFilesInDirectory() throws IOException, ExecutionSetupException {
    Path inputPath = Path.of("/hivePath/");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();
    setupFsListIteratorMockWithLargeFiles(fs, inputPath);

    OperatorContext context = getCtx();
    when(context.getTargetBatchSize()).thenReturn(100);

    setupMutator();
    DirListInputSplitProto.DirListInputSplit split = getDirListInputSplit(inputPath.toString(), inputPath.toString());
    reader = new DirListingRecordReader(context, fs, split, true, null, null, true);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    ((DirListingRecordReader)reader).setBatchSize(100);

    int noRecordsRead = reader.next();


    assertEquals(noRecordsRead, 100);
    Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
    VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");

    assertEquals(outputpaths.getValueCount(), 100);
    assertEquals(outputpaths.getObject(0).toString(), "/hivePath/bar/file1.parquet?version=1");
    assertEquals(outputpaths.getObject(1).toString(), "/hivePath/bar/file2.parquet?version=1");
    assertEquals(outputpaths.getObject(9).toString(), "/hivePath/bar/file10.parquet?version=1");
    assertEquals(outputpaths.getObject(99).toString(), "/hivePath/bar/file100.parquet?version=1");

    //Reset the vectors for the second batch
    mutator.close();
    setupMutator();

    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    noRecordsRead = reader.next();
    assertEquals(noRecordsRead, 100);

    fieldVectorMap = mutator.getFieldVectorMap();
    outputpaths = (VarCharVector) fieldVectorMap.get("filepath");

    assertEquals(outputpaths.getValueCount(), 100);
    assertEquals(outputpaths.getObject(0).toString(), "/hivePath/bar/file101.parquet?version=1");
    assertEquals(outputpaths.getObject(1).toString(), "/hivePath/bar/file102.parquet?version=1");
    assertEquals(outputpaths.getObject(99).toString(), "/hivePath/bar/file200.parquet?version=1");

    //Reset the vectors for the third batch
    mutator.close();
    setupMutator();

    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    noRecordsRead = reader.next();
    assertEquals(noRecordsRead, 100);

    fieldVectorMap = mutator.getFieldVectorMap();
    outputpaths = (VarCharVector) fieldVectorMap.get("filepath");

    assertEquals(outputpaths.getValueCount(), 100);
    assertEquals(outputpaths.getObject(0).toString(), "/hivePath/bar/subBar/file201.parquet?version=1");
    assertEquals(outputpaths.getObject(1).toString(), "/hivePath/bar/subBar/file202.parquet?version=1");
    assertEquals(outputpaths.getObject(99).toString(), "/hivePath/bar/subBar/file300.parquet?version=1");
  }

  @Test
  public void testDirListReaderWithDifferentOperatingAndRootPath() throws Exception {
    Path rootPath = Path.of("/randompath/");
    Path operatingPath = Path.of("/randompath/bar");
    HadoopFileSystem fs = (HadoopFileSystem) setUpFs();

    try(AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      setupMutator();
      setupFsListIteratorMock(fs, rootPath);

      DirListInputSplitProto.DirListInputSplit split = getDirListInputSplit(operatingPath.toString(), rootPath.toString());
      reader = new DirListingRecordReader(getCtx(), fs, split, false, null, null, true);
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

      assertEquals(outputpaths.getObject(0).toString(), "/randompath/bar/subBar1/file2.parquet?version=1");
      assertEquals(extractPartitionData(outputPartInfo.getObject(0)), "PartitionData{dir0=bar, dir1=subBar1}");
      assertEquals(mtime.get(0), 1);
      assertEquals(size.get(0), 20);

      assertEquals(outputpaths.getObject(1).toString(), "/randompath/bar/subBar1/file3.parquet?version=31");
      assertEquals(extractPartitionData(outputPartInfo.getObject(1)), "PartitionData{dir0=bar, dir1=subBar1}");
      assertEquals(mtime.get(1), 31);
      assertEquals(size.get(1), 70);

      assertEquals(outputpaths.getObject(2).toString(), "/randompath/bar/subBar1/file4.parquet?version=33");
      assertEquals(extractPartitionData(outputPartInfo.getObject(2)), "PartitionData{dir0=bar, dir1=subBar1}");
      assertEquals(mtime.get(2), 33);
      assertEquals(size.get(2), 1010);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
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
    DirListInputSplitProto.DirListInputSplit split = getDirListInputSplit(inputPath.toString(), inputPath.toString());
    reader = new DirListingRecordReader(context, fs, split, true, null, null, true);
    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);

    ((DirListingRecordReader)reader).setBatchSize(100);

    int noRecordsRead = reader.next();


    assertEquals(2, noRecordsRead);
    Map<String, ValueVector> fieldVectorMap = mutator.getFieldVectorMap();
    VarCharVector outputpaths = (VarCharVector) fieldVectorMap.get("filepath");

    assertEquals(2, outputpaths.getValueCount());
    assertEquals(outputpaths.getObject(0).toString(), "/hivePath/bar/subBar1/file2.parquet?version=1");
    assertEquals(outputpaths.getObject(1).toString(), "/hivePath/bar/subBar1/file3.parquet?version=31");

    //Reset the vectors for the second batch
    mutator.close();
  }


    private String extractPartitionData(byte[] partitionInfoBytes) throws IOException, ClassNotFoundException {
    java.io.ByteArrayInputStream fis = new java.io.ByteArrayInputStream(partitionInfoBytes);
    java.io.ObjectInputStream ois = new java.io.ObjectInputStream(fis);
    IcebergPartitionData partitionData = (IcebergPartitionData)ois.readObject();
    return partitionData.toString();
  }
}
