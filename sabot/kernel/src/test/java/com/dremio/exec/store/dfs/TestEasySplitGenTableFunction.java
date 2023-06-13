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
package com.dremio.exec.store.dfs;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Tests for {@link SplitGenTableFunction}
 */
public class TestEasySplitGenTableFunction extends BaseTestQuery {
  private static String file1 = "file1.csv" ;
  private static String file2 = "file2.csv" ;
  private static long file1Length = 1000 ;
  private static long file2Length = 1032 ;

    private interface RowHandler {
        void accept(String path, long size, long mtime);
    }

    @Test
    public void testNoPartitionSplits() throws Exception {
        try (VarCharVector pathVector = new VarCharVector(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH, allocator);
             BigIntVector sizeVector = new BigIntVector(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_SIZE, allocator);
             BigIntVector mtimeVector = new BigIntVector(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.MODIFICATION_TIME, allocator);
             AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable();
             VectorContainer incoming = new VectorContainer()) {
            List<ValueVector> incomingVectors = ImmutableList.of(pathVector, sizeVector, mtimeVector);
            incoming.addCollection(incomingVectors);
            incomingVectors.stream().forEach(ValueVector::allocateNew);
            AtomicInteger counter = new AtomicInteger(0);
          RowHandler incomingRow = (path, size, mtime) -> {
                int idx = counter.getAndIncrement();
                pathVector.set(idx, path.getBytes(StandardCharsets.UTF_8));
                sizeVector.set(idx, size);
                mtimeVector.set(idx, mtime);
            };
          EasySplitGenTableFunction tableFunction = new EasySplitGenTableFunction(null, getOpCtx(), getConfig());
            long currentTime = System.currentTimeMillis();
            incomingRow.accept("file1.csv", file1Length, currentTime);
            incomingRow.accept("file2.csv", file2Length, currentTime);
            incomingRow.accept("file3.csv", 211L, currentTime);
            incoming.setAllCount(3);
            incoming.buildSchema();

            VectorAccessible outgoing = tableFunction.setup(incoming);
            closer.addAll(outgoing);

            VarBinaryVector outgoingSplits = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, RecordReader.SPLIT_INFORMATION);

            tableFunction.startRow(0);
            assertEquals(1, tableFunction.processRow(0, 2));
            assertEquals(1, outgoingSplits.getValueCount());
            assertSplit(extractSplit(outgoingSplits, 0), file1, 0L, file1Length);
            tableFunction.startRow(1);
            assertEquals(1, tableFunction.processRow(1, 5));
            assertEquals(0, tableFunction.processRow(2, 2));
            assertEquals(2, outgoingSplits.getValueCount());
            assertSplit(extractSplit(outgoingSplits, 1), file2, 0L,file2Length);
            tableFunction.close();
        }
    }

    @Test
    public void testMultiPartitionsPerSplit() {
    }

    private void assertSplit(SplitAndPartitionInfo split, String path, long start, long fileSize) throws InvalidProtocolBufferException {
        EasyProtobuf.EasyDatasetSplitXAttr xAttr = EasyProtobuf.EasyDatasetSplitXAttr
                .parseFrom(split.getDatasetSplitInfo().getExtendedProperty());
        assertEquals(path, xAttr.getPath());
        assertEquals(start, xAttr.getStart());
        assertEquals(fileSize, xAttr.getLength());
    }

    private SplitAndPartitionInfo extractSplit(VarBinaryVector splits, int idx) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(splits.get(idx));
             ObjectInput in = new ObjectInputStream(bis)) {
            return (SplitAndPartitionInfo) in.readObject();
        }
    }

    private TableFunctionConfig getConfig() {
        TableFunctionContext functionContext = mock(TableFunctionContext.class);
        when(functionContext.getFullSchema()).thenReturn(RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA);
        List<SchemaPath> expectedColumns = Arrays.asList(SchemaPath.getSimplePath(RecordReader.SPLIT_IDENTITY), SchemaPath.getSimplePath(RecordReader.SPLIT_INFORMATION), SchemaPath.getSimplePath(RecordReader.COL_IDS));
        when(functionContext.getColumns()).thenReturn(expectedColumns);
        FileConfig fc = new FileConfig();
        fc.setLocation("/test");
        when(functionContext.getFormatSettings()).thenReturn(fc);

        TableFunctionConfig config = new TableFunctionConfig(TableFunctionConfig.FunctionType.SPLIT_GENERATION, false,
                functionContext);
        return config;
    }

    private OperatorContext getOpCtx() {
        SabotContext sabotContext = getSabotContext();
        return new OperatorContextImpl(sabotContext.getConfig(), sabotContext.getDremioConfig(), getAllocator(), sabotContext.getOptionManager(), 10, sabotContext.getExpressionSplitCache());
    }
}
