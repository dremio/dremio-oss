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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.logging.log4j.util.TriConsumer;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.deltalake.DeltaConstants;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Tests for {@link SplitGenTableFunction}
 */
public class TestSplitGenTableFunction extends BaseTestQuery {

    @Test
    public void testNoPartitionSplits() {
        try (VarCharVector pathVector = new VarCharVector(DeltaConstants.SCHEMA_ADD_PATH, allocator);
             BigIntVector sizeVector = new BigIntVector(DeltaConstants.SCHEMA_ADD_SIZE, allocator);
             BigIntVector mtimeVector = new BigIntVector(DeltaConstants.SCHEMA_ADD_MODIFICATION_TIME, allocator);
             AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable();
             VectorContainer incoming = new VectorContainer()) {
            List<ValueVector> incomingVectors = ImmutableList.of(pathVector, sizeVector, mtimeVector);
            incoming.addCollection(incomingVectors);
            incomingVectors.stream().forEach(ValueVector::allocateNew);
            AtomicInteger counter = new AtomicInteger(0);
            TriConsumer<String, Long, Long> incomingRow = (path, size, mtime) -> {
                int idx = counter.getAndIncrement();
                pathVector.set(idx, path.getBytes(StandardCharsets.UTF_8));
                sizeVector.set(idx, size);
                mtimeVector.set(idx, mtime);
            };
            SplitGenTableFunction tableFunction = new SplitGenTableFunction(null, getOpCtx(), getConfig(Collections.EMPTY_LIST));
            long currentTime = System.currentTimeMillis();
            incomingRow.accept("file1.parquet", 1024L, currentTime);
            incomingRow.accept("file2.parquet", 2054L, currentTime);
            incomingRow.accept("file3.parquet", 211L, currentTime);
            incoming.setAllCount(3);
            incoming.buildSchema();

            VectorAccessible outgoing = tableFunction.setup(incoming);
            closer.addAll(outgoing);

            VarBinaryVector outgoingSplits = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, RecordReader.SPLIT_INFORMATION);

            tableFunction.startRow(0);
            assertEquals(1, tableFunction.processRow(0, 5));
            assertEquals(0, tableFunction.processRow(1, 5));
            assertEquals(1, outgoingSplits.getValueCount());

            SplitAndPartitionInfo split0 = extractSplit(outgoingSplits, 0);
            assertSplit(split0, "/test/file1.parquet", 0L, 1024L, 1024L, currentTime);

            tableFunction.startRow(1);
            assertEquals(1, tableFunction.processRow(1, 5));
            tableFunction.startRow(2);
            assertEquals(1, tableFunction.processRow(2, 5));

            assertEquals(3, outgoingSplits.getValueCount());
            assertSplit(extractSplit(outgoingSplits, 1), "/test/file2.parquet", 0L,2054L, 2054L, currentTime);
            assertSplit(extractSplit(outgoingSplits, 2), "/test/file3.parquet", 0L,211L, 211L, currentTime);
            tableFunction.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSplitsLargerThanMaxRecords() {
        final long blockSize = getOpCtx().getOptions().getOption(ExecConstants.PARQUET_SPLIT_SIZE).getNumVal();
        final int batchSize = 5;
        try (VarCharVector pathVector = new VarCharVector(DeltaConstants.SCHEMA_ADD_PATH, allocator);
             BigIntVector sizeVector = new BigIntVector(DeltaConstants.SCHEMA_ADD_SIZE, allocator);
             BigIntVector mtimeVector = new BigIntVector(DeltaConstants.SCHEMA_ADD_MODIFICATION_TIME, allocator);
             AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable();
             VectorContainer incoming = new VectorContainer()) {
            List<ValueVector> incomingVectors = ImmutableList.of(pathVector, sizeVector, mtimeVector);
            incoming.addCollection(incomingVectors);
            incomingVectors.stream().forEach(ValueVector::allocateNew);
            AtomicInteger counter = new AtomicInteger(0);
            TriConsumer<String, Long, Long> incomingRow = (path, size, mtime) -> {
                int idx = counter.getAndIncrement();
                pathVector.set(idx, path.getBytes(StandardCharsets.UTF_8));
                sizeVector.set(idx, size);
                mtimeVector.set(idx, mtime);
            };
            SplitGenTableFunction tableFunction = new SplitGenTableFunction(null, getOpCtx(), getConfig(Collections.EMPTY_LIST));
            long currentTime = System.currentTimeMillis();
            long sizeThatCrossesBoundaries = blockSize * batchSize * 2; // fills up two batches of splits from a single input row
            incomingRow.accept("file1.parquet", sizeThatCrossesBoundaries, currentTime);
            incomingRow.accept("file2.parquet", 2054L, currentTime);
            incoming.setAllCount(2);
            incoming.buildSchema();

            VectorAccessible outgoing = tableFunction.setup(incoming);
            closer.addAll(outgoing);

            VarBinaryVector outgoingSplits = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, RecordReader.SPLIT_INFORMATION);

            tableFunction.startRow(0); // expected to produce 2 batches
            // Batch 1
            assertEquals(5, tableFunction.processRow(0, 5));
            // Batch 2
            assertEquals(5, tableFunction.processRow(5, 5));
            // Nothing left
            assertEquals(0, tableFunction.processRow(10, 5));

            assertEquals(10, outgoingSplits.getValueCount());

            for (int i = 0; i < 10; i++) {
                long startingPosition = blockSize * i;
                assertSplit(extractSplit(outgoingSplits, i), "/test/file1.parquet", startingPosition, blockSize, sizeThatCrossesBoundaries, currentTime);
            }

            tableFunction.startRow(1);
            assertEquals(1, tableFunction.processRow(10, 5));
            assertEquals(11, outgoingSplits.getValueCount());
            assertSplit(extractSplit(outgoingSplits, 10), "/test/file2.parquet", 0L, 2054L, 2054L, currentTime);
            tableFunction.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testMultiPartitionsPerSplit() {
    }

    private void assertSplit(SplitAndPartitionInfo split, String path, long start, long size, long fileSize, long mtime) throws InvalidProtocolBufferException {
        ParquetProtobuf.ParquetBlockBasedSplitXAttr xAttr = ParquetProtobuf.ParquetBlockBasedSplitXAttr
                .parseFrom(split.getDatasetSplitInfo().getExtendedProperty());
        assertEquals(path, xAttr.getPath());
        assertEquals(size, xAttr.getLength());
        assertEquals(0, xAttr.getLastModificationTime());
        assertEquals(start, xAttr.getStart());
        assertEquals(fileSize, xAttr.getFileLength());
    }

    private SplitAndPartitionInfo extractSplit(VarBinaryVector splits, int idx) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(splits.get(idx));
             ObjectInput in = new ObjectInputStream(bis)) {
            return (SplitAndPartitionInfo) in.readObject();
        }
    }

    private TableFunctionConfig getConfig(List<String> partitionCol) {
        TableFunctionContext functionContext = mock(TableFunctionContext.class);
        when(functionContext.getPartitionColumns()).thenReturn(partitionCol);
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
