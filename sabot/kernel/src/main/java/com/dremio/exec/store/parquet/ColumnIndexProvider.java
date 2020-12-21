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

package com.dremio.exec.store.parquet;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.NettyArrowBuf;

/**
 * ColumnIndexProvider:
 * Given a columnChunkMetadata returns its ColumnIndex.
 */

public class ColumnIndexProvider implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ColumnIndexProvider.class);
    private final Map<Long, ColumnIndex> columnIndexMap = new HashMap<>();

    /**
     * Two types of instantiation calls:
     * @param bulkInputStream
     * @param columns
     *
     * From the bulkInputStream and columns, read is done and Map is populated.
     *
     * Async readers already have the Map constructed, the class is instantiated with the Map directly.
     * @throws IOException
     */

    public ColumnIndexProvider(BulkInputStream bulkInputStream, BufferAllocator allocator, List<ColumnChunkMetaData> columns) {
        long startColumnIndex = -1;
        long endColumnIndex = -1;

        // Read ColumnIndexes from the disk and construct an incore map.
        Preconditions.checkState(bulkInputStream != null,
                "InputParamaters to ColumnIndexProvider bulkInputStream null");
        Preconditions.checkState(allocator != null,
                "InputParamaters to ColumnIndexProvider allocator null");
        for (ColumnChunkMetaData column : columns) {
            IndexReference columnRef = column.getColumnIndexReference();
            if (columnRef == null) {
                continue;
            }
            if (startColumnIndex == -1) {
                startColumnIndex = columnRef.getOffset();
                endColumnIndex = startColumnIndex + columnRef.getLength();
            } else if (columnRef.getOffset() < startColumnIndex) {
                startColumnIndex = columnRef.getOffset();
            } else if (columnRef.getOffset() >= endColumnIndex) {
                endColumnIndex = columnRef.getOffset() + columnRef.getLength();
            }
        }
        if (startColumnIndex != -1) {
            NettyArrowBuf byteBuffer = NettyArrowBuf.unwrapBuffer(allocator.buffer(endColumnIndex - startColumnIndex));
            try {
                bulkInputStream.seek(startColumnIndex);
                bulkInputStream.readFully(byteBuffer,(int)(endColumnIndex - startColumnIndex));
                for (ColumnChunkMetaData column : columns) {
                    IndexReference columnRef = column.getColumnIndexReference();
                    if (columnRef == null) {
                        continue;
                    }
                    NettyArrowBuf is = byteBuffer.slice((int) (columnRef.getOffset() - startColumnIndex), columnRef.getLength());
                    try (ByteBufInputStream offsetInputStream = new ByteBufInputStream(is)) {
                        ColumnIndex offsetIndex = ParquetMetadataConverter.fromParquetColumnIndex(column.getPrimitiveType(), Util.readColumnIndex(offsetInputStream));
                        columnIndexMap.put(columnRef.getOffset(), offsetIndex);
                    }
                }
            } catch (IOException ex) {
                columnIndexMap.clear();
                //Ignore IOException
            } finally {
                byteBuffer.release();
            }
        }
    }

    public ColumnIndexProvider(Map<Long, ColumnIndex> columnIndexMap) {
        if (columnIndexMap != null) {
            this.columnIndexMap.putAll(columnIndexMap);
        }
    }

    public ColumnIndex getColumnIndex(ColumnChunkMetaData column) {
        IndexReference columnRef = column.getColumnIndexReference();
        if (columnRef != null) {
            return columnIndexMap.get(columnRef.getOffset());
        }
        return null;
    }

    @Override
    public void close() {
      columnIndexMap.clear();
    }
}
