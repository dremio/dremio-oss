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

import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;

import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.NettyArrowBuf;

/**
 * OffsetIndexProvider:
 * Given a columnChunkMetadata returns its OffsetIndex.
 */

public class OffsetIndexProvider implements Closeable {
    private final Map<Long, OffsetIndex> offsetIndexMap = new HashMap<>();

    /**
     * Two types of instantiation call:
     * @param bulkInputStream
     * @param columns
     * offsetIndexMap is populated from (seekableInputStream+columns).
     *
     * Async readers prepopulate the Map that corresponds to Long and OffsetIndex, they instantiate directly with Map.
     * @throws IOException
     */

    public OffsetIndexProvider(BulkInputStream bulkInputStream, org.apache.arrow.memory.BufferAllocator allocator, List<ColumnChunkMetaData> columns) {
        long startOffsetIndex = -1;
        long endOffsetIndex = -1;

        //If Map is not provided, read it.

        Preconditions.checkState(bulkInputStream != null, "Seekabale InputStream Null in OffsetIndexProvider");
        Preconditions.checkState(allocator != null, "Allocator Null in OffsetIndexProvider");
        for (ColumnChunkMetaData column : columns) {
            IndexReference offsetRef = column.getOffsetIndexReference();
            if (offsetRef == null) {
                continue;
            }
            if (startOffsetIndex == -1) {
                startOffsetIndex = offsetRef.getOffset();
                endOffsetIndex = startOffsetIndex + offsetRef.getLength();
            } else if (offsetRef.getOffset() < startOffsetIndex) {
                startOffsetIndex = offsetRef.getOffset();
            } else if (offsetRef.getOffset() >= endOffsetIndex) {
                endOffsetIndex = offsetRef.getOffset() + offsetRef.getLength();
            }
        }
        if (startOffsetIndex != -1) {
            NettyArrowBuf byteBuffer = NettyArrowBuf.unwrapBuffer(allocator.buffer(endOffsetIndex - startOffsetIndex));
            try {
                bulkInputStream.seek(startOffsetIndex);
                bulkInputStream.readFully(byteBuffer, (int)(endOffsetIndex - startOffsetIndex));
                for (ColumnChunkMetaData column : columns) {
                    IndexReference offsetRef = column.getOffsetIndexReference();
                    if (offsetRef == null) {
                        continue;
                    }
                    NettyArrowBuf is = byteBuffer.slice((int) (offsetRef.getOffset() - startOffsetIndex), offsetRef.getLength());
                    try (ByteBufInputStream offsetInputStream = new ByteBufInputStream(is)) {
                        OffsetIndex offsetIndex = ParquetMetadataConverter.fromParquetOffsetIndex(Util.readOffsetIndex(offsetInputStream));
                        offsetIndexMap.put(offsetRef.getOffset(), offsetIndex);
                    }
                }
            } catch (IOException ex) {
                offsetIndexMap.clear();
                //Ignore the IOException.
            } finally {
                byteBuffer.release();
            }
        }

    }

    public OffsetIndexProvider(Map<Long, OffsetIndex> offsetIndexMap) {
        if (offsetIndexMap != null) {
            this.offsetIndexMap.putAll(offsetIndexMap);
        }
    }

    public OffsetIndex getOffsetIndex(ColumnChunkMetaData column) {
        IndexReference offsetRef = column.getOffsetIndexReference();
        if (offsetRef != null) {
            return offsetIndexMap.get(offsetRef.getOffset());
        }
        return null;
    }

    @Override
    public void close() {
        offsetIndexMap.clear();
    }
}
