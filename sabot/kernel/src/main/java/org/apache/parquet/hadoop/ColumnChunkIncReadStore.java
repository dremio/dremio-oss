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
package org.apache.parquet.hadoop;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import com.dremio.exec.store.parquet.BulkInputStream;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.io.file.Path;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

public class ColumnChunkIncReadStore implements PageReadStore {

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private CompressionCodecFactory codecFactory;
  private BufferAllocator allocator;
  private Path path;
  private long rowCount;
  private InputStreamProvider inputStreamProvider;

  public ColumnChunkIncReadStore(long rowCount, CompressionCodecFactory codecFactory, BufferAllocator allocator,
      Path path, InputStreamProvider inputStreamProvider) {
    this.codecFactory = codecFactory;
    this.allocator = allocator;
    this.path = path;
    this.rowCount = rowCount;
    this.inputStreamProvider = inputStreamProvider;
  }

  public class SingleStreamColumnChunkIncPageReader extends ColumnChunkIncPageReader {
    private long lastPosition;

    public SingleStreamColumnChunkIncPageReader(ColumnChunkMetaData metaData, ColumnDescriptor columnDescriptor, BulkInputStream in) throws IOException {
      super(metaData, columnDescriptor, in);
      lastPosition = in.getPos();
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      try {
        in.seek(lastPosition);
        final DictionaryPage dictionaryPage = super.readDictionaryPage();
        lastPosition = in.getPos();
        return dictionaryPage;
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public DataPage readPage() {
      try {
        in.seek(lastPosition);
        final DataPage dataPage = super.readPage();
        lastPosition = in.getPos();
        return dataPage;
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  class ColumnChunkIncPageReader implements PageReader {

    ColumnChunkMetaData metaData;
    ColumnDescriptor columnDescriptor;
    long fileOffset;
    long size;
    private long valueReadSoFar = 0;

    private DictionaryPage dictionaryPage;
    protected BulkInputStream in;
    private BytesInputDecompressor decompressor;

    // Release the data page buffer before reading the next page or in close
    private ByteBuf lastDataPageUncompressed;

    // Release the dictionary page buffer in close
    private ByteBuf dictionaryPageUncompressed;

    public ColumnChunkIncPageReader(ColumnChunkMetaData metaData, ColumnDescriptor columnDescriptor, BulkInputStream in) throws IOException {
      this.metaData = metaData;
      this.columnDescriptor = columnDescriptor;
      this.size = metaData.getTotalSize();
      this.fileOffset = metaData.getStartingPos();
      this.in = in;
      this.decompressor = codecFactory.getDecompressor(metaData.getCodec());
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (dictionaryPage == null) {
        PageHeader pageHeader = new PageHeader();
        long pos = 0;
        try {
          pos = in.getPos();
          pageHeader = Util.readPageHeader(in.asSeekableInputStream());
          if (pageHeader.getDictionary_page_header() == null) {
            in.seek(pos);
            return null;
          }
          dictionaryPage = readDictionaryPageHelper(pageHeader);
        } catch (Exception e) {
          throw new RuntimeException("Error reading dictionary page." +
            "\nFile path: " + path.toURI().getPath() +
            "\nRow count: " + rowCount +
            "\nColumn Chunk Metadata: " + metaData +
            "\nPage Header: " + pageHeader +
            "\nFile offset: " + fileOffset +
            "\nSize: " + size +
            "\nValue read so far: " + valueReadSoFar +
            "\nPosition: " + pos, e);
        }
      }
      return dictionaryPage;
    }


    private DictionaryPage readDictionaryPageHelper(PageHeader pageHeader) throws IOException {
      ByteBuffer data = uncompressPage(pageHeader, false);
      return new DictionaryPage(
          BytesInput.from(data, 0, pageHeader.uncompressed_page_size),
          pageHeader.getDictionary_page_header().getNum_values(),
          parquetMetadataConverter.getEncoding(pageHeader.dictionary_page_header.encoding)
      );
    }

    @Override
    public long getTotalValueCount() {
      return metaData.getValueCount();
    }

    @Override
    public DataPage readPage() {
      PageHeader pageHeader = new PageHeader();
      try {
        releasePrevDataPageBuffers();
        while(valueReadSoFar < metaData.getValueCount()) {
          pageHeader = Util.readPageHeader(in.asSeekableInputStream());
          int uncompressedPageSize = pageHeader.getUncompressed_page_size();
          int compressedPageSize = pageHeader.getCompressed_page_size();
          switch (pageHeader.type) {
            case DICTIONARY_PAGE:
              if (dictionaryPage == null) {
                dictionaryPage = readDictionaryPageHelper(pageHeader);
              } else {
                in.skip(pageHeader.compressed_page_size);
              }
              break;
            case DATA_PAGE:
              valueReadSoFar += pageHeader.data_page_header.getNum_values();
              ByteBuffer destBuffer = uncompressPage(pageHeader, true);
              return new DataPageV1(
                      BytesInput.from(destBuffer, 0, pageHeader.uncompressed_page_size),
                      pageHeader.data_page_header.num_values,
                      pageHeader.uncompressed_page_size,
                      fromParquetStatistics(pageHeader.data_page_header.statistics, columnDescriptor.getType()),
                      parquetMetadataConverter.getEncoding(pageHeader.data_page_header.repetition_level_encoding),
                      parquetMetadataConverter.getEncoding(pageHeader.data_page_header.definition_level_encoding),
                      parquetMetadataConverter.getEncoding(pageHeader.data_page_header.encoding)
              );
            // TODO - finish testing this with more files
            case DATA_PAGE_V2:
              valueReadSoFar += pageHeader.data_page_header_v2.getNum_values();
              destBuffer = uncompressPage(pageHeader, true);
              DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
              int dataSize = uncompressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
              return DataPageV2.uncompressed(
                      dataHeaderV2.getNum_rows(),
                      dataHeaderV2.getNum_nulls(),
                      dataHeaderV2.getNum_values(),
                      BytesInput.from(destBuffer, 0, dataHeaderV2.getRepetition_levels_byte_length()),
                      BytesInput.from(destBuffer,
                          dataHeaderV2.getRepetition_levels_byte_length(),
                          dataHeaderV2.getDefinition_levels_byte_length()),
                      parquetMetadataConverter.getEncoding(dataHeaderV2.getEncoding()),
                      BytesInput.from(destBuffer,
                        dataHeaderV2.getRepetition_levels_byte_length() + dataHeaderV2.getDefinition_levels_byte_length(),
                        dataSize),
                      fromParquetStatistics(dataHeaderV2.getStatistics(), columnDescriptor.getType()));
            default:
              in.skip(pageHeader.compressed_page_size);
              break;
          }
        }
        return null;
      } catch (OutOfMemoryException e) {
        throw e; // throw as it is
      } catch (Exception e) {
        throw new RuntimeException("Error reading page." +
          "\nFile path: " + path.toURI().getPath() +
          "\nRow count: " + rowCount +
          "\nColumn Chunk Metadata: " + metaData +
          "\nPage Header: " + pageHeader +
          "\nFile offset: " + fileOffset +
          "\nSize: " + size +
          "\nValue read so far: " + valueReadSoFar, e);
      }
    }

    void close() {
      try {
        releasePrevDataPageBuffers();
      } finally {
        if (dictionaryPageUncompressed != null) {
          dictionaryPageUncompressed.release();
          dictionaryPageUncompressed = null;
        }
      }
    }

    private void releasePrevDataPageBuffers() {
      if (lastDataPageUncompressed != null) {
        lastDataPageUncompressed.release();
        lastDataPageUncompressed = null;
      }
    }

    private void readFully(ByteBuf dest, int size) throws IOException {
      in.readFully(dest, size);

      // reset the position back to beginning
      dest.readerIndex(0);
    }

    private ByteBuffer uncompressPage(PageHeader pageHeader, boolean isDataPage) throws IOException {
      final int compressedPageSize = pageHeader.compressed_page_size;
      final int uncompressedPageSize = pageHeader.uncompressed_page_size;
      final ByteBuf src = NettyArrowBuf.unwrapBuffer(allocator.buffer(compressedPageSize));
      ByteBuf dest = null;
      try {
        readFully(src, compressedPageSize);
        dest = NettyArrowBuf.unwrapBuffer(allocator.buffer(uncompressedPageSize));
        ByteBuffer destBuffer = dest.nioBuffer(0, uncompressedPageSize);

        switch (pageHeader.type) {
          /**
           * Page structure :
           * [RepetitionLevelBytes][DefinitionLevelBytes][DataBytes]
           * Only the data bytes are compressed.
           */
          case DATA_PAGE_V2:
            final int dataOffset = pageHeader.getData_page_header_v2().getRepetition_levels_byte_length() +
              pageHeader.getData_page_header_v2().getDefinition_levels_byte_length();
            final int compressedDataSize = compressedPageSize - dataOffset;
            //Copy the repetition levels and definition levels as it is.
            if (dataOffset > 0) {
              final ByteBuffer rlDlBuffer = src.nioBuffer(0, dataOffset);
              destBuffer.put(rlDlBuffer);
            }
            // decompress the data part
            if (compressedDataSize > 0) {
              final int uncompressedDataSize = uncompressedPageSize - dataOffset;
              final ByteBuffer srcDataBuf = src.nioBuffer(dataOffset, compressedDataSize);
              final ByteBuffer destDataBuf = dest.nioBuffer(dataOffset, uncompressedDataSize);
              // important to add the starting position to the sizes so that
              // the decompresser sets limits correctly.
              decompressor.decompress(srcDataBuf, compressedDataSize,
                destDataBuf, uncompressedDataSize);
            }
            break;
          default:
            ByteBuffer srcBuffer = src.nioBuffer(0, compressedPageSize);
            decompressor.decompress(srcBuffer, compressedPageSize, destBuffer, uncompressedPageSize);
        }
        if (isDataPage) {
          lastDataPageUncompressed = dest;
        } else {
          dictionaryPageUncompressed = dest;
        }
        return destBuffer;
      } catch (IOException e) {
        if (dest != null) {
          dest.release();
        }
        throw e;
      } finally {
        src.release(); // we don't need this anymore
      }
    }
  }

  private Map<ColumnDescriptor, ColumnChunkIncPageReader> columns = new HashMap<>();

  public void addColumn(ColumnDescriptor descriptor, ColumnChunkMetaData metaData) throws IOException {
    final BulkInputStream in = inputStreamProvider.getStream(metaData);
    in.seek(metaData.getStartingPos());
    columns.put(descriptor, inputStreamProvider.isSingleStream()
      ? new SingleStreamColumnChunkIncPageReader(metaData, descriptor, in)
      : new ColumnChunkIncPageReader(metaData, descriptor, in));
  }

  public void close() throws IOException {
    for (ColumnChunkIncPageReader reader : columns.values()) {
      reader.close();
    }
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor descriptor) {
    return columns.get(descriptor);
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }
}
