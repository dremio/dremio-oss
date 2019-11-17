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
package com.dremio.exec.util;

import static java.lang.String.format;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.PageHeaderWithOffset;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import com.dremio.common.VM;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.parquet.SingletonParquetFooterCache;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 * Reads a parquet file and dictionaries for all binary columns.
 */
public class LocalDictionariesReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalDictionariesReader.class);

  private static boolean isBinaryType(PrimitiveTypeName type) {
    return (PrimitiveTypeName.BINARY == type || PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY == type);
  }

  private static final ParquetMetadataConverter CONVERTER = new ParquetMetadataConverter();

  /**
   * Return dictionary per row group for all binary columns in given parquet file.
   * @param fs filesystem object.
   * @param filePath parquet file to scan
   * @return pair of dictionaries found for binary fields and list of binary fields which are not dictionary encoded.
   * @throws IOException
   */
  public static Pair<Map<ColumnDescriptor, Dictionary>, Set<ColumnDescriptor>> readDictionaries(FileSystem fs, Path filePath, CompressionCodecFactory codecFactory) throws IOException {
    // Passing the max footer length is not required in this case as the parquet reader would already have failed.
    final ParquetMetadata parquetMetadata = SingletonParquetFooterCache.readFooter(fs, filePath, ParquetMetadataConverter.NO_FILTER,
      ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getDefault().getNumVal());
    if (parquetMetadata.getBlocks().size() > 1) {
      throw new IOException(
        format("Global dictionaries can only be built on a parquet file with a single row group, found %d row groups for file %s",
          parquetMetadata.getBlocks().size(), filePath));
    }
    final BlockMetaData rowGroupMetadata = parquetMetadata.getBlocks().get(0);
    final Map<ColumnPath, ColumnDescriptor> columnDescriptorMap = Maps.newHashMap();

    for (ColumnDescriptor columnDescriptor : parquetMetadata.getFileMetaData().getSchema().getColumns()) {
      columnDescriptorMap.put(ColumnPath.get(columnDescriptor.getPath()), columnDescriptor);
    }

    final Set<ColumnDescriptor> columnsToSkip = Sets.newHashSet(); // columns which are found in parquet file but are not dictionary encoded
    final Map<ColumnDescriptor, Dictionary> dictionaries = Maps.newHashMap();
    try(final FSInputStream in = fs.open(filePath)) {
      for (ColumnChunkMetaData columnChunkMetaData : rowGroupMetadata.getColumns()) {
        if (isBinaryType(columnChunkMetaData.getType())) {
          final ColumnDescriptor column = columnDescriptorMap.get(columnChunkMetaData.getPath());
          // if first page is dictionary encoded then load dictionary, otherwise skip this column.
          final PageHeaderWithOffset pageHeader = columnChunkMetaData.getPageHeaders().get(0);
          if (PageType.DICTIONARY_PAGE == pageHeader.getPageHeader().getType()) {
            dictionaries.put(column, readDictionary(in, column, pageHeader, codecFactory.getDecompressor(columnChunkMetaData.getCodec())));
          } else {
            columnsToSkip.add(column);
          }
        }
      }
    }
    return new ImmutablePair<>(dictionaries, columnsToSkip);
  }

  public static Dictionary readDictionary(FSInputStream in, ColumnDescriptor column, PageHeaderWithOffset pageHeader, BytesInputDecompressor decompressor) throws IOException {
    in.setPosition(pageHeader.getOffset());
    final byte[] data = new byte[pageHeader.getPageHeader().getCompressed_page_size()];
    int read = in.read(data);
    if (read != data.length) {
      throw new IOException(format("Failed to read dictionary page, read %d bytes, expected %d", read, data.length));
    }
    final DictionaryPage dictionaryPage = new DictionaryPage(
      decompressor.decompress(BytesInput.from(data), pageHeader.getPageHeader().getUncompressed_page_size()),
      pageHeader.getPageHeader().getDictionary_page_header().getNum_values(),
      CONVERTER.getEncoding(pageHeader.getPageHeader().getDictionary_page_header().getEncoding()));
    return dictionaryPage.getEncoding().initDictionary(column, dictionaryPage);
  }

  public static void main(String[] args) {
    try (final BufferAllocator bufferAllocator = new RootAllocator(VM.getMaxDirectMemory())) {
      final Configuration fsConf = new Configuration();
      final FileSystem fs = HadoopFileSystem.getLocal(fsConf);
      final Path filePath = Path.of(args[0]);
      final CompressionCodecFactory codecFactory = CodecFactory.createDirectCodecFactory(fsConf, new ParquetDirectByteBufferAllocator(bufferAllocator), 0);
      final Pair<Map<ColumnDescriptor, Dictionary>, Set<ColumnDescriptor>> dictionaries = readDictionaries(fs, filePath, codecFactory);
      for (Map.Entry<ColumnDescriptor, Dictionary> entry :  dictionaries.getLeft().entrySet()) {
        printDictionary(entry.getKey(), entry.getValue());
      }
      System.out.println("Binary columns which are not dictionary encoded: " + dictionaries.getRight());
    } catch (IOException ioe) {
      logger.error("Failed ", ioe);
    }
  }

  // utility to dump dictionary contents
  public static void printDictionary(ColumnDescriptor columnDescriptor, Dictionary localDictionary) {
    System.out.println("Dictionary for column " + columnDescriptor.toString());
    for (int i = 0; i < localDictionary.getMaxId(); ++i) {
      switch (columnDescriptor.getType()) {
        case INT32:
          System.out.println(format("%d: %d", i, localDictionary.decodeToInt(i)));
          break;
        case INT64:
          System.out.println(format("%d: %d", i, localDictionary.decodeToLong(i)));
          break;
        case INT96:
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          System.out.println(format("%d: %s", i, new String(localDictionary.decodeToBinary(i).getBytesUnsafe())));
          break;
        case FLOAT:
          System.out.println(format("%d: %f", i, localDictionary.decodeToFloat(i)));
          break;
        case DOUBLE:
          System.out.println(format("%d: %f", i, localDictionary.decodeToDouble(i)));
          break;
        case BOOLEAN:
          System.out.println(format("%d: %b", i, localDictionary.decodeToBoolean(i)));
          break;
        default:
          break;
      }
    }
  }
}
