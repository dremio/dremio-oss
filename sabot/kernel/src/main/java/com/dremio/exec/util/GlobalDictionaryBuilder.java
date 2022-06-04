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
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.io.api.Binary;

import com.dremio.common.VM;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.cache.VectorAccessibleSerializable;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.WritableBatch;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.io.file.PathFilters;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Global dictionary builder.
 * Global dictionaries are versioned and each version has a separate directory.
 * - table root path (ex /data/foo/)
 *  - _dicts_0       (dictionaries for version 0)
 *     - _c1.dict
 *     - _c2.dict
 *   - _dicts_1       (dictionaries for version 1)
 *     - _c1.dict
 *     - _c2.dict
 *     - _c3.dict
 * Dictionary versions are monotonically increasing.
 * If a higher version of dictionary is present then dictionaries for a lower version can not be created.
 */

public class GlobalDictionaryBuilder {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GlobalDictionaryBuilder.class);
  private static final Predicate<Path> PARQUET_FILES_FILTER = PathFilters.endsWith(".parquet");

  public static final String DICTIONARY_TEMP_ROOT_PREFIX = "_tmp_dicts"; // dictionaries in flight
  public static final String DICTIONARY_ROOT_PREFIX = "_dicts"; // dictionaries
  public static final Pattern DICTIONARY_VERSION_PATTERN = Pattern.compile("^" + DICTIONARY_ROOT_PREFIX + "(\\d+)+$");
  public static final Predicate<Path> DICTIONARY_ROOT_FILTER = PathFilters.startsWith(DICTIONARY_ROOT_PREFIX);

  public static final String DICTIONARY_FILES_EXTENSION = "dict";
  public static final Predicate<Path> DICTIONARY_FILES_FILTER = PathFilters.endsWith("." + DICTIONARY_FILES_EXTENSION);
  public static final Pattern DICTIONARY_FILES_PATTERN = Pattern.compile("_(.*?)." + DICTIONARY_FILES_EXTENSION);

  public static String dictionaryFileName(String columnFullPath) {
    return format("_%s.%s", columnFullPath, DICTIONARY_FILES_EXTENSION);
  }

  public static String dictionaryFileName(ColumnDescriptor columnDescriptor) {
    return format("_%s.%s", SchemaPath.getCompoundPath(columnDescriptor.getPath()).getAsUnescapedPath(), DICTIONARY_FILES_EXTENSION);
  }

  public static Path dictionaryFilePath(Path dictionaryRootDir, String columnFullPath) {
    return dictionaryRootDir.resolve(dictionaryFileName(columnFullPath));
  }

  public static Path dictionaryFilePath(Path dictionaryRootDir, ColumnDescriptor columnDescriptor) {
    return dictionaryRootDir.resolve(dictionaryFileName(columnDescriptor));
  }

  public static String getColumnFullPath(String dictionaryFileName) {
    final Matcher matcher = DICTIONARY_FILES_PATTERN.matcher(dictionaryFileName);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  /**
   * @param fs Filesystem
   * @param tableDir root of parquet table
   * @return the highest dictionary version found, -1 if no dictionaries are present
   * @throws IOException
   */
  public static long getDictionaryVersion(FileSystem fs, Path tableDir) throws IOException {
    long maxVersion = -1;
    try (DirectoryStream<FileAttributes> stream = fs.list(tableDir, DICTIONARY_ROOT_FILTER)) {
      for (FileAttributes attributes : stream) {
        if (!attributes.isDirectory()) {
          continue;
        }
        Matcher matcher = DICTIONARY_VERSION_PATTERN.matcher(attributes.getPath().getName());
        if (!matcher.find()) {
          continue;
        }

        try {
          final long version = Long.parseLong(matcher.group(1));
          if (version > maxVersion) {
            maxVersion = version;
          }
        } catch (NumberFormatException nfe) {
        }
      }
    }
    return maxVersion;
  }

  public static String dictionaryRootDirName(long version) {
    return DICTIONARY_ROOT_PREFIX  + version;
  }

  public static Path getDictionaryVersionedRootPath(FileSystem fs, Path tableDir, long version) throws IOException {
    final Path dictionaryRootDir = tableDir.resolve(dictionaryRootDirName(version));
    if (version != -1 && fs.exists(dictionaryRootDir) && fs.isDirectory(dictionaryRootDir)) {
      return dictionaryRootDir;
    }
    return null;
  }

  private static Path createTempRootDir(FileSystem fs, Path tableDir, long version) throws IOException {
    final Path tmpPath = tableDir.resolve(format("%s%d_%s", DICTIONARY_TEMP_ROOT_PREFIX, version, UUID.randomUUID().toString()));
    fs.mkdirs(tmpPath);
    return tmpPath;
  }

  public static Path createDictionaryVersionedRootPath(FileSystem fs, Path tableDir, long nextVersion, Path tmpDictionaryRootPath) throws IOException {
    final Path dictionaryRootDir = tableDir.resolve(dictionaryRootDirName(nextVersion));
    if (fs.exists(dictionaryRootDir)) {
      throw new IOException(format("Dictionary already exists for version: %d, path: %s", nextVersion, dictionaryRootDir));
    }
    final long currentVersion = getDictionaryVersion(fs, tableDir);
    if (currentVersion > nextVersion) {
      throw new IOException(format("Dictionary exists with a higher version %d, attempted version %d", currentVersion, nextVersion));
    }
    if (!fs.rename(tmpDictionaryRootPath, dictionaryRootDir)) {
      throw new IOException(format("Failed to rename temporary dictionaries at %s to %s, for version %d", tmpDictionaryRootPath, dictionaryRootDir, nextVersion));
    }
    return dictionaryRootDir;
  }


  public static Path getDictionaryFile(FileSystem fs, Path dictRootDir, String columnFullPath) throws IOException {
    Path f = dictRootDir.resolve(dictionaryFileName(columnFullPath));
    if (fs.exists(f)) {
      return f;
    }
    return null;
  }

  public static Path getDictionaryFile(FileSystem fs, Path dictRootDir, ColumnDescriptor columnDescriptor) throws IOException {
    Path f = dictionaryFilePath(dictRootDir, columnDescriptor);
    if (fs.exists(f)) {
      return f;
    }
    return null;
  }

  public static Map<String, Path> listDictionaryFiles(FileSystem fs, Path dictRootDir) throws IOException {
    final Map<String, Path> files = Maps.newHashMap();
    try(DirectoryStream<FileAttributes> stream = fs.list(dictRootDir, DICTIONARY_FILES_FILTER)) {
      for (FileAttributes fileAttributes : stream) {
        files.put(getColumnFullPath(fileAttributes.getPath().getName()), fileAttributes.getPath());
      }
    }
    return files;
  }

  public static VectorContainer readDictionary(FileSystem fs,
                                               Path dictionaryRootDir,
                                               String columnFullPath,
                                               BufferAllocator bufferAllocator) throws IOException {
    return readDictionary(fs, dictionaryFilePath(dictionaryRootDir, columnFullPath), bufferAllocator);
  }

  public static VectorContainer readDictionary(FileSystem fs,
                                               Path dictionaryRootDir,
                                               ColumnDescriptor columnDescriptor,
                                               BufferAllocator bufferAllocator) throws IOException {
    return readDictionary(fs, dictionaryFilePath(dictionaryRootDir, columnDescriptor), bufferAllocator);
  }

  public static VectorContainer readDictionary(FileSystem fs,
                                               Path dictionaryFile,
                                               BufferAllocator bufferAllocator) throws IOException {
    final VectorAccessibleSerializable vectorAccessibleSerializable = new VectorAccessibleSerializable(bufferAllocator);
    try (final FSInputStream in = fs.open(dictionaryFile)) {
      vectorAccessibleSerializable.readFromStream(in);
      return vectorAccessibleSerializable.get();
    }
  }

  /**
   * Updates existing global dictionaries for a parquet table.
   * @param codecFactory compression codec factory
   * @param fs filesystem
   * @param tableDir root directory for given table that has parquet files
   * @param partitionDir newly added partition directory.
   * @param bufferAllocator memory allocator
   * @return GlobalDictionariesInfo that has dictionary versiom, root path and columns along with path to dictionary files.
   * @throws IOException
   */
  public static GlobalDictionariesInfo updateGlobalDictionaries(CompressionCodecFactory codecFactory, FileSystem fs, Path tableDir, Path partitionDir, BufferAllocator bufferAllocator) throws IOException {
    final Map<ColumnDescriptor, Path> globalDictionaries = Maps.newHashMap();

    final long dictionaryVersion = getDictionaryVersion(fs, tableDir);
    final long nextDictionaryVersion = dictionaryVersion + 1;
    final Path dictionaryRootDir = getDictionaryVersionedRootPath(fs, tableDir, dictionaryVersion);
    final Path tmpDictionaryRootDir = createTempRootDir(fs, tableDir, nextDictionaryVersion);

    final Map<ColumnDescriptor, List<Dictionary>> allDictionaries;
    try (final DirectoryStream<FileAttributes> stream = fs.list(partitionDir, PARQUET_FILES_FILTER)) {
      allDictionaries = readLocalDictionaries(codecFactory, fs, stream, bufferAllocator);
    }

    for (Map.Entry<ColumnDescriptor, List<Dictionary>> entry : allDictionaries.entrySet()) {
      final ColumnDescriptor columnDescriptor = entry.getKey();
      Path dictionaryFile = null;
      if (dictionaryRootDir != null) {
        dictionaryFile = getDictionaryFile(fs, dictionaryRootDir, columnDescriptor);
      }
      if (dictionaryFile == null) {
        final Path newDictionaryFile = dictionaryFilePath(tmpDictionaryRootDir, columnDescriptor);
        logger.debug("Creating a new global dictionary for {} with version {}", columnDescriptor.toString(), nextDictionaryVersion);
        createDictionaryFile(fs, newDictionaryFile, columnDescriptor, entry.getValue(), null, bufferAllocator);
        globalDictionaries.put(columnDescriptor, newDictionaryFile);
      } else {
        // read previously created global dictionary and add new values to it.
        try (final VectorContainer vectorContainer = readDictionary(fs, dictionaryFile, bufferAllocator)) {
          final Path newDictionaryFile = dictionaryFilePath(tmpDictionaryRootDir, columnDescriptor);
          logger.debug("Updating global dictionary for {} with version {}", columnDescriptor.toString(), nextDictionaryVersion);
          createDictionaryFile(fs, newDictionaryFile, columnDescriptor, entry.getValue(), vectorContainer, bufferAllocator);
          globalDictionaries.put(columnDescriptor, newDictionaryFile);
        }
      }
    }
    final Path nextDictionaryRootDir = createDictionaryVersionedRootPath(fs, tableDir, nextDictionaryVersion, tmpDictionaryRootDir);
    return new GlobalDictionariesInfo(globalDictionaries, nextDictionaryRootDir,  nextDictionaryVersion);
  }

  /**
   * Builds a global dictionary for parquet table for BINARY or FIXED_LEN_BYTE_ARRAY column types.
   * It will remove exiting dictionaries if present and create new ones.
   * @param codec compression codec factory
   * @param fs filesystem
   * @param tableDir root directory for given table that has parquet files
   * @param bufferAllocator memory allocator
   * @return GlobalDictionariesInfo that has dictionary version, root path and columns along with path to dictionary files.
   * @throws IOException
   */
  public static GlobalDictionariesInfo createGlobalDictionaries(CompressionCodecFactory codecFactory,
      FileSystem fs, Path tableDir, BufferAllocator bufferAllocator) throws IOException {
    final Map<ColumnDescriptor, Path> globalDictionaries = Maps.newHashMap();
    final Map<ColumnDescriptor, List<Dictionary>> allDictionaries;
    try (final DirectoryStream<FileAttributes> stream = fs.list(tableDir, PARQUET_FILES_FILTER)) {
      allDictionaries = readLocalDictionaries(codecFactory, fs, stream, bufferAllocator);
    }

    final long dictionaryVersion = getDictionaryVersion(fs, tableDir) + 1;
    final Path tmpDictionaryRootDir = createTempRootDir(fs, tableDir, dictionaryVersion);
    logger.debug("Building global dictionaries for columns {} with version {}", allDictionaries.keySet(), dictionaryVersion);

    // Sort all local dictionaries and write it to file with an index if needed
    for (Map.Entry<ColumnDescriptor, List<Dictionary>> entry : allDictionaries.entrySet()) {
      final ColumnDescriptor columnDescriptor = entry.getKey();
      final Path dictionaryFile = dictionaryFilePath(tmpDictionaryRootDir, columnDescriptor);
      logger.debug("Creating a new global dictionary for {} with version {}", columnDescriptor.toString(), dictionaryVersion);
      createDictionaryFile(fs, dictionaryFile, columnDescriptor, entry.getValue(), null, bufferAllocator);
      globalDictionaries.put(columnDescriptor, dictionaryFile);
    }
    final Path finalDictionaryRootDir = createDictionaryVersionedRootPath(fs, tableDir, dictionaryVersion, tmpDictionaryRootDir);
    return new GlobalDictionariesInfo(globalDictionaries, finalDictionaryRootDir,  dictionaryVersion);
  }

  private static Map<ColumnDescriptor, List<Dictionary>> readLocalDictionaries(CompressionCodecFactory codecFactory, FileSystem fs, Iterable<FileAttributes> files, BufferAllocator allocator) throws IOException{
    final Set<ColumnDescriptor> columnsToSkip = Sets.newHashSet(); // These columns are not dictionary encoded in at least one file.
    final Map<ColumnDescriptor, List<Dictionary>> allDictionaries = Maps.newHashMap();
    for (FileAttributes fileAttributes : files) {
      logger.debug("Scanning file {}", fileAttributes.getPath());
      final Pair<Map<ColumnDescriptor, Dictionary>, Set<ColumnDescriptor>> localDictionaries = LocalDictionariesReader.readDictionaries(
        fs, fileAttributes.getPath(), codecFactory);

      // Skip columns which are not dictionary encoded
      for (ColumnDescriptor skippedColumn : localDictionaries.getRight()) {
        columnsToSkip.add(skippedColumn);
        allDictionaries.remove(skippedColumn);
      }

      for (final Map.Entry<ColumnDescriptor, Dictionary> entry : localDictionaries.getLeft().entrySet()) {
        if (!columnsToSkip.contains(entry.getKey())) {
          if (allDictionaries.containsKey(entry.getKey())) {
            allDictionaries.get(entry.getKey()).add(entry.getValue());
          } else {
            allDictionaries.put(entry.getKey(), Lists.newArrayList(entry.getValue()));
          }
        }
      }
    }
    logger.debug("Skipping columns {}", columnsToSkip);
    return allDictionaries;
  }

  private static void createDictionaryFile(FileSystem fs, Path dictionaryFile, ColumnDescriptor columnDescriptor, List<Dictionary> dictionaries,
                                           VectorContainer existingDict, BufferAllocator bufferAllocator) throws IOException {
    try (final OutputStream out = fs.create(dictionaryFile, true)) {
      switch (columnDescriptor.getType()) {
        case INT32: {
          try (final VectorContainer dict = buildIntegerGlobalDictionary(dictionaries, existingDict, columnDescriptor, bufferAllocator)) {
            writeDictionary(out, dict, dict.getRecordCount(), bufferAllocator);
          }
        }
        break;

        case INT64: {
          try (final VectorContainer dict = buildLongGlobalDictionary(dictionaries, existingDict, columnDescriptor, bufferAllocator)) {
            writeDictionary(out, dict, dict.getRecordCount(), bufferAllocator);
          }
        }
        break;

        case INT96:
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY: {
          try (final VectorContainer dict = buildBinaryGlobalDictionary(dictionaries, existingDict, columnDescriptor, bufferAllocator)) {
            writeDictionary(out, dict, dict.getRecordCount(), bufferAllocator);
          }
        }
        break;

        case FLOAT: {
          try (final VectorContainer dict = buildFloatGlobalDictionary(dictionaries, existingDict, columnDescriptor, bufferAllocator)) {
            writeDictionary(out, dict, dict.getRecordCount(), bufferAllocator);
          }
        }
        break;

        case DOUBLE: {
          try (final VectorContainer dict = buildDoubleGlobalDictionary(dictionaries, existingDict, columnDescriptor, bufferAllocator)) {
            writeDictionary(out, dict, dict.getRecordCount(), bufferAllocator);
          }
        }
        break;

        default:
          throw new IOException("Invalid data type " + columnDescriptor.getType());
      }
    }
  }

  private static VectorContainer buildIntegerGlobalDictionary(List<Dictionary> dictionaries, VectorContainer existingDict, ColumnDescriptor columnDescriptor, BufferAllocator bufferAllocator) {
    final Field field = new Field(SchemaPath.getCompoundPath(columnDescriptor.getPath()).getAsUnescapedPath(), new FieldType(true, new ArrowType.Int(32, true), null), null);
    final VectorContainer input = new VectorContainer(bufferAllocator);
    final IntVector intVector = input.addOrGet(field);
    intVector.allocateNew();
    final SortedSet<Integer> values = Sets.newTreeSet();
    for (Dictionary dictionary : dictionaries) {
      for (int i = 0; i <= dictionary.getMaxId(); ++i) {
        values.add(dictionary.decodeToInt(i));
      }
    }
    if (existingDict != null) {
      final IntVector existingDictValues = existingDict.getValueAccessorById(IntVector.class, 0).getValueVector();
      for (int i = 0; i < existingDict.getRecordCount(); ++i) {
        values.add(existingDictValues.get(i));
      }
    }
    final Iterator<Integer> iter = values.iterator();
    int recordCount = 0;
    while (iter.hasNext()) {
      intVector.setSafe(recordCount++, iter.next());
    }
    intVector.setValueCount(recordCount);
    input.setRecordCount(recordCount);
    input.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return input;
  }

  private static VectorContainer buildLongGlobalDictionary(List<Dictionary> dictionaries, VectorContainer existingDict, ColumnDescriptor columnDescriptor, BufferAllocator bufferAllocator) {
    final Field field = new Field(SchemaPath.getCompoundPath(columnDescriptor.getPath()).getAsUnescapedPath(), new FieldType(true, new ArrowType.Int(64, true), null), null);
    final VectorContainer input = new VectorContainer(bufferAllocator);
    final BigIntVector longVector = input.addOrGet(field);
    longVector.allocateNew();
    SortedSet<Long> values = Sets.newTreeSet();
    for (Dictionary dictionary : dictionaries) {
      for (int i = 0; i <= dictionary.getMaxId(); ++i) {
        values.add(dictionary.decodeToLong(i));
      }
    }
    if (existingDict != null) {
      final BigIntVector existingDictValues = existingDict.getValueAccessorById(BigIntVector.class, 0).getValueVector();
      for (int i = 0; i < existingDict.getRecordCount(); ++i) {
        values.add(existingDictValues.get(i));
      }
    }
    final Iterator<Long> iter = values.iterator();
    int recordCount = 0;
    while (iter.hasNext()) {
      longVector.setSafe(recordCount++, iter.next());
    }
    longVector.setValueCount(recordCount);
    input.setRecordCount(recordCount);
    input.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return input;
  }

  private static VectorContainer buildDoubleGlobalDictionary(List<Dictionary> dictionaries, VectorContainer existingDict, ColumnDescriptor columnDescriptor, BufferAllocator bufferAllocator) {
    final Field field = new Field(SchemaPath.getCompoundPath(columnDescriptor.getPath()).getAsUnescapedPath(), new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null);
    final VectorContainer input = new VectorContainer(bufferAllocator);
    final Float8Vector doubleVector = input.addOrGet(field);
    doubleVector.allocateNew();
    SortedSet<Double> values = Sets.newTreeSet();
    for (Dictionary dictionary : dictionaries) {
      for (int i = 0; i <= dictionary.getMaxId(); ++i) {
        values.add(dictionary.decodeToDouble(i));
      }
    }
    if (existingDict != null) {
      final Float8Vector existingDictValues = existingDict.getValueAccessorById(Float8Vector.class, 0).getValueVector();
      for (int i = 0; i < existingDict.getRecordCount(); ++i) {
        values.add(existingDictValues.get(i));
      }
    }
    final Iterator<Double> iter = values.iterator();
    int recordCount = 0;
    while (iter.hasNext()) {
      doubleVector.setSafe(recordCount++, iter.next());
    }
    doubleVector.setValueCount(recordCount);
    input.setRecordCount(recordCount);
    input.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return input;
  }

  private static VectorContainer buildFloatGlobalDictionary(List<Dictionary> dictionaries, VectorContainer existingDict, ColumnDescriptor columnDescriptor, BufferAllocator bufferAllocator) {
    final Field field = new Field(SchemaPath.getCompoundPath(columnDescriptor.getPath()).getAsUnescapedPath(), new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null), null);
    final VectorContainer input = new VectorContainer(bufferAllocator);
    final Float4Vector floatVector = input.addOrGet(field);
    floatVector.allocateNew();
    SortedSet<Float> values = Sets.newTreeSet();
    for (Dictionary dictionary : dictionaries) {
      for (int i = 0; i <= dictionary.getMaxId(); ++i) {
        values.add(dictionary.decodeToFloat(i));
      }
    }
    if (existingDict != null) {
      final Float4Vector existingDictValues = existingDict.getValueAccessorById(Float4Vector.class, 0).getValueVector();
      for (int i = 0; i < existingDict.getRecordCount(); ++i) {
        values.add(existingDictValues.get(i));
      }
    }
    final Iterator<Float> iter = values.iterator();
    int recordCount = 0;
    while (iter.hasNext()) {
      floatVector.setSafe(recordCount++, iter.next());
    }
    floatVector.setValueCount(recordCount);
    input.setRecordCount(recordCount);
    input.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return input;
  }

  private static VectorContainer buildBinaryGlobalDictionary(List<Dictionary> dictionaries, VectorContainer existingDict, ColumnDescriptor columnDescriptor, BufferAllocator bufferAllocator) {
    final Field field = new Field(SchemaPath.getCompoundPath(columnDescriptor.getPath()).getAsUnescapedPath(), new FieldType(true, new ArrowType.Binary(), null), null);
    final VectorContainer input = new VectorContainer(bufferAllocator);
    final VarBinaryVector binaryVector = input.addOrGet(field);
    binaryVector.allocateNew();
    final SortedSet<Binary> values = new TreeSet<>();
    for (Dictionary dictionary : dictionaries) {
      for (int i = 0; i <= dictionary.getMaxId(); ++i) {
        values.add(dictionary.decodeToBinary(i));
      }
    }
    if (existingDict != null) {
      final VarBinaryVector existingDictValues = existingDict.getValueAccessorById(VarBinaryVector.class, 0).getValueVector();
      for (int i = 0; i < existingDict.getRecordCount(); ++i) {
        values.add(Binary.fromConstantByteArray(existingDictValues.get(i)));
      }
    }
    final Iterator<Binary> iter = values.iterator();
    int recordCount = 0;
    while (iter.hasNext()) {
      final byte[] data = iter.next().getBytes();
      binaryVector.setSafe(recordCount++, data, 0, data.length);
    }
    binaryVector.setValueCount(recordCount);
    input.setRecordCount(recordCount);
    input.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return input;
  }


  public static void writeDictionary(OutputStream out,
                                     VectorAccessible input, int recordCount,
                                     BufferAllocator bufferAllocator) throws IOException {
    final WritableBatch writableBatch = WritableBatch.getBatchNoHVWrap(recordCount, input, false /* isSv2 */);
    final VectorAccessibleSerializable serializer = new VectorAccessibleSerializable(writableBatch, bufferAllocator);
    serializer.writeToStream(out);
  }

  public static void main(String []args) {
    try (final BufferAllocator bufferAllocator = new RootAllocator(VM.getMaxDirectMemory())) {
      final Path tableDir  = Path.of(args[0]);
      final Configuration conf = new Configuration();
      final CompressionCodecFactory codecFactory = CodecFactory.createDirectCodecFactory(conf, new ParquetDirectByteBufferAllocator(bufferAllocator), 0);
      final FileSystem fs = HadoopFileSystem.get(tableDir, conf);
      if (fs.exists(tableDir) && fs.isDirectory(tableDir)) {
        Map<ColumnDescriptor, Path> dictionaryEncodedColumns = createGlobalDictionaries(codecFactory, fs, tableDir, bufferAllocator).getColumnsToDictionaryFiles();
        long version = getDictionaryVersion(fs, tableDir);
        Path dictionaryRootDir = getDictionaryVersionedRootPath(fs, tableDir, version);
        for (ColumnDescriptor columnDescriptor: dictionaryEncodedColumns.keySet()) {
          final VectorContainer data = readDictionary(fs, dictionaryRootDir, columnDescriptor, bufferAllocator);
          System.out.println("Dictionary for column [" + columnDescriptor.toString() + " size " + data.getRecordCount());
          BatchPrinter.printBatch(data);
          data.clear();
        }
      }
    } catch (IOException ioe) {
      logger.error("Failed ", ioe);
    }
  }

  public static class GlobalDictionariesInfo {
    final private long version;
    final private Path rootPath;
    final Map<ColumnDescriptor, Path> columnsToDictionaryFiles;

    public GlobalDictionariesInfo(Map<ColumnDescriptor, Path> columnsToDictionaryFiles, Path rootPath, long version) {
      this.columnsToDictionaryFiles = columnsToDictionaryFiles;
      this.rootPath = rootPath;
      this.version = version;
    }

    public long getVersion() {
      return version;
    }

    public Path getRootPath() {
      return rootPath;
    }

    public Map<ColumnDescriptor, Path> getColumnsToDictionaryFiles() {
      return columnsToDictionaryFiles;
    }
  }
}
