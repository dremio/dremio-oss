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
package com.dremio.exec.store.hive.exec;

import static com.dremio.exec.store.hive.HiveUtilities.addProperties;
import static com.dremio.exec.store.hive.HiveUtilities.createSerDe;
import static com.dremio.exec.store.hive.HiveUtilities.getInputFormatClass;
import static com.dremio.exec.store.hive.HiveUtilities.getStructOI;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.orc.OrcConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.hive.HivePf4jPlugin;
import com.dremio.exec.store.hive.HiveSettings;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.hive.proto.HiveReaderProto.HiveSplitXattr;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.PartitionXattr;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;

/**
 * Helper class for {@link HiveScanBatchCreator} to create a {@link ProducerOperator} that uses readers provided by
 * Hive.
 */
class ScanWithHiveReader {
  private static final Logger logger = LoggerFactory.getLogger(ScanWithHiveReader.class);

  /**
   * Use different classes for different Hive native formats:
   * ORC, AVRO, RCFFile, Text and Parquet.
   * If input format is none of them falls to default reader.
   */
  private static final Map<String, Class<? extends HiveAbstractReader>> readerMap = new HashMap<>();
  static {
    readerMap.put(OrcInputFormat.class.getCanonicalName(), HiveOrcReader.class);
    readerMap.put(AvroContainerInputFormat.class.getCanonicalName(), HiveAvroReader.class);
    readerMap.put(RCFileInputFormat.class.getCanonicalName(), HiveRCFileReader.class);
    readerMap.put(MapredParquetInputFormat.class.getCanonicalName(), HiveParquetReader.class);
    readerMap.put(TextInputFormat.class.getCanonicalName(), HiveTextReader.class);
  }

  private static final boolean isNativeZlibLoaded;
  static {
    boolean isLoaded;
    try {
      Method m = ZlibDecompressor.class.getDeclaredMethod("isNativeZlibLoaded");
      m.setAccessible(true);
      isLoaded = (boolean) m.invoke(null);
    } catch (ReflectiveOperationException e) {
      // ignore
      logger.warn("Cannot detect if Zlib native codec is properly loaded", e);
      isLoaded = true;
    }
    isNativeZlibLoaded = isLoaded;
  }

  private static Class<? extends HiveAbstractReader> getNativeReaderClass(Optional<String> formatName,
                                                                          OptionManager options, Configuration configuration, boolean isTransactional) {
    if (!formatName.isPresent()) {
      return HiveDefaultReader.class;
    }

    Class<? extends HiveAbstractReader> readerClass = readerMap.get(formatName.get());
    if (readerClass == HiveOrcReader.class) {
      // Validate reader
      if (OrcConf.USE_ZEROCOPY.getBoolean(configuration)) {
        if (!NativeCodeLoader.isNativeCodeLoaded()) {
          throw UserException.dataReadError()
              .message("Hadoop native library is required for Hive ORC data, but is not loaded").build(logger);
        }
        // TODO: find a way to access compression codec information?
        if (!SnappyDecompressor.isNativeCodeLoaded()) {
          throw UserException.dataReadError()
            .message("Snappy native library is required for Hive ORC data, but is not loaded").build(logger);
        }

        if (!isNativeZlibLoaded) {
          throw UserException
          .dataReadError()
          .message("Zlib native library is required for Hive ORC data, but is not loaded")
          .build(logger);
        }
      }

      if (new HiveSettings(options).vectorizeOrcReaders() && !isTransactional) {
        // We don't use vectorized ORC reader if the table is
        // a transactional Hive table
        return HiveORCVectorizedReader.class;
      }
    }

    if (readerClass == null) {
      return HiveDefaultReader.class;
    }

    return readerClass;
  }

  private static Constructor<? extends HiveAbstractReader> getNativeReaderCtor(Class<? extends HiveAbstractReader> clazz)
      throws NoSuchMethodException {
    return clazz.getConstructor(HiveTableXattr.class, SplitAndPartitionInfo.class, List.class, OperatorContext.class,
                                JobConf.class, SerDe.class, StructObjectInspector.class, SerDe.class, StructObjectInspector.class,
                                ScanFilter.class, Collection.class, UserGroupInformation.class);
  }

  static RecordReaderIterator createReaders(
      final HiveConf hiveConf,
      final FragmentExecutionContext fragmentExecContext,
      final OperatorContext context,
      final HiveProxyingSubScan config,
      final HiveTableXattr tableXattr,
      final CompositeReaderConfig compositeReader,
      final UserGroupInformation readerUGI,
      List<SplitAndPartitionInfo> splits){

    if(splits.isEmpty()) {
      return RecordReaderIterator.from(Collections.emptyIterator());
    }

    final List<Pair<SplitAndPartitionInfo, Supplier<RecordReader>>> readers = FluentIterable.from(splits).transform(split ->
            readerUGI.doAs((PrivilegedAction<Pair<SplitAndPartitionInfo, Supplier<RecordReader>>>) () -> {
              final Supplier<RecordReader> innerReader = () -> compositeReader.wrapIfNecessary(context.getAllocator(),
                      getRecordReader(tableXattr, context, hiveConf, split, compositeReader, config, readerUGI),
                      split);
              return Pair.of(split, innerReader);
            })).toList();
    return new HiveRecordReaderIterator(context, compositeReader, readers);
  }

  private static RecordReader getRecordReader(HiveTableXattr tableXattr,
                                              OperatorContext context, HiveConf hiveConf,
                                              SplitAndPartitionInfo split, CompositeReaderConfig compositeReader,
                                              HiveProxyingSubScan config, UserGroupInformation readerUgi) {
    try(Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      final HiveSplitXattr splitXattr = HiveSplitXattr.parseFrom(split.getDatasetSplitInfo().getExtendedProperty());
      final JobConf jobConf = new JobConf(hiveConf);
      final Properties tableProperties = new Properties();
      addProperties(jobConf, tableProperties, HiveReaderProtoUtil.getTableProperties(tableXattr));

      final boolean isTransactional = AcidUtils.isTablePropertyTransactional(jobConf);
      final boolean isPartitioned = config.getPartitionColumns()!=null && config.getPartitionColumns().size() > 0;
      final Optional<String> tableInputFormat = HiveReaderProtoUtil.getTableInputFormat(tableXattr);

      final SerDe tableSerDe = createSerDe(jobConf, HiveReaderProtoUtil.getTableSerializationLib(tableXattr).get(),
              tableProperties);
      final StructObjectInspector tableOI = getStructOI(tableSerDe);
      final SerDe partitionSerDe;
      final StructObjectInspector partitionOI;

      boolean hasDeltas = false;
      if (isTransactional) {
        InputSplit inputSplit = HiveUtilities.deserializeInputSplit(splitXattr.getInputSplit());
        if (inputSplit instanceof OrcSplit) {
          hasDeltas = hasDeltas((OrcSplit) inputSplit);
        }
      }

      final Class<? extends HiveAbstractReader> tableReaderClass =
              getNativeReaderClass(tableInputFormat, context.getOptions(), hiveConf, isTransactional && hasDeltas);

      final Constructor<? extends HiveAbstractReader> tableReaderCtor = getNativeReaderCtor(tableReaderClass);

      Constructor<? extends HiveAbstractReader> readerCtor = tableReaderCtor;
      // It is possible to for a partition to have different input format than table input format.
      if (isPartitioned) {
        final List<Prop> partitionPropertiesList;
        final Properties partitionProperties = new Properties();
        final Optional<String> partitionInputFormat;
        final Optional<String> partitionStorageHandlerName;
        // First add table properties and then add partition properties. Partition properties override table properties.
        addProperties(jobConf, partitionProperties, HiveReaderProtoUtil.getTableProperties(tableXattr));

        // If Partition Properties are stored in DatasetMetadata (Pre 3.2.0)
        if (HiveReaderProtoUtil.isPreDremioVersion3dot2dot0LegacyFormat(tableXattr)) {
          logger.debug("Reading partition properties from DatasetMetadata");
          partitionPropertiesList = HiveReaderProtoUtil.getPartitionProperties(tableXattr, splitXattr.getPartitionId());
          addProperties(jobConf, partitionProperties, partitionPropertiesList);
          partitionSerDe =
                  createSerDe(jobConf,
                          HiveReaderProtoUtil.getPartitionSerializationLib(tableXattr, splitXattr.getPartitionId()).get(),
                          partitionProperties
                  );
          partitionInputFormat = HiveReaderProtoUtil.getPartitionInputFormat(tableXattr, splitXattr.getPartitionId());
          partitionStorageHandlerName = HiveReaderProtoUtil.getPartitionStorageHandler(tableXattr, splitXattr.getPartitionId());

        } else {
          logger.debug("Reading partition properties from PartitionChunk");
          final PartitionXattr partitionXattr = HiveReaderProtoUtil.getPartitionXattr(split);
          partitionPropertiesList = HiveReaderProtoUtil.getPartitionProperties(tableXattr, partitionXattr);
          addProperties(jobConf, partitionProperties, partitionPropertiesList);
          partitionSerDe =
                  createSerDe(jobConf,
                          HiveReaderProtoUtil.getPartitionSerializationLib(tableXattr, partitionXattr),
                          partitionProperties
                  );
          partitionInputFormat = HiveReaderProtoUtil.getPartitionInputFormat(tableXattr, partitionXattr);
          partitionStorageHandlerName = HiveReaderProtoUtil.getPartitionStorageHandler(tableXattr, partitionXattr);
        }

        jobConf.setInputFormat(getInputFormatClass(jobConf, partitionInputFormat, partitionStorageHandlerName));
        partitionOI = getStructOI(partitionSerDe);

        if (!partitionInputFormat.equals(tableInputFormat) || isTransactional && hasDeltas) {
          final Class<? extends HiveAbstractReader> partitionReaderClass = getNativeReaderClass(
                  partitionInputFormat, context.getOptions(), jobConf, isTransactional);
          readerCtor = getNativeReaderCtor(partitionReaderClass);
        }
      } else {
        partitionSerDe = null;
        partitionOI = null;
        jobConf.setInputFormat(getInputFormatClass(jobConf, tableInputFormat, HiveReaderProtoUtil.getTableStorageHandler(tableXattr)));
      }

      return readerCtor.newInstance(tableXattr, split,
              compositeReader.getInnerColumns(), context, jobConf, tableSerDe, tableOI, partitionSerDe,
              partitionOI, config.getFilter(), config.getReferencedTables(), readerUgi);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static boolean hasDeltas(OrcSplit orcSplit) throws IOException {
    final Path path = orcSplit.getPath();
    final Path root;

    // If the split has a base, extract the base file size, bucket and root path info.
    if (orcSplit.hasBase()) {
      if (orcSplit.isOriginal()) {
        root = path.getParent();
      } else {
        root = path.getParent().getParent();
      }
    } else {
      root = path;
    }

    final Path[] deltas = AcidUtils.deserializeDeltas(root, orcSplit.getDeltas());
    return deltas.length > 0;
  }
}
