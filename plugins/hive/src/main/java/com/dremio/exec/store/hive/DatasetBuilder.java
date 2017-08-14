/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.hive;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.DateTimes;
import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TimedRunnable;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.implicit.DecimalTools;
import com.dremio.hive.proto.HiveReaderProto.FileSystemCachedEntity;
import com.dremio.hive.proto.HiveReaderProto.FileSystemPartitionUpdateKey;
import com.dremio.hive.proto.HiveReaderProto.HiveReadSignature;
import com.dremio.hive.proto.HiveReaderProto.HiveReadSignatureType;
import com.dremio.hive.proto.HiveReaderProto.HiveSplitXattr;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.PartitionProp;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.hive.proto.HiveReaderProto.ReaderType;
import com.dremio.hive.proto.HiveReaderProto.SerializedInputSplit;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionValue;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.proto.EntityId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import io.protostuff.ByteString;

class DatasetBuilder implements SourceTableDefinition {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DatasetBuilder.class);

  private static final String EMPTY_STRING = "";

  public static final double RECORD_SIZE = 1024;

  private final HiveClient client;
  private final NamespaceKey datasetPath;
  private final String user;
  private final HiveConf hiveConf;

  private Properties tableProperties;
  private final String dbName;
  private final String tableName;
  private Table table;
  private DatasetConfig datasetConfig;
  private boolean built = false;
  private List<DatasetSplit> splits = new ArrayList<>();
  private final boolean ignoreAuthzErrors;


  private DatasetBuilder(HiveClient client, String user, NamespaceKey datasetPath, boolean ignoreAuthzErrors, HiveConf hiveConf, String dbName, String tableName, Table table, DatasetConfig oldConfig){
    if(oldConfig == null){
      datasetConfig = new DatasetConfig()
          .setPhysicalDataset(new PhysicalDataset())
          .setId(new EntityId().setId(UUID.randomUUID().toString()));
    } else {
      datasetConfig = oldConfig;
      // We're rewriting the read definition. Delete the old one.
      oldConfig.setReadDefinition(null);
    }
    this.client = client;
    this.user = user;
    this.datasetPath = datasetPath;
    this.hiveConf = hiveConf;
    this.table = table;
    this.dbName = dbName;
    this.tableName = tableName;
    this.ignoreAuthzErrors = ignoreAuthzErrors;
  }

  /**
   * @return null if datasetPath is not canonical and couldn't find a corresponding table in the source
   */
  static DatasetBuilder getDatasetBuilder(
      HiveClient client,
      String user,
      NamespaceKey datasetPath,
      boolean isCanonicalDatasetPath,
      boolean ignoreAuthzErrors,
      HiveConf hiveConf,
      DatasetConfig oldConfig) throws TException {
    final List<String> noSourceSchemaPath =
      datasetPath.getPathComponents().subList(1, datasetPath.getPathComponents().size());

    // extract database and table names from dataset path
    final String dbName;
    final String tableName;
    switch (noSourceSchemaPath.size()) {
    case 1:
      dbName = "default";
      tableName = noSourceSchemaPath.get(0);
      break;
    case 2:
      dbName = noSourceSchemaPath.get(0);
      tableName = noSourceSchemaPath.get(1);
      break;
    default:
      //invalid.
      return null;
    }

    // if the dataset path is not canonized we need to get it from the source
    final Table table;
    final String canonicalTableName;
    final String canonicalDbName;
    if (isCanonicalDatasetPath) {
      canonicalDbName = dbName;
      canonicalTableName = tableName;
      table = null;
    } else {
      // passed datasetPath is not canonical, we need to get it from the source
      table = client.getTable(dbName, tableName, ignoreAuthzErrors);
      if(table == null){
        return null;
      }
      canonicalTableName = table.getTableName();
      canonicalDbName = table.getDbName();
    }

    final List<String> canonicalDatasetPath = Lists.newArrayList(datasetPath.getRoot(), canonicalDbName, canonicalTableName);
    return new DatasetBuilder(client, user, new NamespaceKey(canonicalDatasetPath), ignoreAuthzErrors, hiveConf, canonicalDbName, canonicalTableName, table, oldConfig);
  }

  @Override
  public NamespaceKey getName() {
    return datasetPath;
  }

  @Override
  public DatasetConfig getDataset() throws Exception {
    buildIfNecessary();
    return datasetConfig;
  }

  @Override
  public List<DatasetSplit> getSplits() throws Exception {
    buildIfNecessary();
    return ImmutableList.copyOf(splits);
  }

  private void buildIfNecessary() throws Exception {
    if(built){
      return;
    }
    if(table == null){
      table = client.getTable(dbName, tableName, ignoreAuthzErrors);
      if(table == null){
        throw UserException.dataReadError().message("Initially found table %s.%s but then failed to retrieve from Hive metastore.", dbName, tableName).build(logger);
      }
    }

    // store table properties.
    tableProperties = MetaStoreUtils.getSchema(table.getSd(), table.getSd(), table.getParameters(), table.getDbName(), table.getTableName(), table.getPartitionKeys());

    List<Field> fields = new ArrayList<>();
    List<FieldSchema> hiveFields = table.getSd().getCols();
    for(FieldSchema hiveField : hiveFields) {
      Field f = HiveSchemaConverter.getArrowFieldFromHivePrimitiveType(hiveField.getName(), TypeInfoUtils.getTypeInfoFromTypeString(hiveField.getType()));
      if (f != null) {
        fields.add(f);
      }
    }

    final List<String> partitionColumns = new ArrayList<>();
    for (FieldSchema field : table.getPartitionKeys()) {
      Field f = HiveSchemaConverter.getArrowFieldFromHivePrimitiveType(field.getName(),
              TypeInfoUtils.getTypeInfoFromTypeString(field.getType()));
      if (f != null) {
        fields.add(f);
        partitionColumns.add(field.getName());
      }
    }

    final BatchSchema batchSchema = BatchSchema.newBuilder().addFields(fields).build();


    HiveTableXattr.Builder tableExtended = HiveTableXattr.newBuilder().addAllTableProperty(fromProperties(tableProperties));

    if(table.getSd().getInputFormat() != null){
      tableExtended.setInputFormat(table.getSd().getInputFormat());
    }

    String storageHandler = table.getParameters().get(META_TABLE_STORAGE);
    if(storageHandler != null){
      tableExtended.setStorageHandler(storageHandler);
    }

    tableExtended.setSerializationLib(table.getSd().getSerdeInfo().getSerializationLib());
    tableExtended.setTableHash(getHash(table));

    datasetConfig
      .setFullPathList(datasetPath.getPathComponents())
      .setType(DatasetType.PHYSICAL_DATASET)
      .setName(tableName)
      .setOwner(user)
      .setPhysicalDataset(new PhysicalDataset())
      .setRecordSchema(batchSchema.toByteString())
      .setReadDefinition(new ReadDefinition()
        .setPartitionColumnsList(partitionColumns)
        .setSortColumnsList(FluentIterable.from(table.getSd().getSortCols())
          .transform(new Function<Order, String>() {
            @Override
            public String apply(Order order) {
              return order.getCol();
            }
          }).toList())
        .setLastRefreshDate(System.currentTimeMillis())
        .setExtendedProperty(ByteString.copyFrom(tableExtended.build().toByteArray()))
        .setReadSignature(null)
      );

    buildSplits(tableExtended, dbName, tableName);
    // reset the extended properties since buildSplits() may change them.
    datasetConfig.getReadDefinition().setExtendedProperty(ByteString.copyFrom(tableExtended.build().toByteArray()));

    built = true;

  }

  /**
   * Get the stats from table properties. If not found -1 is returned for each stats field.
   * CAUTION: stats may not be up-to-date with the underlying data. It is always good to run the ANALYZE command on
   * Hive table to have up-to-date stats.
   * @param properties
   * @return
   */
  private HiveStats getStatsFromProps(final Properties properties) {
    long numRows = -1;
    long sizeInBytes = -1;
    try {
      final String numRowsProp = properties.getProperty(StatsSetupConst.ROW_COUNT);
      if (numRowsProp != null) {
          numRows = Long.valueOf(numRowsProp);
      }

      final String sizeInBytesProp = properties.getProperty(StatsSetupConst.TOTAL_SIZE);
      if (sizeInBytesProp != null) {
        sizeInBytes = Long.valueOf(sizeInBytesProp);
      }
    } catch (final NumberFormatException e) {
      logger.error("Failed to parse Hive stats in metastore.", e);
      // continue with the defaults.
    }

    return new HiveStats(numRows, sizeInBytes);
  }

  private SerializedInputSplit serialize(InputSplit split) throws IOException{
    ByteArrayDataOutput output = ByteStreams.newDataOutput();
    split.write(output);
    return SerializedInputSplit.newBuilder()
      .setInputSplitClass(split.getClass().getName())
      .setInputSplit(com.google.protobuf.ByteString.copyFrom(output.toByteArray())).build();
  }

  private boolean allowParquetNative(boolean currentStatus, Class<? extends InputFormat<?, ?>> clazz){
    return currentStatus && MapredParquetInputFormat.class.equals(clazz);
  }

  private boolean isRecursive(Properties properties){
    return "true".equalsIgnoreCase(properties.getProperty("mapred.input.dir.recursive", "false")) &&
        "true".equalsIgnoreCase(properties.getProperty("hive.mapred.supports.subdirectories", "false"));
  }


  private class HiveSplitWork {
    List<DatasetSplit> splits;
    HiveStats hiveStats;

    public HiveSplitWork(List<DatasetSplit> splits, HiveStats hiveStats) {
      this.splits = splits;
      this.hiveStats = hiveStats;
    }

    public List<DatasetSplit> getSplits() {
      return splits;
    }

    public HiveStats getHiveStats() {
      return hiveStats;
    }
  }

  private class HiveSplitsGenerator extends TimedRunnable<HiveSplitWork> {
    private final JobConf job;
    private final InputFormat<?, ?> format;
    private final HiveStats totalStats;
    private final Partition partition;
    private final int partitionId;

    public HiveSplitsGenerator(JobConf job, InputFormat<?, ?> format, HiveStats totalStats, Partition partition, int partitionId) {
      this.job = job;
      this.format = format;
      this.totalStats = totalStats;
      this.partition = partition;
      this.partitionId = partitionId;
    }

    @Override
    protected HiveSplitWork runInner() throws Exception {
      List<DatasetSplit> splits = Lists.newArrayList();
      double totalEstimatedRecords = 0;

      InputSplit[] inputSplits = format.getSplits(job, 1);
      double totalSize = 0;
      for (final InputSplit inputSplit : inputSplits) {
        totalSize += inputSplit.getLength();
      }
      int id = 0;
      for (final InputSplit inputSplit : inputSplits) {

        HiveSplitXattr.Builder splitAttr = HiveSplitXattr.newBuilder();

        splitAttr.setPartitionId(partitionId);
        splitAttr.setInputSplit(serialize(inputSplit));

        DatasetSplit split = new DatasetSplit();

        String splitKey = partition == null ? table.getSd().getInputFormat() + id : partition.getSd().getLocation() + id;
        split.setSplitKey(splitKey);
        final long length = inputSplit.getLength();
        split.setSize(length);
        split.setPartitionValuesList(getPartitions(table, partition));
        split.setAffinitiesList(FluentIterable.of(inputSplit.getLocations()).transform(new Function<String, Affinity>(){
          @Override
          public Affinity apply(String input) {
            return new Affinity().setHost(input).setFactor((double) length);
          }}).toList());

        // if the estimated rows is known, multiply it times the portion of the total data in this split to get the split estimated rows.
        double splitEstimatedRecords = Math.ceil(totalStats.isValid() ? (inputSplit.getLength()/totalSize * totalStats.getNumRows()) : inputSplit.getLength() / RECORD_SIZE);
        totalEstimatedRecords += splitEstimatedRecords;
        split.setRowCount((long) splitEstimatedRecords);

        split.setExtendedProperty(ByteString.copyFrom(splitAttr.build().toByteArray()));
        splits.add(split);
        id++;
      }
      return new HiveSplitWork(splits, new HiveStats((long) totalEstimatedRecords, (long) totalSize));
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      if (partition != null) {
        return new IOException("Failure while trying to get splits for partition " + partition.getSd().getLocation(), e);
      } else {
        return new IOException("Failure while trying to get splits for table " + tableName, e);
      }
    }
  }

  private void buildSplits(HiveTableXattr.Builder tableExtended, String dbName, String tableName) throws Exception {
    ReadDefinition metadata = datasetConfig.getReadDefinition();
    final HiveStats metastoreStats = getStatsFromProps(tableProperties);
    setFormat(table, tableExtended);

    boolean allowParquetNative = true;
    HiveStats observedStats = new HiveStats(0,0);

    Stopwatch spiltStart = Stopwatch.createStarted();
    if (metadata.getPartitionColumnsList().isEmpty()) {
      final JobConf job = new JobConf(hiveConf);
      addConfToJob(job, tableProperties);
      Class<? extends InputFormat<?, ?>> inputFormat = getInputFormatClass(job, table, null);
      allowParquetNative = allowParquetNative(allowParquetNative, inputFormat);
      job.setInputFormat(inputFormat);
      final InputFormat<?, ?> format = job.getInputFormat();

      if(addInputPath(table.getSd(), job)){
        // only generate splits if there is an input path.
        HiveSplitWork hiveSplitWork = new HiveSplitsGenerator(job, format, metastoreStats, null, 0).runInner();
        splits.addAll(hiveSplitWork.getSplits());
        observedStats.add(hiveSplitWork.getHiveStats());
      }

      // add a single partition from table properties.
      tableExtended.addAllPartitionProperties(Collections.singletonList(getPartitionProperty(tableExtended, fromProperties(tableProperties))));

      if (format instanceof FileInputFormat) {
        final FileSystemPartitionUpdateKey updateKey = getFSBasedUpdateKey(table.getSd().getLocation(), job, isRecursive(tableProperties), 0);
        if (updateKey != null) {
          metadata.setReadSignature(ByteString.copyFrom(
            HiveReadSignature.newBuilder()
              .setType(HiveReadSignatureType.FILESYSTEM)
              .addAllFsPartitionUpdateKeys(Collections.singletonList(updateKey))
              .build()
              .toByteArray()));
        }
      }
    } else {
      final List<FileSystemPartitionUpdateKey> updateKeys = Lists.newArrayList();
      boolean allFSBasedPartitions = true;
      final List<TimedRunnable<HiveSplitWork>> splitsGenerators = Lists.newArrayList();
      final List<PartitionProp> partitionProps = Lists.newArrayList();
      int partitionId = 0;
      List<Integer> partitionHashes = Lists.newArrayList();

      for(Partition partition : client.getPartitions(dbName, tableName)) {
        partitionHashes.add(getHash(partition));
        final Properties partitionProperties = getPartitionMetadata(partition, table);
        final JobConf job = new JobConf(hiveConf);

        addConfToJob(job, tableProperties);
        addConfToJob(job, partitionProperties);

        Class<? extends InputFormat<?, ?>> inputFormat = getInputFormatClass(job, table, partition);
        allowParquetNative = allowParquetNative(allowParquetNative, inputFormat);
        job.setInputFormat(inputFormat);

        partitionProps.add(getPartitionProperty(partition, fromProperties(partitionProperties)));

        final InputFormat<?, ?> format = job.getInputFormat();
        final HiveStats totalPartitionStats = getStatsFromProps(partitionProperties);
        if (addInputPath(partition.getSd(), job)) {
          splitsGenerators.add(new HiveSplitsGenerator(job, format, totalPartitionStats, partition, partitionId));
        }
        if (format instanceof FileInputFormat) {
          final FileSystemPartitionUpdateKey updateKey = getFSBasedUpdateKey(partition.getSd().getLocation(), job, isRecursive(partitionProperties), partitionId);
          if (updateKey != null) {
            updateKeys.add(updateKey);
          }
        } else {
          allFSBasedPartitions = false;
        }
        ++partitionId;
      }

      Collections.sort(partitionHashes);
      tableExtended.setPartitionHash(Objects.hashCode(partitionHashes));
      // set partition properties in table's xattr
      tableExtended.addAllPartitionProperties(partitionProps);

      if (!splitsGenerators.isEmpty()) {
        final List<HiveSplitWork> hiveSplitWorks = TimedRunnable.run("Get splits for hive table " + tableName, logger, splitsGenerators, 16);
        for (HiveSplitWork splitWork : hiveSplitWorks) {
          splits.addAll(splitWork.getSplits());
          observedStats.add(splitWork.getHiveStats());
        }
      }

      // If all partitions had filesystem based partitions then set updatekey
      if (allFSBasedPartitions && !updateKeys.isEmpty()) {
        metadata.setReadSignature(ByteString.copyFrom(
          HiveReadSignature.newBuilder()
            .setType(HiveReadSignatureType.FILESYSTEM)
            .addAllFsPartitionUpdateKeys(updateKeys)
            .build()
            .toByteArray()));
      }
    }

    if(allowParquetNative){
      tableExtended.setReaderType(ReaderType.NATIVE_PARQUET);
    } else {
      tableExtended.setReaderType(ReaderType.BASIC);
    }

    HiveStats actualStats = metastoreStats.isValid() ? metastoreStats : observedStats;
    metadata.setScanStats(new ScanStats()
        .setRecordCount(actualStats.getNumRows())
        .setDiskCost((float) actualStats.getSizeInBytes())
        .setCpuCost((float) actualStats.getSizeInBytes())
        .setType(ScanStatsType.NO_EXACT_ROW_COUNT)
        .setScanFactor(allowParquetNative ? ScanCostFactor.PARQUET.getFactor() : ScanCostFactor.OTHER.getFactor())
        );
    spiltStart.stop();
    logger.debug("Computing splits for table {} took {} ms", datasetPath, spiltStart.elapsed(TimeUnit.MILLISECONDS));
  }

  private PartitionProp getPartitionProperty(Partition partition, List<Prop> props) {
    PartitionProp.Builder partitionPropBuilder = PartitionProp.newBuilder();

    if (partition.getSd().getInputFormat() != null) {
      partitionPropBuilder.setInputFormat(partition.getSd().getInputFormat());
    }
    if (partition.getParameters().get(META_TABLE_STORAGE) != null) {
      partitionPropBuilder.setStorageHandler(partition.getParameters().get(META_TABLE_STORAGE));
    }
    if (partition.getSd().getSerdeInfo().getSerializationLib() != null) {
      partitionPropBuilder.setSerializationLib(partition.getSd().getSerdeInfo().getSerializationLib());
    }
    partitionPropBuilder.addAllPartitionProperty(props);
    return partitionPropBuilder.build();
  }

  private PartitionProp getPartitionProperty(HiveTableXattr.Builder tableExtended, List<Prop> props) {
    // set a single partition for a table
    PartitionProp.Builder partitionPropBuilder = PartitionProp.newBuilder();
    if (tableExtended.getInputFormat() != null) {
      partitionPropBuilder.setInputFormat(tableExtended.getInputFormat());
    }
    if (tableExtended.getStorageHandler() != null) {
      partitionPropBuilder.setStorageHandler(tableExtended.getStorageHandler());
    }
    if (tableExtended.getSerializationLib() != null) {
      partitionPropBuilder.setSerializationLib(tableExtended.getSerializationLib());
    }
    partitionPropBuilder.addAllPartitionProperty(props);
    return partitionPropBuilder.build();
  }

  private FileSystemPartitionUpdateKey getFSBasedUpdateKey(String partitionDir, JobConf job, boolean isRecursive, int partitionId) throws IOException {
    final List<FileSystemCachedEntity> cachedEntities = Lists.newArrayList();
    final Path rootLocation = new Path(partitionDir);
    final FileSystemWrapper fs = FileSystemWrapper.get(rootLocation, job);

    if (fs.exists(rootLocation)) {
      final FileStatus rootStatus = fs.getFileStatus(rootLocation);
      if (rootStatus.isDirectory()) {
        cachedEntities.add(FileSystemCachedEntity.newBuilder()
          .setPath(EMPTY_STRING)
          .setLastModificationTime(rootStatus.getModificationTime())
          .setIsDir(true)
          .build());

        for (FileStatus fileStatus : fs.list(isRecursive, rootLocation)) {
          final Path filePath = fileStatus.getPath();
          if (fileStatus.isDirectory()) {
            cachedEntities.add(FileSystemCachedEntity.newBuilder()
              .setPath(PathUtils.relativePath(filePath, rootLocation))
              .setLastModificationTime(fileStatus.getModificationTime())
              .setIsDir(true)
              .build());
          } else if (fileStatus.isFile()) {
            cachedEntities.add(FileSystemCachedEntity.newBuilder()
              .setPath(PathUtils.relativePath(filePath, rootLocation))
              .setLastModificationTime(fileStatus.getModificationTime())
              .setIsDir(false)
              .build());
          }
        }
      } else {
        cachedEntities.add(FileSystemCachedEntity.newBuilder()
          .setPath(EMPTY_STRING)
          .setLastModificationTime(rootStatus.getModificationTime())
          .setIsDir(false)
          .build());
      }
      return FileSystemPartitionUpdateKey.newBuilder()
        .setPartitionId(partitionId)
        .setPartitionRootDir(fs.makeQualified(rootLocation).toString())
        .addAllCachedEntities(cachedEntities)
        .build();
    }
    return null;
  }

  private static boolean addInputPath(StorageDescriptor sd, JobConf job) throws IOException {
    final Path path = new Path(sd.getLocation());
    final FileSystem fs = FileSystemWrapper.get(path, job);

    if (fs.exists(path)) {
      FileInputFormat.addInputPath(job, path);
      return true;
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  public List<Prop> fromProperties(Properties props){
    List<Prop> output = new ArrayList<>();
    for(Entry<Object, Object> eo : props.entrySet()){
      Entry<String, String> e = (Entry<String, String>) (Object) eo;
      output.add(Prop.newBuilder().setKey(e.getKey()).setValue(e.getValue()).build());
    }
    return output;
  }

  public static void setFormat(final Table table, HiveTableXattr.Builder xattr) throws ExecutionSetupException{
    if(table.getSd().getInputFormat() != null){
      xattr.setInputFormat(table.getSd().getInputFormat());
      return;
    }

    if(table.getParameters().get(META_TABLE_STORAGE) != null){
      xattr.setStorageHandler(table.getParameters().get(META_TABLE_STORAGE));
      return;
    }
  }

  private static List<PartitionValue> getPartitions(Table table, Partition partition) {
    if(partition == null){
      return Collections.emptyList();
    }

    final List<String> partitionValues = partition.getValues();
    final List<PartitionValue> output = Lists.newArrayList();
    final List<FieldSchema> partitionKeys = table.getPartitionKeys();
    for(int i =0; i < partitionKeys.size(); i++){
      PartitionValue value = getPartitionValue(partitionKeys.get(i), partitionValues.get(i));
      if(value != null){
        output.add(value);
      }
    }
    return output;
  }

  private static PartitionValue getPartitionValue(FieldSchema partitionCol, String value) {
    final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(partitionCol.getType());
    PartitionValue out = new PartitionValue();
    out.setColumn(partitionCol.getName());

    if("__HIVE_DEFAULT_PARTITION__".equals(value)){
      return out;
    }

    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
          case BINARY:
            return out.setBinaryValue(ByteString.copyFrom(value.getBytes()));
          case BOOLEAN:
            return out.setBitValue(Boolean.parseBoolean(value));
          case DOUBLE:
            return out.setDoubleValue(Double.parseDouble(value));
          case FLOAT:
            return out.setFloatValue(Float.parseFloat(value));
          case BYTE:
          case SHORT:
          case INT:
            return out.setIntValue(Integer.parseInt(value));
          case LONG:
            return out.setLongValue(Long.parseLong(value));
          case STRING:
          case VARCHAR:
            return out.setStringValue(value);
          case CHAR:
            return out.setStringValue(value.trim());
          case TIMESTAMP:
            return out.setLongValue(DateTimes.toMillisFromJdbcTimestamp(value));
          case DATE:
            return out.setLongValue(DateTimes.toMillisFromJdbcDate(value));
          case DECIMAL:
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            if(decimalTypeInfo.getPrecision() > 38){
              throw UserException.unsupportedError()
                .message("Dremio only supports decimals up to 38 digits in precision. This Hive table has a partition value with scale of %d digits.", decimalTypeInfo.getPrecision())
                .build(logger);
            }
            HiveDecimal decimal = HiveDecimalUtils.enforcePrecisionScale(HiveDecimal.create(value), decimalTypeInfo.precision(), decimalTypeInfo.scale());
            final BigDecimal original = decimal.bigDecimalValue();
            // we can't just use unscaledValue() since BigDecimal doesn't store trailing zeroes and we need to ensure decoding includes the correct scale.
            final BigInteger unscaled = original.movePointRight(decimalTypeInfo.scale()).unscaledValue();
            return out.setBinaryValue(ByteString.copyFrom(DecimalTools.signExtend16(unscaled.toByteArray())));
          default:
            HiveUtilities.throwUnsupportedHiveDataTypeError(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory().toString());
        }
      default:
        HiveUtilities.throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
    }

    return null; // unreachable
  }

  /**
   * Wrapper around {@link MetaStoreUtils#getPartitionMetadata(Partition, Table)} which also adds parameters from table
   * to properties returned by {@link MetaStoreUtils#getPartitionMetadata(Partition, Table)}.
   *
   * @param partition the source of partition level parameters
   * @param table     the source of table level parameters
   * @return properties
   */
  public static Properties getPartitionMetadata(final Partition partition, final Table table) {
    final Properties properties = MetaStoreUtils.getPartitionMetadata(partition, table);

    // SerDe expects properties from Table, but above call doesn't add Table properties.
    // Include Table properties in final list in order to not to break SerDes that depend on
    // Table properties. For example AvroSerDe gets the schema from properties (passed as second argument)
    for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
      if (entry.getKey() != null && entry.getKey() != null) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }

    return properties;
  }


  /**
   * Utility method which adds give configs to {@link JobConf} object.
   *
   * @param job {@link JobConf} instance.
   * @param properties New config properties
   * @param hiveConf HiveConf of Hive storage plugin
   */
  public static void addConfToJob(final JobConf job, final Properties properties) {
    for (Object obj : properties.keySet()) {
      job.set((String) obj, (String) properties.get(obj));
    }
  }

  public static Class<? extends InputFormat<?, ?>> getInputFormatClass(final JobConf job, final Table table, final Partition partition) throws Exception {
    if(partition != null){
      if(partition.getSd().getInputFormat() != null){
        return (Class<? extends InputFormat<?, ?>>) Class.forName(partition.getSd().getInputFormat());
      }

      if(partition.getParameters().get(META_TABLE_STORAGE) != null){
        final HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(job, partition.getParameters().get(META_TABLE_STORAGE));
        return (Class<? extends InputFormat<?, ?>>) storageHandler.getInputFormatClass();
      }
    }

    if(table.getSd().getInputFormat() != null){
      return (Class<? extends InputFormat<?, ?>>) Class.forName(table.getSd().getInputFormat());
    }

    if(table.getParameters().get(META_TABLE_STORAGE) != null){
      final HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(job, table.getParameters().get(META_TABLE_STORAGE));
      return (Class<? extends InputFormat<?, ?>>) storageHandler.getInputFormatClass();
    }

    throw new ExecutionSetupException("Unable to get Hive table InputFormat class. There is neither " +
        "InputFormat class explicitly specified nor a StorageHandler class provided.");
  }

  @Override
  public DatasetType getType() {
    return DatasetType.PHYSICAL_DATASET;
  }

  @Override
  public boolean isSaveable() {
    return true;
  }

  static int getHash(Table table) {
    return Objects.hashCode(
      table.getTableType(),
      table.getParameters(),
      table.getPartitionKeys(),
      table.getSd(),
      table.getViewExpandedText(),
      table.getViewOriginalText());
  }

  static int getHash(Partition partition) {
    return Objects.hashCode(
      partition.getSd(),
      partition.getParameters(),
      partition.getValues());
  }
}
