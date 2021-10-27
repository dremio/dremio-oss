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

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.util.Preconditions;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.Closeable;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.RuntimeFilterEvaluator;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.TypeCoercion;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.hive.BaseHiveStoragePlugin;
import com.dremio.exec.store.hive.HivePf4jPlugin;
import com.dremio.exec.store.hive.metadata.ManagedHiveSchema;
import com.dremio.exec.store.parquet.ParquetCoercionReader;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.store.parquet.UnifiedParquetReader;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Throwables;

/**
 * Iterator of RecordReaders to read given hive parquet splits.
 * Need to set next {@link FileSplitParquetRecordReader} in a fileSplitParquetRecordReader; instead of creating
 * (and holding) all fileSplitParquetRecordReaders and setting next in each, this holds just the
 * current and next fileSplitParquetRecordReaders
 */
public class HiveParquetSplitReaderIterator implements RecordReaderIterator {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveParquetSplitReaderIterator.class);

    int location;
    int nextLocation;
    FileSplitParquetRecordReader next;

    private final JobConf jobConf;
    private final boolean vectorize;
    private final OperatorContext context;
    private final ManagedHiveSchema hiveSchema;
    private final boolean enableDetailedTracing;
    private final UserGroupInformation readerUGI;
    private UserGroupInformation currentUGI;
    private final CompositeReaderConfig compositeReader;
    private final BaseHiveStoragePlugin hiveStoragePlugin;
    private final List<ParquetFilterCondition> conditions;
    private final List<HiveParquetSplit> hiveParquetSplits;
    private final HiveReaderProto.HiveTableXattr tableXattr;
    private final HiveSplitsPathRowGroupsMap pathRowGroupsMap;
    private final List<RuntimeFilterEvaluator> runtimeFilterEvaluators;
    private final List<RuntimeFilter> runtimeFilters;
    private final BatchSchema fullSchema;
    private final Collection<List<String>> referencedTables;
    private boolean produceFromBufferedSplits;

    HiveParquetSplitReaderIterator(
            final JobConf jobConf,
            final OperatorContext context,
            final List<HiveParquetSplit> sortedSplits,
            final UserGroupInformation readerUGI,
            final CompositeReaderConfig compositeReader,
            final BaseHiveStoragePlugin hiveStoragePlugin,
            final HiveReaderProto.HiveTableXattr tableXattr,
            final ScanFilter scanFilter,
            final BatchSchema fullSchema,
            final Collection<List<String>> referencedTables,
            final boolean produceBuffered) {
        this.location = -1;
        this.nextLocation = 0;
        this.next = null;

        this.jobConf = jobConf;
        this.context = context;
        this.hiveSchema = new ManagedHiveSchema(jobConf, tableXattr);
        this.readerUGI = readerUGI;
        this.compositeReader = compositeReader;
        this.hiveStoragePlugin = hiveStoragePlugin;
        this.conditions = scanFilter == null ? null : ((ParquetScanFilter) scanFilter).getConditions();
        this.tableXattr = tableXattr;
        this.hiveParquetSplits = sortedSplits;
        this.fullSchema = fullSchema;
        this.referencedTables = referencedTables;

        try {
            currentUGI = UserGroupInformation.getCurrentUser();
        } catch (IOException e) {
            Throwables.propagate(e);
        }

        vectorize = context.getOptions().getOption(ExecConstants.PARQUET_READER_VECTORIZE);
        enableDetailedTracing = context.getOptions().getOption(ExecConstants.ENABLED_PARQUET_TRACING);

        pathRowGroupsMap = new HiveSplitsPathRowGroupsMap(sortedSplits);
        this.runtimeFilterEvaluators = new ArrayList<>();
        this.runtimeFilters = new ArrayList<>();
        this.produceFromBufferedSplits = produceBuffered;
    }

    @Override
    public boolean hasNext() {
        return nextLocation + (produceFromBufferedSplits ? 0 : 1) < hiveParquetSplits.size();
    }

    @Override
    public void produceFromBuffered(boolean toProduce) {
        this.produceFromBufferedSplits = toProduce;
    }

    @Override
    public RecordReader next() {
        Preconditions.checkState(hasNext());

        if (location == -1) {
            next = currentUGI.doAs((PrivilegedAction<FileSplitParquetRecordReader>) () ->
                    createFileSplitReaderFromSplit(hiveParquetSplits.get(this.nextLocation)));
        }
        location = nextLocation;
        FileSplitParquetRecordReader curr = next;
        setNextLocation(location + 1);

        next = null;
        if (nextLocation < hiveParquetSplits.size()) {
            next = currentUGI.doAs((PrivilegedAction<FileSplitParquetRecordReader>) () ->
                    createFileSplitReaderFromSplit(hiveParquetSplits.get(this.nextLocation)));
        }

        curr.setNextFileSplitReader(next);

        return currentUGI.doAs((PrivilegedAction<ParquetCoercionReader>) () -> {
            final TypeCoercion hiveTypeCoercion = new HiveTypeCoercion(hiveSchema.getTypeInfos(),
                    hiveSchema.isVarcharTruncationEnabled());
            RecordReader wrappedRecordReader = compositeReader.wrapIfNecessary(context.getAllocator(), curr,
                    hiveParquetSplits.get(location).getDatasetSplit());
            return ParquetCoercionReader.newInstance(context, compositeReader.getInnerColumns(),
                    wrappedRecordReader, fullSchema, hiveTypeCoercion, curr.getFilterConditions());
        });
    }

    private void setNextLocation(int baseNext) {
        this.nextLocation = baseNext;
        if (runtimeFilterEvaluators.isEmpty() || nextLocation >= hiveParquetSplits.size()) {
            return;
        }
        boolean skipPartition;
        do {
            skipPartition = false;
            final SplitAndPartitionInfo split = this.hiveParquetSplits.get(this.nextLocation).getDatasetSplit();
            final List<NameValuePair<?>> nameValuePairs = this.compositeReader.getPartitionNVPairs(this.context.getAllocator(), split);
            try {
                for (RuntimeFilterEvaluator runtimeFilterEvaluator : runtimeFilterEvaluators) {
                    if (runtimeFilterEvaluator.canBeSkipped(split, nameValuePairs)) {
                        skipPartition = true;
                        this.nextLocation++;
                        break;
                    }
                }
            } finally {
                AutoCloseables.close(RuntimeException.class, nameValuePairs);
            }
        } while (skipPartition && nextLocation < hiveParquetSplits.size());
    }

    private FileSplitParquetRecordReader createFileSplitReaderFromSplit(final HiveParquetSplit hiveParquetSplit) {
        try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
            final Stream<HiveReaderProto.Prop> partitionProperties;
            // If Partition Properties are stored in DatasetMetadata (Pre 3.2.0)
            if (HiveReaderProtoUtil.isPreDremioVersion3dot2dot0LegacyFormat(tableXattr)) {
                logger.debug("Reading partition properties from DatasetMetadata");
                partitionProperties = HiveReaderProtoUtil.getPartitionProperties(tableXattr, hiveParquetSplit.getPartitionId());
            } else {
                logger.debug("Reading partition properties from PartitionChunk");
                partitionProperties = HiveReaderProtoUtil.getPartitionProperties(tableXattr,
                        HiveReaderProtoUtil.getPartitionXattr(hiveParquetSplit.getDatasetSplit()));
            }

            partitionProperties.forEach(prop -> jobConf.set(prop.getKey(), prop.getValue()));
            final List<ParquetFilterCondition> copyOfFilterConditions =
                    conditions==null ? null:
                            conditions.stream()
                                    .map(c ->
                                            new ParquetFilterCondition(c.getPath(), c.getFilter(), c.getExpr(), c.getSort(), c.getRexFilter()))
                                    .collect(Collectors.toList());

            return new FileSplitParquetRecordReader(
                    hiveStoragePlugin,
                    context,
                    UnifiedParquetReader.getReaderFactory(context.getConfig()),
                    fullSchema,
                    compositeReader.getInnerColumns(),
                    copyOfFilterConditions,
                    hiveParquetSplit.getFileSplit(),
                    hiveParquetSplit.getHiveSplitXAttr(),
                    jobConf,
                    referencedTables,
                    vectorize,
                    fullSchema,
                    enableDetailedTracing,
                    readerUGI,
                    hiveSchema,
                    pathRowGroupsMap,
                    compositeReader
              );
        }
    }

    @Override
    public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
        if (runtimeFilter.getPartitionColumnFilter() != null) {
            final RuntimeFilterEvaluator filterEvaluator =
                    new RuntimeFilterEvaluator(context.getAllocator(), context.getStats(), context.getOptions(), runtimeFilter);
            this.runtimeFilterEvaluators.add(filterEvaluator);
            this.runtimeFilters.add(runtimeFilter);
            logger.debug("Runtime filter added to the iterator [{}]", runtimeFilter);
        }
    }

    @Override
    public List<RuntimeFilter> getRuntimeFilters() {
        return runtimeFilters;
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(next);
    }
}
