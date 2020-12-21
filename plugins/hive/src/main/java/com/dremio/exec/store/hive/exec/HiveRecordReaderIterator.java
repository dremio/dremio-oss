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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.RuntimeFilterEvaluator;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Reader iterator for hive abstract readers
 */
@NotThreadSafe
public class HiveRecordReaderIterator implements RecordReaderIterator {
    private static Logger logger = LoggerFactory.getLogger(HiveRecordReaderIterator.class);

    private final OperatorContext context;
    private final CompositeReaderConfig readerConfig;
    private final List<Pair<SplitAndPartitionInfo, Supplier<RecordReader>>> readers;
    private final List<RuntimeFilterEvaluator> runtimeFilterEvaluators = new ArrayList<>();

    private int nextLocation = 0;

    public HiveRecordReaderIterator(final OperatorContext context, final CompositeReaderConfig readerConfig,
                                    final List<Pair<SplitAndPartitionInfo, Supplier<RecordReader>>> readers) {
        this.context = context;
        this.readerConfig = readerConfig;
        this.readers = readers;
    }

    @Override
    public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
        if (runtimeFilter.getPartitionColumnFilter() != null) {
            final RuntimeFilterEvaluator filterEvaluator =
                    new RuntimeFilterEvaluator(context.getAllocator(), context.getStats(), runtimeFilter);
            this.runtimeFilterEvaluators.add(filterEvaluator);
            logger.debug("Runtime filter added to the iterator [{}]", runtimeFilter);
        }
        setNextLocation(this.nextLocation);
    }

    @Override
    public boolean hasNext() {
        return nextLocation < readers.size();
    }

    @Override
    public RecordReader next() {
        RecordReader nextReader = readers.get(nextLocation).getRight().get();
        setNextLocation(nextLocation + 1);
        return nextReader;
    }

    private void setNextLocation(int baseNext) {
        this.nextLocation = baseNext;
        if (runtimeFilterEvaluators.isEmpty() || !hasNext()) {
            return;
        }
        boolean skipPartition;
        do {
            skipPartition = false;
            final SplitAndPartitionInfo split = readers.get(nextLocation).getLeft();
            if (split == null) {
                return;
            }

            final List<NameValuePair<?>> nameValuePairs = this.readerConfig.getPartitionNVPairs(this.context.getAllocator(), split);
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
        } while (skipPartition && hasNext());
    }

    @Override
    public void close() throws Exception {
    }
}
