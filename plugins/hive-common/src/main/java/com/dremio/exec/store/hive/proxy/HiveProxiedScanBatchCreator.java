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
package com.dremio.exec.store.hive.proxy;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.op.spi.ProducerOperator;
import java.util.List;
import org.pf4j.ExtensionPoint;

/** Interface for creating ProducerOperators from a proxied Hive SubScan. */
@SuppressWarnings("unused")
public interface HiveProxiedScanBatchCreator extends ExtensionPoint {

  ProducerOperator create() throws ExecutionSetupException;

  /*
   * Create a record reader iterator
   */
  RecordReaderIterator createRecordReaderIterator();

  /*
   * Get the created record reader iterator
   */
  RecordReaderIterator getRecordReaderIterator();

  /*
   * Produce records from buffered splits if present
   */
  void produceFromBufferedSplits(boolean toProduce);

  /*
   * Add splits to the underlying recordreaderiterator
   */
  void addSplits(List<SplitAndPartitionInfo> splits);
}
