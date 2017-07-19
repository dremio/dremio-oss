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
package com.dremio.sabot.op.sender.partition;

import java.io.IOException;
import java.util.List;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.rpc.TunnelProvider;

public interface Partitioner extends AutoCloseable {

  void setup(VectorAccessible incoming,
      HashPartitionSender popConfig,
      OperatorStats stats,
      OperatorContext context,
      TunnelProvider tunnelProvider,
      int numPartitions,
      int start, int end);

  void partitionBatch(VectorAccessible incoming) throws IOException;
  void flushOutgoingBatches() throws IOException;
  void sendTermination() throws IOException;
  List<? extends PartitionOutgoingBatch> getOutgoingBatches();

  /**
   * Method to get PartitionOutgoingBatch based on the fact that there can be > 1 Partitioner
   * @param index
   * @return PartitionOutgoingBatch that matches index within Partitioner. This method can
   * return null if index does not fall within boundary of this Partitioner
   */
  PartitionOutgoingBatch getOutgoingBatch(int index);
  OperatorStats getStats();

  TemplateClassDefinition<Partitioner> TEMPLATE_DEFINITION = new TemplateClassDefinition<>(Partitioner.class, PartitionerTemplate.class);
}