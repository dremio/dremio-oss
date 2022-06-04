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
package com.dremio.exec.planner.physical.visitor;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.HashToMergeExchangePrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.UnorderedDeMuxExchangePrel;
import com.dremio.exec.planner.physical.UnorderedMuxExchangePrel;
import com.dremio.options.OptionManager;
import com.dremio.resource.GroupResourceInformation;
import com.google.common.collect.Lists;

public class InsertLocalExchangeVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {
  private final boolean isMuxEnabled;
  private final int muxFragmentsPerNode;
  private final boolean isDeMuxEnabled;
  private final OptionManager options;
  private final GroupResourceInformation resourceInformation;


  private InsertLocalExchangeVisitor(boolean isMuxEnabled, int muxFragmentsPerNode, boolean isDeMuxEnabled, GroupResourceInformation resourceInformation, OptionManager options) {
    this.isMuxEnabled = isMuxEnabled;
    this.muxFragmentsPerNode = muxFragmentsPerNode;
    this.isDeMuxEnabled = isDeMuxEnabled;
    this.resourceInformation = resourceInformation;
    this.options = options;
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, Void value) throws RuntimeException {
    Prel child = ((Prel)prel.getInput()).accept(this, null);
    // Whenever we encounter a HashToRandomExchangePrel
    //   If MuxExchange is enabled, insert a UnorderedMuxExchangePrel before HashToRandomExchangePrel.
    //   If DeMuxExchange is enabled, insert a UnorderedDeMuxExchangePrel after HashToRandomExchangePrel.
    if (!(prel instanceof HashToRandomExchangePrel)) {
      return (Prel)prel.copy(prel.getTraitSet(), Collections.singletonList(child));
    }

    Prel newPrel = child;

    if (isMuxEnabled && !(child instanceof TableFunctionPrel)) {
      long parallelism = computeParallelism(prel, options, resourceInformation);
      long parallelismPerNode = parallelism / resourceInformation.getExecutorNodeCount();
      //if parallelism for this exchange is already less than or equal to muxFragmentsPerNode, no point
      // adding the mux exchange here
      if (parallelismPerNode <= muxFragmentsPerNode) {
        return (Prel)prel.copy(prel.getTraitSet(), Collections.singletonList(child));
      }
      newPrel = new UnorderedMuxExchangePrel(child.getCluster(), child.getTraitSet(), child, muxFragmentsPerNode);
    }

    newPrel = new HashToRandomExchangePrel(prel.getCluster(),
        prel.getTraitSet(), newPrel, ((HashToRandomExchangePrel) prel).getFields());

    if (isDeMuxEnabled) {
      HashToRandomExchangePrel hashExchangePrel = (HashToRandomExchangePrel) newPrel;
      // Insert a DeMuxExchange to narrow down the number of receivers
      newPrel = new UnorderedDeMuxExchangePrel(prel.getCluster(), prel.getTraitSet(), hashExchangePrel,
          hashExchangePrel.getFields());
    }

    return newPrel;
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  public static Prel insertLocalExchanges(Prel prel, OptionManager options, GroupResourceInformation resourceInformation) {
    final boolean isMuxEnabled = options.getOption(PlannerSettings.MUX_EXCHANGE);
    final boolean isDeMuxEnabled = options.getOption(PlannerSettings.DEMUX_EXCHANGE);

    if (resourceInformation == null) {
      // this may happen in some tests with mocked QueryContext
      // shouldn't happen in real Dremio instances
      return prel;
    }

    if (!isMuxEnabled && !isDeMuxEnabled) {
      return prel;
    }
    final double totalClusterCoresThreshold = options.getOption(PlannerSettings.MUX_USE_THRESHOLD);
    boolean useMuxExchange = isMuxEnabled;
    long muxFragmentsPerNode = 1;

    // if total cluster cores above threshold, then we will use mux exchange with 1 fragment per node
    // this is the legacy behavior
    if (isMuxEnabled && computeTotalParallelism(resourceInformation, options) < totalClusterCoresThreshold) {
      // if a specific number of fragments per node has been set, so use that, but don't exceed cores/node
      if (options.getOption(PlannerSettings.MUX_FRAGS) != 0) {
        muxFragmentsPerNode = Math.min(options.getOption(PlannerSettings.MUX_FRAGS),
          resourceInformation.getAverageExecutorCores(options));
      } else {
        // compute whether to use mux exchange and how many fragments per node
        // by computing the total number of Arrow Buffers we expect the hash exchanges to need per node
        // this is affected by the number of cores in the cluster and the count and type of the columns in the data
        ExchangeBufferCounter analyzer = new ExchangeBufferCounter(options, resourceInformation);

        prel.accept(analyzer);

        long totalBufferCount = analyzer.getTotalBufferCount();
        long bufferCountThreshold = options.getOption(PlannerSettings.MUX_BUFFER_THRESHOLD);

        // if total count is less than threshold, no need to use mux exchange for this query
        if (totalBufferCount < bufferCountThreshold) {
          useMuxExchange = false;
        } else {
          // compute number of frags per node needed to get total below threshold
          muxFragmentsPerNode = bufferCountThreshold * resourceInformation.getAverageExecutorCores(options) / totalBufferCount;
          // can't exceed number of cores per node
          muxFragmentsPerNode = Math.min(muxFragmentsPerNode, resourceInformation.getAverageExecutorCores(options));
          // can't be less than 1
          muxFragmentsPerNode = Math.max(muxFragmentsPerNode, 1);
        }
      }
    }

    if (useMuxExchange || isDeMuxEnabled) {
      return prel.accept(
        new InsertLocalExchangeVisitor(useMuxExchange, (int) muxFragmentsPerNode, isDeMuxEnabled, resourceInformation, options), null);
    }
    return prel;
  }

  private static class ExchangeBufferCounter extends RoutingShuttle {
    private final OptionManager options;
    private final GroupResourceInformation resourceInformation;

    private long totalBufferCount;

    private ExchangeBufferCounter(final OptionManager options, GroupResourceInformation cri) {
      this.options = options;
      this.resourceInformation = cri;
    }

    @Override
    public RelNode visit(RelNode rel) {
      super.visit(rel);
      if (rel instanceof HashToRandomExchangePrel || rel instanceof HashToMergeExchangePrel) {
        long buffers = computeBufferCountForExchange(rel, options, resourceInformation);
        totalBufferCount += buffers;
      }
      return rel;
    }

    public long getTotalBufferCount() {
      return totalBufferCount;
    }
  }

  private static long computeTotalParallelism(GroupResourceInformation resourceInformation, OptionManager options) {
    return resourceInformation.getAverageExecutorCores(options) * resourceInformation.getExecutorNodeCount();
  }

  private static long computeBufferCountForExchange(RelNode prel, OptionManager options, GroupResourceInformation cri) {
    long parallelism = computeParallelism(prel, options, cri);
    long nodeCount = cri.getExecutorNodeCount();
    if (nodeCount == 0) {
      // In case all the engines are stopped, we don't have info about executors count.
      // Let's not use mux exchange.
      return 0;
    }
    int buffersPerBatch = prel.getRowType()
      .getFieldList().stream().mapToInt(f -> bufferCountForType(f.getType())).sum();
    return buffersPerBatch * parallelism * parallelism / nodeCount;
  }

  private static long computeParallelism(RelNode prel, OptionManager options, GroupResourceInformation resourceInformation) {
    double rc = prel.getCluster().getMetadataQuery().getRowCount(prel);
    long maxWidth = computeTotalParallelism(resourceInformation, options);
    return Math.min((int) (rc / PrelUtil.getPlannerSettings(prel.getCluster()).getSliceTarget()), maxWidth);
  }

  private static boolean isVariableLength(RelDataType type) {
    return SqlTypeFamily.CHARACTER.contains(type) || SqlTypeFamily.BINARY.contains(type);
  }

  private static boolean isStruct(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.ROW || type.getSqlTypeName() == SqlTypeName.STRUCTURED;
  }

  private static boolean isArray(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.ARRAY;
  }

  private static int bufferCountForType(RelDataType type) {
    if (isStruct(type)) {
      // 1 buffer for nullability + buffers for children
      return 1 + type.getFieldList().stream().mapToInt(f -> bufferCountForType(f.getType())).sum();
    }
    if (isArray(type)) {
      // 1 buffer for nullability + 1 buffer offset + buffers for component type
      return 2 + bufferCountForType(type.getComponentType());
    }
    if (!isVariableLength(type)) {
      // 1 buffer for nullability + 1 buffer for data
      return 2;
    }
    // must be variable length
    // 1 buffer for nullability + 1 buffer for offset + 1 buffer data
    return 3;
  }
}
