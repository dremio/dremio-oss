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

package com.dremio.sabot.op.join.vhash;

import static com.dremio.exec.ExecConstants.ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET;
import static com.dremio.exec.ExecConstants.RUNTIME_FILTER_KEY_MAX_SIZE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.commons.collections.CollectionUtils;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;
import com.dremio.exec.proto.CoordExecRPC.FragmentAssignment;
import com.dremio.exec.proto.CoordExecRPC.MajorFragmentAssignment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.ExecProtos.CompositeColumnFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilter;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.RuntimeFilterManager;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.exec.util.ValueListFilterBuilder;
import com.dremio.exec.util.ValueListWithBloomFilter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class RuntimeFilterUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RuntimeFilterUtil.class);

  @VisibleForTesting
  public static boolean isRuntimeFilterEnabledForNonPartitionedCols(OperatorContext operatorContext) {
    return operatorContext.getOptions().getOption(ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET);
  }

  @VisibleForTesting
  public static int getRuntimeValFilterCap(OperatorContext operatorContext) {
    return (int) operatorContext.getOptions().getOption(ExecConstants.RUNTIME_FILTER_VALUE_FILTER_MAX_SIZE);
  }

  @VisibleForTesting
  public static int getRuntimeFilterKeyMaxSize(OperatorContext operatorContext) {
    return (int) operatorContext.getOptions().getOption(RUNTIME_FILTER_KEY_MAX_SIZE);
  }

  public static boolean shouldFragBuildRuntimeFilters(RuntimeFilterInfo runtimeFilterInfo, int minorFragment) {
    /*
     * a. For partitioned columns, a consolidated bloomfilter made on composite build keys.
     * b. For non-partitioned columns, an individual value filter is used per column.
     *
     * Only minor fragments [0,1,2] are allowed to send the filter to the scan operator.
     *
     * If it is a shuffle join, each minor fragment will create a filter from the table keys it
     * has and send it to minor fragments [0,1,2]. At merge points, filter will be consolidated
     * before sending to the probe scan.
     *
     * For broadcast hashjoin cases, no exchanges required. Only minor fragments [0,1,2] will
     * create and send the filter.
     */
    return runtimeFilterInfo != null && (!runtimeFilterInfo.isBroadcastJoin() || minorFragment <= 2);
  }

  public static void prepareAndSendRuntimeFilters(RuntimeFilterManager runtimeFilterManager,
                                                  RuntimeFilterInfo runtimeFilterInfo,
                                                  PartitionColFilters partitionColFilters,
                                                  NonPartitionColFilters nonPartitionColFilters,
                                                  OperatorContext operatorContext,
                                                  HashJoinPOP hashJoinConfig) throws Exception {
    List<RuntimeFilterProbeTarget> probeTargets = runtimeFilterInfo.getRuntimeFilterProbeTargets();

    for (int i = 0; i < probeTargets.size(); i++) {
      RuntimeFilterProbeTarget probeTarget = probeTargets.get(i);
      Optional<BloomFilter> bloomFilter = partitionColFilters.getBloomFilter(i, probeTarget);
      List<ValueListFilter> valueListFilters = nonPartitionColFilters == null ? new ArrayList<>() :
        nonPartitionColFilters.getValueListFilters(i, probeTarget);

      prepareAndSendRuntimeFilter(runtimeFilterManager, probeTarget, bloomFilter,
        valueListFilters, runtimeFilterInfo.isBroadcastJoin(), operatorContext, hashJoinConfig);
    }
  }

  private static void prepareAndSendRuntimeFilter(RuntimeFilterManager runtimeFilterManager,
                                                  RuntimeFilterProbeTarget probeTarget,
                                                  Optional<BloomFilter> partitionColFilter,
                                                  List<ValueListFilter> nonPartitionColFilters,
                                                  boolean isBroadcastJoin,
                                                  OperatorContext operatorContext,
                                                  HashJoinPOP hashJoinConfig) throws Exception {
    final RuntimeFilter.Builder runtimeFilterBuilder = RuntimeFilter.newBuilder()
      .setProbeScanOperatorId(probeTarget.getProbeScanOperatorId())
      .setProbeScanMajorFragmentId(probeTarget.getProbeScanMajorFragmentId());

    /* Add partiton column filter */
    if (!partitionColFilter.isPresent()) {
      // No valid bloomfilter for partition pruning
      logger.debug("No valid partition column filter for {}", probeTarget.toTargetIdString());
    } else {
      Preconditions.checkState(!CollectionUtils.isEmpty(probeTarget.getPartitionBuildTableKeys()));
      Preconditions.checkState(!partitionColFilter.get().isCrossingMaxFPP());

      final CompositeColumnFilter partitionFilter = CompositeColumnFilter.newBuilder()
        .setFilterType(ExecProtos.RuntimeFilterType.BLOOM_FILTER)
        .addAllColumns(probeTarget.getPartitionProbeTableKeys())
        .setValueCount(partitionColFilter.get().getNumBitsSet())
        .setSizeBytes(partitionColFilter.get().getSizeInBytes())
        .build();

      runtimeFilterBuilder.setPartitionColumnFilter(partitionFilter);
    }

    /* Add non-partition column filters */
    for (ValueListFilter valueListFilter : nonPartitionColFilters) {
     ExecProtos.RuntimeFilterType type = ExecProtos.RuntimeFilterType.VALUE_LIST;
      if (valueListFilter instanceof ValueListWithBloomFilter) {
        type = ExecProtos.RuntimeFilterType.VALUE_LIST_WITH_BLOOM_FILTER;
      }
      final CompositeColumnFilter nonPartitionColFilter = CompositeColumnFilter.newBuilder()
        /* Already col's FieldName from PartitionProbeTableKeys() saved in valueListFilter */
        .addColumns(valueListFilter.getFieldName())
        .setFilterType(type)
        .setValueCount(valueListFilter.getValueCount())
        .setSizeBytes(valueListFilter.getSizeInBytes())
        .build();

      runtimeFilterBuilder.addNonPartitionColumnFilter(nonPartitionColFilter);
    }

    final RuntimeFilter runtimeFilter = runtimeFilterBuilder.build();
    if (runtimeFilter.getPartitionColumnFilter().getColumnsCount() == 0 &&
      runtimeFilter.getNonPartitionColumnFilterCount() == 0) {
      // No partition column filter or non-partition column filter
      logger.warn("No valid partition and/or non-partition column filter for {}", probeTarget.toTargetIdString());
      runtimeFilterManager.incrementDropCount();
      return;
    }

    RuntimeFilterManager.RuntimeFilterManagerEntry fmEntry = null;
    if (!isBroadcastJoin && operatorContext.getFragmentHandle().getMinorFragmentId() <= 2) {
      // This fragment is one of the merge points. Set up FilterManager for interim use.
      fmEntry = runtimeFilterManager.coalesce(runtimeFilter, partitionColFilter,
        nonPartitionColFilters, operatorContext.getFragmentHandle().getMinorFragmentId());
    }

    if (isBroadcastJoin) {
      sendRuntimeFilterToProbeScan(runtimeFilter, partitionColFilter, nonPartitionColFilters,
        operatorContext, hashJoinConfig);

      long numberOfValuesInBloomFilter = partitionColFilter.isPresent() ? partitionColFilter.get().getNumBitsSet() : 0;
      int numberOfHashFunctions = partitionColFilter.isPresent() ? partitionColFilter.get().getNumHashFunctions() : 0;
      addRunTimeFilterInfosToProfileDetails(operatorContext,
        prepareRunTimeFilterDetailsInfos(probeTarget, runtimeFilter,
          numberOfValuesInBloomFilter, numberOfHashFunctions));
    } else if (fmEntry != null && fmEntry.isComplete() && !fmEntry.isDropped()) {
      // All other filter pieces have already arrived. This one was last one to join.
      // Send merged filter to probe scan and close this individual piece explicitly.
      sendRuntimeFilterToProbeScan(runtimeFilter, Optional.ofNullable(fmEntry.getPartitionColFilter()),
        fmEntry.getNonPartitionColFilters(), operatorContext, hashJoinConfig);
      runtimeFilterManager.remove(fmEntry);

      long numberOfValuesInBloomFilter = partitionColFilter.isPresent() ? partitionColFilter.get().getNumBitsSet() :0;
      int numberOfHashFunctions = partitionColFilter.isPresent() ? partitionColFilter.get().getNumHashFunctions() : 0;
      addRunTimeFilterInfosToProfileDetails(operatorContext,
        prepareRunTimeFilterDetailsInfos(probeTarget, runtimeFilter,
          numberOfValuesInBloomFilter, numberOfHashFunctions));
    } else {
      // Send filter to merge points (minor fragments <= 2) if not complete.
      sendRuntimeFilterAtMergePoints(runtimeFilter, partitionColFilter, nonPartitionColFilters,
        operatorContext, hashJoinConfig);
    }
  }

  public static List<UserBitShared.RunTimeFilterDetailsInfo> prepareRunTimeFilterDetailsInfos(
    RuntimeFilterProbeTarget probeTarget, RuntimeFilter runtimeFilter, long numberOfValuesInBloomFilter, int numberOfHashFuntions) {
    List<UserBitShared.RunTimeFilterDetailsInfo> runTimeFilterDetailsInfos = new ArrayList<>();
    String probeTargetScanId = String.format("%02d-%02d", probeTarget.getProbeScanMajorFragmentId(), probeTarget.getProbeScanOperatorId() & 0xFF);
    if (!probeTarget.getPartitionProbeTableKeys().isEmpty()) {
      UserBitShared.RunTimeFilterDetailsInfo runTimeFilterDetailsInfo = UserBitShared.RunTimeFilterDetailsInfo.newBuilder()
        .setProbeTarget(probeTargetScanId)
        .addAllProbeFieldNames(probeTarget.getPartitionProbeTableKeys())
        .setIsNonPartitionedColumn(false)
        .setIsPartitionedCoulmn(true)
        .setNumberOfValues(numberOfValuesInBloomFilter)
        .setNumberOfHashFunctions(numberOfHashFuntions)
        .build();
      runTimeFilterDetailsInfos.add(runTimeFilterDetailsInfo);
    }

    for (int i = 0; i < runtimeFilter.getNonPartitionColumnFilterCount(); i++) {
      String probeField = runtimeFilter.getNonPartitionColumnFilter(i).getColumns(0);
      UserBitShared.RunTimeFilterDetailsInfo runTimeFilterDetailsInfoForNonPartitionColumn = UserBitShared.RunTimeFilterDetailsInfo.newBuilder()
        .setProbeTarget(probeTargetScanId)
        .addAllProbeFieldNames(Arrays.asList(probeField))
        .setIsNonPartitionedColumn(true)
        .setIsPartitionedCoulmn(false)
        .setNumberOfValues(runtimeFilter.getNonPartitionColumnFilter(i).getValueCount())
        .setNumberOfHashFunctions(0)
        .build();
      runTimeFilterDetailsInfos.add(runTimeFilterDetailsInfoForNonPartitionColumn);
    }

    return runTimeFilterDetailsInfos;
  }

  @VisibleForTesting
  static void sendRuntimeFilterToProbeScan(RuntimeFilter filter, Optional<BloomFilter> partitionColFilter,
                                           List<ValueListFilter> nonPartitionColFilters,
                                           OperatorContext operatorContext, HashJoinPOP hashJoinConfig) throws Exception {
    logger.debug("Sending join runtime filter to probe scan {}:{}, Filter {}",
      filter.getProbeScanOperatorId(), filter.getProbeScanMajorFragmentId(), partitionColFilter);
    logger.debug("Partition col filter fpp {}",
      partitionColFilter.map(BloomFilter::getExpectedFPP).orElse(-1D));

    final List<ArrowBuf> orderedBuffers = new ArrayList<>(nonPartitionColFilters.size() + 1);
    final ArrowBuf bloomFilterBuf = partitionColFilter.map(bf -> bf.getDataBuffer()).orElse(null);
    partitionColFilter.ifPresent(bf -> orderedBuffers.add(bloomFilterBuf));
    nonPartitionColFilters.forEach(v -> orderedBuffers.add(v.buf()));

    final MajorFragmentAssignment majorFragmentAssignment =
      operatorContext.getExtMajorFragmentAssignments(filter.getProbeScanMajorFragmentId());
    if (majorFragmentAssignment == null) {
      logger.warn("Major fragment assignment for probe scan id {} is null. Dropping the runtime filter.",
        filter.getProbeScanOperatorId());
      return;
    }

    // Sends the filters to node endpoints running minor fragments 0,1,2.
    for (FragmentAssignment assignment : majorFragmentAssignment.getAllAssignmentList()) {
      try {
        logger.info("Sending filter to OpId {}, Frag {}:{}", filter.getProbeScanOperatorId(),
          filter.getProbeScanMajorFragmentId(), assignment.getMinorFragmentIdList());
        final OutOfBandMessage message = new OutOfBandMessage(
          operatorContext.getFragmentHandle().getQueryId(),
          filter.getProbeScanMajorFragmentId(),
          assignment.getMinorFragmentIdList(),
          filter.getProbeScanOperatorId(),
          operatorContext.getFragmentHandle().getMajorFragmentId(),
          operatorContext.getFragmentHandle().getMinorFragmentId(),
          hashJoinConfig.getProps().getOperatorId(),
          new OutOfBandMessage.Payload(filter),
          orderedBuffers.toArray(new ArrowBuf[orderedBuffers.size()]),
          true);
        final NodeEndpoint endpoint = operatorContext.getEndpointsIndex().getNodeEndpoint(assignment.getAssignmentIndex());
        operatorContext.getTunnelProvider().getExecTunnel(endpoint).sendOOBMessage(message);
      } catch (Exception e) {
        logger.warn("Error while sending runtime filter to minor fragments " + assignment.getMinorFragmentIdList(), e);
      }
    }
  }

  @VisibleForTesting
  public static void addRunTimeFilterInfosToProfileDetails(OperatorContext operatorContext,
                                                           List<UserBitShared.RunTimeFilterDetailsInfo> runTimeFilterDetailsInfos) {
    OperatorStats stats = operatorContext.getStats();
    stats.setProfileDetails(UserBitShared.OperatorProfileDetails.newBuilder().addAllRuntimefilterDetailsInfos(runTimeFilterDetailsInfos).build());
  }

  @VisibleForTesting
  static void sendRuntimeFilterAtMergePoints(RuntimeFilter filter, Optional<BloomFilter> bloomFilter,
                                             List<ValueListFilter> nonPartitionColFilters,
                                             OperatorContext operatorContext, HashJoinPOP hashJoinConfig) throws Exception {
    final List<ArrowBuf> orderedBuffers = new ArrayList<>(nonPartitionColFilters.size() + 1);
    final ArrowBuf bloomFilterBuf = bloomFilter.map(bf -> bf.getDataBuffer()).orElse(null);
    bloomFilter.ifPresent(bf -> orderedBuffers.add(bloomFilterBuf));
    nonPartitionColFilters.forEach(vlf -> orderedBuffers.add(vlf.buf()));

    // Sends the filters to node endpoints running minor fragments 0,1,2.
    for (FragmentAssignment a : operatorContext.getAssignments()) {
      try {
        final List<Integer> targetMinorFragments = a.getMinorFragmentIdList().stream()
          .filter(i -> i <= 2)
          .filter(i -> i != operatorContext.getFragmentHandle().getMinorFragmentId()) // skipping myself
          .collect(Collectors.toList());
        if (targetMinorFragments.isEmpty()) {
          continue;
        }

        // Operator ID int is transformed as follows - (fragmentId << 16) + opId;
        logger.debug("Sending filter from {}:{} to {}", operatorContext.getFragmentHandle().getMinorFragmentId(),
          hashJoinConfig.getProps().getOperatorId(),
          targetMinorFragments);
        final OutOfBandMessage message = new OutOfBandMessage(
          operatorContext.getFragmentHandle().getQueryId(),
          operatorContext.getFragmentHandle().getMajorFragmentId(),
          targetMinorFragments,
          hashJoinConfig.getProps().getOperatorId(),
          operatorContext.getFragmentHandle().getMajorFragmentId(),
          operatorContext.getFragmentHandle().getMinorFragmentId(),
          hashJoinConfig.getProps().getOperatorId(),
          new OutOfBandMessage.Payload(filter),
          orderedBuffers.toArray(new ArrowBuf[orderedBuffers.size()]),
          true);
        final NodeEndpoint endpoint = operatorContext.getEndpointsIndex().getNodeEndpoint(a.getAssignmentIndex());
        operatorContext.getTunnelProvider().getExecTunnel(endpoint).sendOOBMessage(message);
      } catch (Exception e) {
        logger.warn("Error while sending runtime filter to minor fragments " + a.getMinorFragmentIdList(), e);
      }
    }
  }

  public static void workOnOOB(OutOfBandMessage message, RuntimeFilterManager filterManager,
                               OperatorContext context, HashJoinPOP config) {
    if (message.getBuffers() == null || message.getBuffers().length != 1) {
      logger.warn("Empty runtime filter received from minor fragment: " + message.getSendingMinorFragmentId());
      return;
    }
    // check above ensures there is only one element in the array. This is a merged message buffer.
    final ArrowBuf msgBuf = message.getBuffers()[0];

    try {
      final RuntimeFilter runtimeFilter = message.getPayload(RuntimeFilter.parser());
      long nextSliceStart = 0L;
      ExecProtos.CompositeColumnFilter partitionColFilterProto = runtimeFilter.getPartitionColumnFilter();

      // Partition col filters
      BloomFilter bloomFilterPiece = null;
      if (partitionColFilterProto != null && !partitionColFilterProto.getColumnsList().isEmpty()) {
        Preconditions.checkArgument(msgBuf.capacity() >= partitionColFilterProto.getSizeBytes(), "Invalid filter size. " +
          "Buffer capacity is %s, expected filter size %s", msgBuf.capacity(), partitionColFilterProto.getSizeBytes());
        bloomFilterPiece = BloomFilter.prepareFrom(msgBuf.slice(nextSliceStart, partitionColFilterProto.getSizeBytes()));
        Preconditions.checkState(bloomFilterPiece.getNumBitsSet() == partitionColFilterProto.getValueCount(),
          "Bloomfilter value count mismatched. Expected %s, Actual %s", partitionColFilterProto.getValueCount(), bloomFilterPiece.getNumBitsSet());
        nextSliceStart += partitionColFilterProto.getSizeBytes();
        logger.debug("Received runtime filter piece {}, attempting merge.", bloomFilterPiece.getName());
      }

      final List<ValueListFilter> valueListFilterPieces = new ArrayList<>(runtimeFilter.getNonPartitionColumnFilterCount());
      for (int i =0; i < runtimeFilter.getNonPartitionColumnFilterCount(); i++) {
        ExecProtos.CompositeColumnFilter nonPartitionColFilterProto = runtimeFilter.getNonPartitionColumnFilter(i);
        final String fieldName = nonPartitionColFilterProto.getColumns(0);
        Preconditions.checkArgument(msgBuf.capacity() >= nextSliceStart + nonPartitionColFilterProto.getSizeBytes(),
          "Invalid filter buffer size for non partition col %s.", fieldName);
        final ValueListFilter valueListFilter = ValueListFilterBuilder
          .fromBuffer(msgBuf.slice(nextSliceStart, nonPartitionColFilterProto.getSizeBytes()));
        nextSliceStart += nonPartitionColFilterProto.getSizeBytes();
        valueListFilter.setFieldName(fieldName);
        Preconditions.checkState(valueListFilter.getValueCount() == nonPartitionColFilterProto.getValueCount(),
          "ValueListFilter %s count mismatched. Expected %s, found %s", fieldName,
          nonPartitionColFilterProto.getValueCount(), valueListFilter.getValueCount());
        valueListFilterPieces.add(valueListFilter);
      }

      final RuntimeFilterManager.RuntimeFilterManagerEntry filterManagerEntry;
      filterManagerEntry = filterManager.coalesce(runtimeFilter, Optional.ofNullable(bloomFilterPiece), valueListFilterPieces, message.getSendingMinorFragmentId());

      if (filterManagerEntry.isComplete() && !filterManagerEntry.isDropped()) {
        // composite filter is ready for further processing - no more pieces expected
        logger.debug("All pieces of runtime filter received. Sending to probe scan now. " + filterManagerEntry.getProbeScanCoordinates());
        Optional<RuntimeFilterProbeTarget> probeNode = config.getRuntimeFilterInfo().getRuntimeFilterProbeTargets()
          .stream()
          .filter(pt -> pt.isSameProbeCoordinate(filterManagerEntry.getCompositeFilter().getProbeScanMajorFragmentId(),
            filterManagerEntry.getCompositeFilter().getProbeScanOperatorId()))
          .findFirst();
        if (probeNode.isPresent()) {
          RuntimeFilterUtil.sendRuntimeFilterToProbeScan(filterManagerEntry.getCompositeFilter(),
            Optional.ofNullable(filterManagerEntry.getPartitionColFilter()),
            filterManagerEntry.getNonPartitionColFilters(), context, config);
          filterManager.remove(filterManagerEntry);
        } else {
          logger.warn("Node coordinates not found for probe target:{}", filterManagerEntry.getProbeScanCoordinates());
        }
      }
    } catch (Exception e) {
      filterManager.incrementDropCount();
      logger.warn("Error while merging runtime filter piece from " + message.getSendingMinorFragmentId(), e);
    }
  }
}
