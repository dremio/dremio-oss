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

package com.dremio.sabot.op.sender.partition.vectorized;

import static com.dremio.exec.ExecConstants.ADAPTIVE_HASH;
import static com.dremio.exec.ExecConstants.ADAPTIVE_HASH_DOP_RATIO;
import static com.dremio.exec.ExecConstants.ADAPTIVE_HASH_START_ROWCOUNT;

import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sender.partition.PartitionSenderOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * This operator allows adaptive distribution 1. Regular Hash distribution based on distribution
 * columns 2. If data skew is detected, switch to Round-Robin fashion distribution based on the DOP
 *
 * <p>When dynamic DOP adjustment is enabled, * collect partitions/counts for some time * send to
 * other peer operators about partitions/count seen via OOB * collect partitions/counts from all
 * peers via OOB * calculate DOP based on partitions/counts
 *
 * <p>Note: * DOP could be at partition value level, currently, it is a single value set for all
 * partition values * DOP could range from 1 ~ maximal width. Currently, it is set either 1, or
 * maximal width (when data skew is detected)
 */
public class AdaptiveVectorizedPartitionSenderOperator extends VectorizedPartitionSenderOperator {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AdaptiveVectorizedPartitionSenderOperator.class);

  // partition value and corresponding counters for current fragment
  private final Map<Integer, Long> partitionCounters = new HashMap<>();

  private int dop;

  // initial dop value during setup
  // there could be multiple rounds of dop adjustment.
  // If we decide not go with maximal dop, the dop will be reset to the initial value
  private int initialDop;

  // the minimal row count starting Dop adjustment
  private long dopAdjustmentRowCount;

  private long seenRecords = 0;

  // if allow adaptive hash
  private boolean allowAdaptiveHash;

  // To limit the OOB message size, only add top n partition counts from each fragment in the
  // message
  private static int TOP_N_PARTITIONS = 5;

  // maximal number of allowed partitions. Adaptive hashing will be disabled if the number of seen
  // partitions across all fragments exceeds this value
  private static int MAX_PARTITIONS = 1;

  // data skew threshold: records of top N partitions / total records
  private static double DATA_SKEW_RATIO = 0.8d;

  // the default timeout to receive partition counts OOB messages after send OOB messages
  private static int DEFAULT_PARTITION_COUNTS_OOB_WAIT_TIME_MS = 1000;

  // Memory guard: the maximal number of partition values
  // adaptive hashing will be disabled if this threshold is reached
  private static int MAX_PARTITION_COUNT = 10000;

  // the minimal receiving rate for partition counts OOB messages
  // dont do dop calculation/adjustment if the receiving rate is lower than this number
  private static double PARTITION_COUNTS_MSG_RECEIVING_RATE = 0.8d;

  // the time to send the partitionCounts OOB message
  private Long partitionCountsOOBMessageSentTime = null;

  // number of times DOP adjustments so far
  private int numDopAdjustments = 0;

  // Number of times operator informed others of local seen partition counters
  private int oobSends = 0;

  // Number of times operator received a notification of partition counters from peers.
  private int oobReceives = 0;

  // partition counts collected from all participants in the cluster
  private Map<Integer, ExecProtos.HashDistributionValueCounts> participantsPartitionCounters =
      new HashMap<>();

  // the expected PartitionCounts OOB messages to receive
  private int expectedReceivedPartitionCountsMsgCount;

  // test only
  private boolean forceAdjustDop = false;

  public AdaptiveVectorizedPartitionSenderOperator(
      final OperatorContext context,
      final TunnelProvider tunnelProvider,
      final HashPartitionSender config) {
    super(context, tunnelProvider, config);
  }

  @Override
  public void setup(VectorAccessible incoming) throws Exception {
    super.setup(incoming);

    double dopRatio = (int) context.getOptions().getOption(ADAPTIVE_HASH_DOP_RATIO);
    Preconditions.checkArgument(
        dopRatio >= 0 && dopRatio <= 1, "HASH_GROUP_DOP_RATIO should between 0 and 1");

    dopAdjustmentRowCount = context.getOptions().getOption(ADAPTIVE_HASH_START_ROWCOUNT);
    allowAdaptiveHash = context.getOptions().getOption(ADAPTIVE_HASH);
    dop = dopRatio == 1 ? numReceivers : (int) (dopRatio * numReceivers) + 1;
    initialDop = dop;

    // expect all minor fragments send partition count OOB messages
    expectedReceivedPartitionCountsMsgCount =
        context.getAssignments().stream().mapToInt(a -> a.getMinorFragmentIdList().size()).sum();
  }

  @Override
  protected OutgoingBatch getBatch(int partition, OutgoingBatch[] modLookup) {
    OutgoingBatch batch = super.getBatch(partition, modLookup);

    if (!allowAdaptiveHash) {
      return batch;
    }

    long partitionValueCount =
        partitionCounters.put(partition, partitionCounters.computeIfAbsent(partition, p -> 0L) + 1);

    if (partitionCounters.size() > MAX_PARTITION_COUNT) {
      allowAdaptiveHash = false;
      partitionCounters.clear();
      return batch;
    }

    // send partition counts OOB messages if
    // * they are not sent yet
    // * this operator has seen enough rows
    if (partitionCountsOOBMessageSentTime == null && ++seenRecords % dopAdjustmentRowCount == 0) {
      notifyOthersOfPartitionCounts();
    }

    if (numDopAdjustments == 0 && shouldAdjustDop()) {
      adjustDop();
    }

    if (dop > 1) {
      int currentReceiver = Math.min(batch.getBatchIdx(), batch.getNextBatchIdx());
      // Round-robin distribution at dop width
      int newReceiver = (int) (currentReceiver + (partitionValueCount % dop) % numReceivers);
      return modLookup[newReceiver];
    }

    return batch;
  }

  private void notifyOthersOfPartitionCounts() {
    try {
      ExecProtos.HashDistributionValueCounts.Builder partitionValueCountsBuilder =
          ExecProtos.HashDistributionValueCounts.newBuilder();
      PriorityQueue<Map.Entry<Integer, Long>> pq = getTopNPartitionCounts();
      while (!pq.isEmpty()) {
        Map.Entry<Integer, Long> partitionCountPair = pq.poll();
        ExecProtos.HashDistributionValueCount partitionValueCount =
            ExecProtos.HashDistributionValueCount.newBuilder()
                .setHashDistributionKey(partitionCountPair.getKey())
                .setCount(partitionCountPair.getValue())
                .build();
        partitionValueCountsBuilder.addHashDistributionValueCounts(partitionValueCount);
      }

      // total seen records so far
      partitionValueCountsBuilder.setTotalSeenRecords(seenRecords);
      // unique partition values so far
      partitionValueCountsBuilder.setUniqueValueCount(partitionCounters.size());

      OutOfBandMessage.Payload payload =
          new OutOfBandMessage.Payload(partitionValueCountsBuilder.build());
      for (CoordExecRPC.FragmentAssignment a : context.getAssignments()) {
        OutOfBandMessage message =
            new OutOfBandMessage(
                context.getFragmentHandle().getQueryId(),
                context.getFragmentHandle().getMajorFragmentId(),
                a.getMinorFragmentIdList(), // include myself. thus, we could calculate OOB
                // receiving window more accurately.
                config.getProps().getOperatorId(),
                context.getFragmentHandle().getMinorFragmentId(),
                payload,
                true);

        CoordinationProtos.NodeEndpoint endpoint =
            context.getEndpointsIndex().getNodeEndpoint(a.getAssignmentIndex());
        context.getTunnelProvider().getExecTunnel(endpoint).sendOOBMessage(message);
        oobSends++;
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "partition value counts from major fragment:{} minor fragment:{} are sent",
            context.getFragmentHandle().getQueryId(),
            context.getFragmentHandle().getMinorFragmentId());
      }
      partitionCountsOOBMessageSentTime = System.currentTimeMillis();
    } catch (Exception ex) {
      logger.warn("Failure while attempting to notify others of partition counts.", ex);
    }
    updateStats();
  }

  @Override
  public void workOnOOB(OutOfBandMessage message) {
    oobReceives++;

    final ExecProtos.HashDistributionValueCounts partitionValueCounts =
        message.getPayload(ExecProtos.HashDistributionValueCounts.PARSER);

    participantsPartitionCounters.put(message.getSendingMinorFragmentId(), partitionValueCounts);

    if (forceAdjustDop || shouldAdjustDop()) {
      adjustDop();
    }
  }

  /**
   * do adjust Dop if received OOB messages from all AdaptiveVectorizedPartitionSenderOperator
   * instances or, timeout, in case of OOB message loss
   */
  private boolean shouldAdjustDop() {
    if (partitionCountsOOBMessageSentTime == null) {
      return false;
    }

    if (expectedReceivedPartitionCountsMsgCount == oobReceives) {
      return true;
    }

    // timeout
    if (System.currentTimeMillis() - partitionCountsOOBMessageSentTime
        >= DEFAULT_PARTITION_COUNTS_OOB_WAIT_TIME_MS) {
      if ((double) oobReceives / expectedReceivedPartitionCountsMsgCount
          > PARTITION_COUNTS_MSG_RECEIVING_RATE) {
        // do dop adjustment if majority OOB message arrive in timeout window
        return true;
      } else {
        // timeout and did not get enough expected OOB messages. dont try to switch to adaptive hash
        allowAdaptiveHash = false;
      }
    }

    return false;
  }

  @VisibleForTesting
  public void setForceAdjustDop(boolean forceAdjustDop) {
    this.forceAdjustDop = forceAdjustDop;
  }

  private void adjustDop() {
    dop =
        calculateDop(
            participantsPartitionCounters.values(),
            TOP_N_PARTITIONS,
            DATA_SKEW_RATIO,
            numReceivers,
            initialDop);

    logger.debug(
        "major fragment:{} minor fragment:{}, Dop is adjusted to {} ",
        context.getFragmentHandle().getQueryId(),
        context.getFragmentHandle().getMinorFragmentId(),
        dop);

    numDopAdjustments++;

    updateStats();
  }

  @VisibleForTesting
  public static int calculateDop(
      Collection<ExecProtos.HashDistributionValueCounts> participantsPartitionCounters,
      int topNPartitions,
      double dataSkewRatio,
      int maxDop,
      int initialDop) {
    long totalSeenRecords = 0;
    // minimal unique partition count
    // we know unique partition counts from each fragment. However, we dont know the total unique
    // partition counts across all fragments
    // since each fragment only send its top N partition values. We could use the max count from all
    // fragments as the lower bound estimate
    long uniquePartitionValueCountLowerBound = 0;
    Map<Long, Long> mergedPartitionCounters = new HashMap<>();
    for (ExecProtos.HashDistributionValueCounts participantsPartitionCounter :
        participantsPartitionCounters) {
      totalSeenRecords += participantsPartitionCounter.getTotalSeenRecords();
      uniquePartitionValueCountLowerBound =
          Math.max(
              uniquePartitionValueCountLowerBound,
              participantsPartitionCounter.getUniqueValueCount());
      for (ExecProtos.HashDistributionValueCount valueCount :
          participantsPartitionCounter.getHashDistributionValueCountsList()) {
        mergedPartitionCounters.put(
            valueCount.getHashDistributionKey(),
            mergedPartitionCounters.computeIfAbsent(valueCount.getHashDistributionKey(), p -> 0L)
                + valueCount.getCount());
      }
    }
    uniquePartitionValueCountLowerBound =
        Math.max(uniquePartitionValueCountLowerBound, mergedPartitionCounters.size());

    // count of unique partition values are more than threshold
    if (uniquePartitionValueCountLowerBound > MAX_PARTITIONS) {
      return initialDop;
    }

    // sort by partition value count in ascending order
    PriorityQueue<Map.Entry<Long, Long>> pq =
        new PriorityQueue<>((a, b) -> (int) (a.getValue() - b.getValue()));
    for (Map.Entry<Long, Long> entry : mergedPartitionCounters.entrySet()) {
      pq.offer(entry);
      if (pq.size() > topNPartitions) {
        pq.poll();
      }
    }

    long topNPartitionCountSum = 0;
    while (!pq.isEmpty()) {
      topNPartitionCountSum += pq.poll().getValue();
    }

    if ((double) topNPartitionCountSum / totalSeenRecords > dataSkewRatio) {
      return maxDop;
    }

    return initialDop;
  }

  private PriorityQueue<Map.Entry<Integer, Long>> getTopNPartitionCounts() {
    // sort by partition value count in ascending order
    PriorityQueue<Map.Entry<Integer, Long>> pq =
        new PriorityQueue<>((a, b) -> (int) (a.getValue() - b.getValue()));

    for (Map.Entry<Integer, Long> entry : partitionCounters.entrySet()) {
      pq.offer(entry);
      if (pq.size() > TOP_N_PARTITIONS) {
        pq.poll();
      }
    }

    return pq;
  }

  private void updateStats() {
    OperatorStats stats = context.getStats();
    stats.setLongStat(PartitionSenderOperator.Metric.OOB_PARTITION_COUNTERS_SENDS, oobSends);
    stats.setLongStat(PartitionSenderOperator.Metric.OOB_PARTITION_COUNTERS_RECEIVES, oobReceives);
    stats.setLongStat(PartitionSenderOperator.Metric.OOB_DOP, dop);
  }

  @Override
  public void noMoreToConsume() throws Exception {
    // send partition counts if not done yet
    if (partitionCountsOOBMessageSentTime == null) {
      notifyOthersOfPartitionCounts();
    }

    super.noMoreToConsume();
  }
}
