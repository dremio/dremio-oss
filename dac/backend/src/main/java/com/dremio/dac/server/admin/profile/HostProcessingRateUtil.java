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

package com.dremio.dac.server.admin.profile;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.dremio.exec.proto.UserBitShared;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 * Util class for computing Record processing rate at various levels.
 */
public class HostProcessingRateUtil {
  private static final String TOTAL_MAX_RECORDS = "maxRecords";
  private static final String TOTAL_PROCESS_TIME = "processTime";
  private static final String TOTAL_NUM_THREADS = "numThreads";

  /**
   * Record processing rate (records/per second) = MaxRecords/Process_Time
   *
   * @param maxRecords
   * @param processNanos
   * @return
   */
  public static BigDecimal computeRecordProcessingRate(BigInteger maxRecords, BigInteger processNanos) {
    return new BigDecimal(maxRecords)
              .multiply(BigDecimal.valueOf(1_000_000_000))
              .divide(new BigDecimal(processNanos), MathContext.DECIMAL32);
  }

  /**
   * Compute record processing rate at phase(major)-operator-host level.
   *
   * @param major - given phase (major)
   * @param ops - operatorProfile
   * @param majorMinorHostTable
   * @return
   */
  public static Set<HostProcessingRate> computeRecordProcRateAtPhaseOperatorHostLevel(int major,
                                                                                      List<ImmutablePair<UserBitShared.OperatorProfile, Integer>> ops,
                                                                                      Table<Integer, Integer, String> majorMinorHostTable) {
    Table<String, String, BigInteger> hostToColumnToValueTable = HashBasedTable.create();
    buildOperatorHostMetrics(major, ops, majorMinorHostTable, hostToColumnToValueTable);
    return constructHostProcessingRateSet(major, hostToColumnToValueTable);
  }

  private static Set<HostProcessingRate> constructHostProcessingRateSet(int major, Table<String, String, BigInteger> hostToColumnToValueTable) {
    // Using treeSet to sort elements in set.
    Set<HostProcessingRate> set = new TreeSet<>();

    if (hostToColumnToValueTable.isEmpty()) {
      return set;
    }

    for(String hostname: hostToColumnToValueTable.rowKeySet()) {
      set.add(new HostProcessingRate(major,
                                     hostname,
                                     hostToColumnToValueTable.get(hostname, TOTAL_MAX_RECORDS),
                                     hostToColumnToValueTable.get(hostname, TOTAL_PROCESS_TIME),
                                     hostToColumnToValueTable.get(hostname, TOTAL_NUM_THREADS)));
    }
    return set;
  }

  /**
   * Build operator-host metrics in the input hostToColumnToValueTable.
   */
  private static void buildOperatorHostMetrics(int major,
                                               List<ImmutablePair<UserBitShared.OperatorProfile, Integer>> ops,
                                               Table<Integer, Integer, String> majorMinorHostTable,
                                               Table<String, String, BigInteger> hostToColumnToValueTable) {
    for (ImmutablePair<UserBitShared.OperatorProfile, Integer> ip : ops) {
      int minor = ip.getRight();
      UserBitShared.OperatorProfile op = ip.getLeft();

      long maxRecords = Long.MIN_VALUE;
      for (UserBitShared.StreamProfile sp : op.getInputProfileList()) {
        maxRecords = Math.max(sp.getRecords(), maxRecords);
      }

      String hostname = majorMinorHostTable.get(major, minor);
      aggregateHostMetrics(hostname,
                           BigInteger.valueOf(maxRecords),
                           BigInteger.valueOf(op.getProcessNanos()),
                           BigInteger.ONE ,
                           hostToColumnToValueTable);
    }
  }

  /**
   * Helper method to aggregate host metrics in the input hostToColumnToValueTable.
   */
  private static void aggregateHostMetrics(String hostname, BigInteger maxRecords, BigInteger processNanos, BigInteger numThreads,
                                           Table<String, String, BigInteger> hostToColumnToValueTable) {
    BigInteger totalMaxRecords = BigInteger.ZERO;
    BigInteger totalProcessNanos = BigInteger.ZERO;
    BigInteger totalNumThreads = BigInteger.ZERO;

    if(hostToColumnToValueTable.containsRow(hostname)) {
      totalMaxRecords = hostToColumnToValueTable.get(hostname, TOTAL_MAX_RECORDS);
      totalProcessNanos = hostToColumnToValueTable.get(hostname, TOTAL_PROCESS_TIME);
      totalNumThreads = hostToColumnToValueTable.get(hostname, TOTAL_NUM_THREADS);
    }

    totalMaxRecords = totalMaxRecords.add(maxRecords);
    totalProcessNanos = totalProcessNanos.add(processNanos);
    totalNumThreads = totalNumThreads.add(numThreads);

    hostToColumnToValueTable.put(hostname, TOTAL_MAX_RECORDS, totalMaxRecords);
    hostToColumnToValueTable.put(hostname, TOTAL_PROCESS_TIME, totalProcessNanos);
    hostToColumnToValueTable.put(hostname, TOTAL_NUM_THREADS, totalNumThreads);
  }

  /**
   * Compute record processing rate at phase(major)-host level.
   *
   * @param hostProcessingRateSet  Set of HostProcessingRate of same phase(major). Multiple elements might have same host.
   * @return
   */
  public static Set<HostProcessingRate> computeRecordProcRateAtPhaseHostLevel(int major, Set<HostProcessingRate> hostProcessingRateSet) {
    if (hostProcessingRateSet == null || hostProcessingRateSet.isEmpty()) {
      return new TreeSet<>();
    }
    Table<String, String, BigInteger> hostToColumnToValueTable = HashBasedTable.create();
    buildPhaseHostMetrics(major, hostProcessingRateSet, hostToColumnToValueTable);
    return constructHostProcessingRateSet(major, hostToColumnToValueTable);
  }

  /**
   * Build phase-host metrics in the input hostToColumnToValueTable.
   */
  private static void buildPhaseHostMetrics(int major,
                                            Set<HostProcessingRate> hostProcessingRateSet,
                                            Table<String, String, BigInteger> hostToColumnToValueTable) {
    for (HostProcessingRate hpr: hostProcessingRateSet) {
      if (major != hpr.getMajor()) {
        // skip elements in set which are not matching the major
        continue;
      }
      aggregateHostMetrics(hpr.getHostname(),
                       hpr.getNumRecords(),
                       hpr.getProcessNanos(),
                       hpr.getNumThreads(),
                       hostToColumnToValueTable);
    }
  }
}
