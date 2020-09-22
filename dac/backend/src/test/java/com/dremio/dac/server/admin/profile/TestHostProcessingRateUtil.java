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

import static com.dremio.dac.server.admin.profile.HostProcessingRateUtil.computeRecordProcRateAtPhaseOperatorHostLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;

import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

/**
 * Test class for HostProcessingRateUtil
 */
public class TestHostProcessingRateUtil {

  @Test
  public void testComputeRecordProcRateAtPhaseHostLevel() {
    int major = 1;
    String hostname1 = "hostname1";
    String hostname2 = "hostname2";

    HostProcessingRate hpr1 = new HostProcessingRate(major, hostname1, BigInteger.valueOf(5),
                                                     BigInteger.valueOf(5_000_000_000L), BigInteger.valueOf(1));
    HostProcessingRate hpr2 = new HostProcessingRate(major, hostname1, BigInteger.valueOf(40),
                                                     BigInteger.valueOf(10_000_000_000L), BigInteger.valueOf(2));
    HostProcessingRate hpr3 = new HostProcessingRate(major, hostname2, BigInteger.valueOf(80),
                                                     BigInteger.valueOf(20_000_000_000L), BigInteger.valueOf(3));
    HostProcessingRate hpr4 = new HostProcessingRate(major+1, hostname1, BigInteger.valueOf(70),
                                                     BigInteger.valueOf(20), BigInteger.valueOf(3));

    Set<HostProcessingRate> unAggregatedHostProcessingRate = Sets.newHashSet(hpr1, hpr2, hpr3, hpr4);

    Map<String, BigDecimal> expectedRecordProcRate = new HashMap<>();
    expectedRecordProcRate.put(hostname1, BigDecimal.valueOf(3));
    expectedRecordProcRate.put(hostname2, BigDecimal.valueOf(4));

    Map<String, BigInteger> expectedNumThreads = new HashMap<>();
    expectedNumThreads.put(hostname1, BigInteger.valueOf(3));
    expectedNumThreads.put(hostname2, BigInteger.valueOf(3));

    Set<HostProcessingRate> aggregatedHostProcessingRate = HostProcessingRateUtil
      .computeRecordProcRateAtPhaseHostLevel(major,unAggregatedHostProcessingRate);

    verifyAggreatedRecordProcessingRates(major, expectedRecordProcRate, expectedNumThreads, aggregatedHostProcessingRate);
  }

  private void verifyAggreatedRecordProcessingRates(int major, Map<String, BigDecimal> expectedRecordProcRate, Map<String, BigInteger> expectedNumThreads, Set<HostProcessingRate> aggregatedHostProcessingRate) {
    Set<String> hostAlreadySeen = new HashSet<>();
    for (HostProcessingRate hpr: aggregatedHostProcessingRate) {
      assertEquals("There should not be a HostProcessingRate record with different phase(major).",
                   major, hpr.getMajor().intValue());

      assertFalse("There should be only one entry per hostname in aggregated set.",
                  hostAlreadySeen.contains(hpr.getHostname()));
      hostAlreadySeen.add(hpr.getHostname());

      // This also inherits checks if "Total Max Records" and "Total Process Time"
      // is correctly computed.
      assertEquals("Record processing rate computed is incorrect.",
                   expectedRecordProcRate.get(hpr.getHostname()),
                   hpr.computeProcessingRate());

      assertEquals("Num Threads computed is incorrect.",
                   expectedNumThreads.get(hpr.getHostname()),
                   hpr.getNumThreads());
    }
  }

  @Test
  public void testComputeRecordProcRateAtPhaseOperatorHostLevel() {
    int major = 1;
    int minor1 = 1;
    int minor2 = 2;
    int minor3 = 3;
    String hostname1 = "hostname1";
    String hostname2 = "hostname2";

    Table<Integer, Integer, String> majorMinorHostTable = HashBasedTable.create();
    majorMinorHostTable.put(major, minor1, hostname1);
    majorMinorHostTable.put(major, minor2, hostname2);
    majorMinorHostTable.put(major, minor3, hostname1);

    StreamProfile streamProfile1 = StreamProfile.newBuilder().setRecords(40).build();
    StreamProfile streamProfile2 = StreamProfile.newBuilder().setRecords(20).build();
    StreamProfile streamProfile3 = StreamProfile.newBuilder().setRecords(60).build();

    OperatorProfile op1 = OperatorProfile.newBuilder().addInputProfile(streamProfile1)
                                                      .setProcessNanos(10_000_000_000L).build();
    OperatorProfile op2 = OperatorProfile.newBuilder().addInputProfile(streamProfile2)
                                                      .setProcessNanos(20_000_000_000L).build();
    OperatorProfile op3 = OperatorProfile.newBuilder().addInputProfile(streamProfile3)
                                                      .setProcessNanos(30_000_000_000L).build();

    ImmutablePair<OperatorProfile, Integer> ip1 = ImmutablePair.of(op1, minor1);
    ImmutablePair<OperatorProfile, Integer> ip2 = ImmutablePair.of(op2, minor2);
    ImmutablePair<OperatorProfile, Integer> ip3 = ImmutablePair.of(op3, minor3);

    List<ImmutablePair<OperatorProfile, Integer>> ops = Lists.newArrayList(ip1, ip2, ip3);

    Map<String, BigDecimal> expectedRecordProcRate = new HashMap<>();
    expectedRecordProcRate.put(hostname1, BigDecimal.valueOf(2.5));
    expectedRecordProcRate.put(hostname2, BigDecimal.valueOf(1));

    Map<String, BigInteger> expectedNumThreads = new HashMap<>();
    expectedNumThreads.put(hostname1, BigInteger.valueOf(2));
    expectedNumThreads.put(hostname2, BigInteger.valueOf(1));

    Set<HostProcessingRate> aggregatedHostProcessingRate =
      computeRecordProcRateAtPhaseOperatorHostLevel(major, ops, majorMinorHostTable);

    verifyAggreatedRecordProcessingRates(major, expectedRecordProcRate, expectedNumThreads, aggregatedHostProcessingRate);
  }
}
