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
package com.dremio.exec.store.hive.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.dremio.exec.store.TimedRunnable;
import com.dremio.exec.store.hive.Hive3PluginOptions;
import com.dremio.exec.store.hive.HiveConfFactory;
import com.dremio.exec.store.hive.HivePluginOptions;
import com.dremio.options.OptionManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.junit.Test;
import org.mockito.Mockito;

public class TestHive3MetadataUtils {
  @Test
  public void testGetInputSplitSizesTimeoutCalculation() {
    List<InputSplit> inputSplitList = getInputSplitList(6, 10);
    Pair p1 = getTimeOutAndMaxDeltas(inputSplitList);
    inputSplitList = getInputSplitList(6, 61);
    Pair p2 = getTimeOutAndMaxDeltas(inputSplitList);
    assertTrue((long) p1.getLeft() < (long) p2.getLeft());
    assertTrue((long) p1.getRight() < (long) p2.getRight());

    inputSplitList = getInputSplitList(20, 20);
    p1 = getTimeOutAndMaxDeltas(inputSplitList);
    inputSplitList = getInputSplitList(2, 41);
    p2 = getTimeOutAndMaxDeltas(inputSplitList);
    assertTrue((long) p1.getLeft() < (long) p2.getLeft());
    assertTrue((long) p1.getRight() < (long) p2.getRight());

    inputSplitList = getNonOrcInputSplitList(12);
    p1 = getTimeOutAndMaxDeltas(inputSplitList);
    assertEquals(1, (long) p1.getLeft());
    assertEquals(2000L, (long) p1.getRight());
  }

  @Test
  public void testPrintFrequentProperties() {
    Properties properties = getTestProperties();

    Table testTable = Mockito.mock(Table.class);
    when(testTable.getTableName()).thenReturn("test_table");

    assertEquals(
        "Top 3 property prefixes for test_table: "
            + "1: impa count=3 examples=[impala_random_key2,impala_random_key3,impala_random_key1, ...], "
            + "2: spar count=2 examples=[spark_key,spark_key2, ...], "
            + "3: bar count=1 examples=[bar, ...]",
        HiveMetadataUtils.printFrequentProperties(properties, testTable));
  }

  @Test
  public void testPropertiesFiltering() {
    Table testTable = Mockito.mock(Table.class);
    when(testTable.getTableName()).thenReturn("test_table");
    OptionManager optionManager = Mockito.mock(OptionManager.class);
    HiveConf hiveConf = new HiveConf();

    // testing as Hive 3 source, filtering impala.* and spark.*
    Properties properties = getTestProperties();
    when(optionManager.getOption(Hive3PluginOptions.HIVE_PROPERTY_EXCLUSION_REGEX))
        .thenReturn("impala|spark");
    HiveMetadataUtils.filterProperties(properties, testTable, optionManager, hiveConf);
    List<String> filteredKeys =
        properties.entrySet().stream().map(e -> e.getKey().toString()).collect(Collectors.toList());
    assertEquals(2, filteredKeys.size());
    assertTrue(filteredKeys.contains("bar"));
    assertTrue(filteredKeys.contains("foo"));

    // test case for disabled filtering
    properties = getTestProperties();
    when(optionManager.getOption(Hive3PluginOptions.HIVE_PROPERTY_EXCLUSION_REGEX))
        .thenReturn("(?!)");
    HiveMetadataUtils.filterProperties(properties, testTable, optionManager, hiveConf);
    assertEquals(properties, getTestProperties());

    // testing for Hive2 source (aka universal build), filtering only impala
    HiveConfFactory.setHive2SourceType(hiveConf);
    properties = getTestProperties();
    // note: we're setting the Hive2 version of the option below
    when(optionManager.getOption(HivePluginOptions.HIVE_PROPERTY_EXCLUSION_REGEX))
        .thenReturn("impala");
    HiveMetadataUtils.filterProperties(properties, testTable, optionManager, hiveConf);
    filteredKeys =
        properties.entrySet().stream().map(e -> e.getKey().toString()).collect(Collectors.toList());
    assertEquals(4, filteredKeys.size());
    assertTrue(filteredKeys.contains("bar"));
    assertTrue(filteredKeys.contains("foo"));
    assertTrue(filteredKeys.contains("spark_key"));
    assertTrue(filteredKeys.contains("spark_key2"));
  }

  private static Properties getTestProperties() {
    Properties properties = new Properties();
    properties.put("impala_random_key1", "some_value1");
    properties.put("impala_random_key2", "some_value2");
    properties.put("impala_random_key3", "some_value2");
    properties.put("spark_key", "some_value3");
    properties.put("spark_key2", "some_value4");
    properties.put("foo", "small_key");
    properties.put("bar", "small_key2");
    return properties;
  }

  private Pair<Long, Long> getTimeOutAndMaxDeltas(List<InputSplit> inputSplitList) {
    List<TimedRunnable<Long>> jobs = new ArrayList<>();
    long maxDeltas =
        HiveMetadataUtils.populateSplitJobAndGetMaxDeltas(null, null, inputSplitList, jobs);
    long timeOut = HiveMetadataUtils.getMaxTimeoutPerCore(inputSplitList, maxDeltas);
    return Pair.of(maxDeltas, timeOut);
  }

  private List<InputSplit> getInputSplitList(int totalSplits, int totalDeltas) {
    int[] deltaSplitSize = new int[totalSplits];
    Random rand = new Random();
    for (int i = 0; i < totalDeltas; i++) {
      int splitNum = rand.nextInt(totalSplits);
      deltaSplitSize[splitNum]++;
    }

    List<InputSplit> inputSplits = new ArrayList<>();
    for (int i = 0; i < totalSplits; i++) {
      List<AcidInputFormat.DeltaMetaData> deltaMetaDataList = new ArrayList<>();
      for (int j = 0; j < deltaSplitSize[i]; j++) {
        AcidInputFormat.DeltaMetaData delta = new AcidInputFormat.DeltaMetaData();
        deltaMetaDataList.add(delta);
      }
      OrcSplit inputSplit =
          new OrcSplit(null, null, 0, 0, null, null, false, false, deltaMetaDataList, 9, 9, null);

      inputSplits.add(inputSplit);
    }
    return inputSplits;
  }

  private List<InputSplit> getNonOrcInputSplitList(int totalSplits) {
    List<InputSplit> inputSplits = new ArrayList<>();
    for (int i = 0; i < totalSplits; i++) {
      InputSplit inputSplit = new ParquetInputFormat.ParquetSplit();
      inputSplits.add(inputSplit);
    }
    return inputSplits;
  }
}
