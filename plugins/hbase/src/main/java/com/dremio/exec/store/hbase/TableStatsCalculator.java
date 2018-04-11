/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dremio.common.config.SabotConfig;

/**
 * Computes size of each region for given table.
 */
public class TableStatsCalculator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableStatsCalculator.class);

  public static final long DEFAULT_ROW_COUNT = 1024L * 1024L;

  private static final String DREMIO_EXEC_HBASE_SCAN_SAMPLE_ROWS_COUNT = "dremio.exec.hbase.scan.samplerows.count";

  private static final int DEFAULT_SAMPLE_SIZE = 100;

  /**
   * Maps each region to its size in bytes.
   */
  private Map<byte[], Long> sizeMap = null;

  private int avgRowSizeInBytes = 1;

  private int colsPerRow = 1;

  /**
   * Computes size of each region for table.
   *
   * @param conn
   * @param hbaseScanSpec
   * @param config
   * @throws IOException
   */
  public TableStatsCalculator(Connection conn, TableName tableName, SabotConfig config, boolean enabled) throws IOException {

    try (Admin admin = conn.getAdmin();
         Table table = conn.getTable(tableName);
         RegionLocator locator = conn.getRegionLocator(tableName)) {
      int rowsToSample = rowsToSample(config);
      if (rowsToSample > 0) {
        Scan scan = new Scan();
        scan.setCaching(rowsToSample < DEFAULT_SAMPLE_SIZE ? rowsToSample : DEFAULT_SAMPLE_SIZE);
        scan.setMaxVersions(1);
        ResultScanner scanner = table.getScanner(scan);
        long rowSizeSum = 0;
        int numColumnsSum = 0, rowCount = 0;
        for (; rowCount < rowsToSample; ++rowCount) {
          Result row = scanner.next();
          if (row == null) {
            break;
          }
          numColumnsSum += row.size();
          Cell[] cells = row.rawCells();
          if (cells != null) {
            for (Cell cell : cells) {
              rowSizeSum += CellUtil.estimatedSerializedSizeOf(cell);
            }
          }
        }
        if (rowCount > 0) {
          avgRowSizeInBytes = (int) (rowSizeSum/rowCount);
          colsPerRow = numColumnsSum/rowCount;
        }
        scanner.close();
      }

      if (!enabled) {
        logger.info("Region size calculation disabled.");
        return;
      }

      logger.info("Calculating region sizes for table '{}'.", tableName.getNameAsString());

      //get regions for table
      List<HRegionLocation> tableRegionInfos = locator.getAllRegionLocations();
      Set<byte[]> tableRegions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      for (HRegionLocation regionInfo : tableRegionInfos) {
        tableRegions.add(regionInfo.getRegionInfo().getRegionName());
      }

      ClusterStatus clusterStatus = null;
      try {
        clusterStatus = admin.getClusterStatus();
      } catch (Exception e) {
        logger.debug(e.getMessage());
      } finally {
        if (clusterStatus == null) {
          return;
        }
      }

      sizeMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

      Collection<ServerName> servers = clusterStatus.getServers();
      //iterate all cluster regions, filter regions from our table and compute their size
      for (ServerName serverName : servers) {
        ServerLoad serverLoad = clusterStatus.getLoad(serverName);

        for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
          byte[] regionId = regionLoad.getName();

          if (tableRegions.contains(regionId)) {
            long regionSizeMB = regionLoad.getMemStoreSizeMB() + regionLoad.getStorefileSizeMB();
            sizeMap.put(regionId, (regionSizeMB > 0 ? regionSizeMB : 1) * (1024*1024));
            if (logger.isDebugEnabled()) {
              logger.debug("Region " + regionLoad.getNameAsString() + " has size " + regionSizeMB + "MB");
            }
          }
        }
      }
      logger.debug("Region sizes calculated");
    }

  }

  private int rowsToSample(SabotConfig config) {
    return config.hasPath(DREMIO_EXEC_HBASE_SCAN_SAMPLE_ROWS_COUNT)
        ? config.getInt(DREMIO_EXEC_HBASE_SCAN_SAMPLE_ROWS_COUNT) : DEFAULT_SAMPLE_SIZE;
  }

  /**
   * Returns size of given region in bytes. Returns 0 if region was not found.
   */
  public long getRegionSizeInBytes(byte[] regionId) {
    if (sizeMap == null) {
      return avgRowSizeInBytes * DEFAULT_ROW_COUNT; // 1 million rows
    } else {
      Long size = sizeMap.get(regionId);
      if (size == null) {
        logger.debug("Unknown region:" + Arrays.toString(regionId));
        return 0;
      } else {
        return size;
      }
    }
  }

  public int getAvgRowSizeInBytes() {
    return avgRowSizeInBytes;
  }

  public int getColsPerRow() {
    return colsPerRow;
  }

}
