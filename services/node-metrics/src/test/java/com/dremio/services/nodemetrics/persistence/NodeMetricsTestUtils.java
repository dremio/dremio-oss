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
package com.dremio.services.nodemetrics.persistence;

import com.dremio.plugins.nodeshistory.NodesHistoryStoreConfig;
import com.dremio.services.nodemetrics.ImmutableNodeMetrics;
import com.dremio.services.nodemetrics.NodeMetrics;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import javax.inject.Provider;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;

public final class NodeMetricsTestUtils {
  private NodeMetricsTestUtils() {}

  public static NodeMetrics generateRandomNodeMetrics(NodeType nodeType) {
    Random r = new Random();
    String ip = "192.168." + r.nextInt(256) + "." + r.nextInt(256);
    String host = RandomStringUtils.randomAlphanumeric(8) + ".local";
    int port = new Random().nextInt(65535);
    double cpu = new Random().nextDouble();
    double memory = new Random().nextDouble();

    return new ImmutableNodeMetrics.Builder()
        .setName(ip)
        .setHost(host)
        .setIp(ip)
        .setPort(port)
        .setCpu(cpu)
        .setMemory(memory)
        .setStatus("green")
        .setIsMaster(nodeType == NodeType.MASTER_COORDINATOR)
        .setIsCoordinator(
            nodeType == NodeType.MASTER_COORDINATOR || nodeType == NodeType.COORDINATOR)
        .setIsExecutor(nodeType == NodeType.EXECUTOR)
        .setIsCompatible(true)
        .setNodeTag("tag")
        .setVersion("version")
        .setStart(System.currentTimeMillis())
        .setDetails("")
        .build();
  }

  public static void writeNodeMetrics(
      List<NodeMetrics> nodeMetricsList,
      Provider<NodesHistoryStoreConfig> nodesHistoryStoreConfigProvider)
      throws IOException {
    NodeMetricsFile nodeMetricsFile = NodeMetricsPointFile.newInstance();
    NodeMetricsCsvFormatter csvFormatter = new NodeMetricsCsvFormatter();

    String csv = csvFormatter.toCsv(nodeMetricsList);
    try (final InputStream csvContents = IOUtils.toInputStream(csv, StandardCharsets.UTF_8)) {
      NodeMetricsStorage nodeMetricsStorage =
          new NodeMetricsStorage(nodesHistoryStoreConfigProvider);
      nodeMetricsStorage.write(csvContents, nodeMetricsFile);
    }
  }

  public enum NodeType {
    MASTER_COORDINATOR,
    COORDINATOR,
    EXECUTOR
  };
}
