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
package com.dremio.exec.planner;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.DirectProvider;
import com.dremio.test.DremioTest;
import javax.inject.Provider;
import org.mockito.Mockito;

public class PhysicalPlanReaderTestFactory {

  public static LogicalPlanPersistence defaultLogicalPlanPersistence(ScanResult scanResult) {
    return new LogicalPlanPersistence(scanResult);
  }

  public static PhysicalPlanReader defaultPhysicalPlanReader(
      ScanResult scanResult, Provider<CatalogService> catalogService) {
    SabotContext sabotContext = Mockito.mock(SabotContext.class);
    Mockito.when(sabotContext.getConnectionReaderProvider())
        .thenReturn(
            DirectProvider.wrap(
                ConnectionReader.of(
                    DremioTest.CLASSPATH_SCAN_RESULT, DremioTest.DEFAULT_SABOT_CONFIG)));
    return new PhysicalPlanReader(
        scanResult,
        new LogicalPlanPersistence(scanResult),
        CoordinationProtos.NodeEndpoint.getDefaultInstance(),
        catalogService,
        sabotContext);
  }

  public static PhysicalPlanReader defaultPhysicalPlanReader(ScanResult scanResult) {
    return defaultPhysicalPlanReader(scanResult, DirectProvider.<CatalogService>wrap(null));
  }

  public static PhysicalPlanReader defaultPhysicalPlanReader(SabotContext c) {
    return defaultPhysicalPlanReader(c, null);
  }

  public static PhysicalPlanReader defaultPhysicalPlanReader(
      SabotContext c, Provider<CatalogService> catalogService) {
    return new PhysicalPlanReader(
        c.getClasspathScan(),
        c.getLpPersistence(),
        CoordinationProtos.NodeEndpoint.getDefaultInstance(),
        catalogService,
        Mockito.mock(SabotContext.class));
  }
}
