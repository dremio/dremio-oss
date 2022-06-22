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
package com.dremio.exec.store.sys.statistics;

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.service.namespace.NamespaceKey;
/**
 * Interface for administrating statistics.
 */
public interface StatisticsAdministrationService {
  String requestStatistics(List<Field> fields, NamespaceKey key, Double samplingRate);

  List<String> deleteStatistics(List<String> fields, NamespaceKey key);

  boolean deleteRowCountStatistics(NamespaceKey key);

  void validate(NamespaceKey key);

  /**
   * Factyory for {@StatisticsAdministrationService}
   */
  interface Factory {
    StatisticsAdministrationService get(String userName);
  }
}
