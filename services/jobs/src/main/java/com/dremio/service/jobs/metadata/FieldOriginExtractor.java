/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.jobs.metadata;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;

/**
 * Extract field origin information from a query.
 * A parent dataset is a dataset referred to by a query.
 * Here we return the mapping of fields to columns in parent datasets (their origin).
 */
public class FieldOriginExtractor {

  /**
   * extract origins of the fields
   * @param graph the root node of the query
   * @param rowType the rowType after validation
   * @return the origins of the fields in the query. at the same index as the original rowType
   */
  public static List<FieldOrigin> getFieldOrigins(RelNode graph, RelDataType rowType) {
    if (graph.getRowType().getFieldCount() != rowType.getFieldCount()) {
      throw new IllegalArgumentException(format(
          "graph and rowType should have the same field count:\ngraph: %s\nrowType: %s",
          graph, rowType));
    }
    List<FieldOrigin> definitions = new ArrayList<>();
    RelMetadataQuery query = graph.getCluster().getMetadataQuery();
    for (int i = 0; i < graph.getRowType().getFieldCount(); i++) {
      Set<RelColumnOrigin> origins = query.getColumnOrigins(graph, i);
      List<Origin> namedOrigins = new ArrayList<>();
      for (RelColumnOrigin relColumnOrigin : origins) {
        List<String> table = Origins.getTable(relColumnOrigin);
        String colName = Origins.getColName(relColumnOrigin);
        namedOrigins.add(new Origin(colName, relColumnOrigin.isDerived()).setTableList(table));
      }
      // we need the rowtype after validation and before planning
      // graph.getRowType() may not have to be the user facing rowtype anymore
      // (even if it often does)
      String name = rowType.getFieldList().get(i).getName();
      definitions.add(i, new FieldOrigin(name).setOriginsList(namedOrigins));
    }
    return definitions;
  }
}
