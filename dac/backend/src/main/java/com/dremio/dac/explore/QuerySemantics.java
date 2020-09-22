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
package com.dremio.dac.explore;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.ExpCalculatedField;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.Expression;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.OrderDirection;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;

/**
 * Allows us to convert user submitted queries into internal dataset state to provide better queries upon transformation.
 */
public final class QuerySemantics {

  private QuerySemantics() {}

  /**
   * Will parse a sql query and return a Dataset state
   * @return the corresponding state
   */
  public static VirtualDatasetState extract(QueryMetadata metadata) {
    final com.dremio.service.jobs.metadata.proto.VirtualDatasetState pState = metadata.getState();
    final VirtualDatasetState state = new VirtualDatasetState();
    if (pState.hasFrom()) {
      state.setFrom(fromBuf(pState.getFrom()));
    }
    if (pState.getColumnsCount() > 0) {
      state.setColumnsList(
        pState.getColumnsList()
          .stream()
          .map((c) -> new Column(c.getName(), fromBuf(c.getExpression())))
          .collect(Collectors.toList()));
    }
    if (pState.getOrdersCount() > 0) {
      state.setOrdersList(
        pState.getOrdersList()
          .stream()
          .map((o) -> new Order(o.getName(), OrderDirection.valueOf(o.getDirection().name())))
          .collect(Collectors.toList())
      );
    }
    state.setReferredTablesList(pState.getReferredTablesList());
    state.setContextList(pState.getContextList());
    return state;
  }

  public static void populateSemanticFields(List<ViewFieldType> viewFieldTypes, VirtualDatasetState state){
    List<Column> columns = new ArrayList<>();
    for (ViewFieldType fieldType : viewFieldTypes) {
      ExpColumnReference colRef = new ExpColumnReference(fieldType.getName());
      columns.add(new Column(fieldType.getName(), colRef.wrap()));
    }
    state.setColumnsList(columns);
  }

  private static From fromBuf(com.dremio.service.jobs.metadata.proto.From pFrom) {
    switch (pFrom.getType()) {
      case SQL:
        return new FromSQL(pFrom.getValue()).setAlias(pFrom.getAlias()).wrap();
      case TABLE:
        return new FromTable(pFrom.getValue()).setAlias(pFrom.getAlias()).wrap();
      default:
        throw new IllegalStateException("Unrecognized FromType " + pFrom.getType());
    }
  }

  private static Expression fromBuf(com.dremio.service.jobs.metadata.proto.Expression pExpression) {
    switch (pExpression.getType()) {
      case REFERENCE:
        return new ExpColumnReference(pExpression.getValue()).wrap();
      case CALCULATED:
        return new ExpCalculatedField(pExpression.getValue()).wrap();
      default:
        throw new IllegalStateException("Unrecognized ExpressionType " + pExpression.getType());
    }
  }

}
