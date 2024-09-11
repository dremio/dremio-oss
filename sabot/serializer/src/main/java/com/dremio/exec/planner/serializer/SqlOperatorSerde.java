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
package com.dremio.exec.planner.serializer;

import com.dremio.plan.serialization.PSqlOperator;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Serialize TO <> FROM SqlOperator to Protobuf. */
public final class SqlOperatorSerde {
  private static final Logger logger = LoggerFactory.getLogger(SqlOperatorSerde.class);

  private final SqlOperatorTable sqlOperatorTable;
  private final LegacySqlOperatorSerde legacySqlOperatorSerde;

  public SqlOperatorSerde(SqlOperatorTable sqlOperatorTable) {
    this.sqlOperatorTable = Preconditions.checkNotNull(sqlOperatorTable);
    this.legacySqlOperatorSerde = new LegacySqlOperatorSerde(sqlOperatorTable);
  }

  public SqlOperator fromProto(PSqlOperator o) {
    // Try with the legacy serde and if it fails use the newer one.
    Optional<SqlOperator> fromProtoLegacy = fromProtoLegacy(o);
    if (fromProtoLegacy.isPresent()) {
      return fromProtoLegacy.get();
    }

    // These are the cases that the legacy serde could not handle:
    String name;
    switch (o.getSqlOperatorTypeCase()) {
      case NAME:
        name = o.getName();
        break;

      case DNAME:
        name = o.getDname();
        break;

      default:
        throw new UnsupportedOperationException("Unknown name type: " + o.getSqlOperatorTypeCase());
    }

    List<SqlOperator> overloads =
        sqlOperatorTable.getOperatorList().stream()
            .filter(x -> x.getName().equalsIgnoreCase(name))
            .collect(Collectors.toList());

    if (overloads.size() > 1) {
      // Continue to filter by the operand count:
      List<SqlOperator> filteredOverloads =
          overloads.stream()
              .filter(
                  x -> {
                    try {
                      SqlOperandCountRange sqlOperandCountRange = x.getOperandCountRange();
                      return sqlOperandCountRange.getMax() == o.getMaxOperands()
                          && sqlOperandCountRange.getMin() == o.getMinOperands();
                    } catch (Exception ex) {
                      // Some operators don't implement getOperandCountRange();
                      return true;
                    }
                  })
              .collect(Collectors.toList());
      if (!filteredOverloads.isEmpty()) {
        overloads = filteredOverloads;
      }
    }

    if (overloads.size() > 1) {
      // Then filter by the class name
      List<SqlOperator> filteredOverloads =
          overloads.stream()
              .filter(x -> x.getClass().getName().equalsIgnoreCase(o.getClassName()))
              .collect(Collectors.toList());
      if (!filteredOverloads.isEmpty()) {
        overloads = filteredOverloads;
      }
    }

    if (overloads.isEmpty()) {
      throw new UnsupportedOperationException("Failed to match SQL Operators for: " + o.getName());
    } else if (overloads.size() > 1) {
      // We need an elegant tie breaking algorithm, but for now let's just return the first one:
      return overloads.get(0);
    } else {
      return overloads.get(0);
    }
  }

  private Optional<SqlOperator> fromProtoLegacy(PSqlOperator o) {
    try {
      // We first try to route the call to the legacy serde for backwards compatibility:
      SqlOperator result = legacySqlOperatorSerde.fromProto(o);
      return Optional.of(result);
    } catch (Exception ex) {
      // If we failed to deserialize with the legacy deserializer, then we just try with the newer
      // one.
      logger.debug("Legacy Operator Serde failed to deserialize: {}", o.getName(), ex);
      return Optional.empty();
    }
  }

  public PSqlOperator toProto(SqlOperator o) {
    // Try with the legacy serde and if it fails use the newer one.
    Optional<PSqlOperator> toProtoLegacy = toProtoLegacy(o);
    if (toProtoLegacy.isPresent()) {
      return toProtoLegacy.get();
    }

    PSqlOperator.Builder builder = PSqlOperator.newBuilder();
    // These are the cases that the legacy serde could not handle:
    if (o instanceof SqlFunction) {
      SqlFunction sqlFunction = (SqlFunction) o;
      // If it's a function than we need to use the identifier, which has the full context path
      // built into it.
      builder.setName(sqlFunction.getNameAsId().toString());
    } else {
      builder.setName(o.getName());
    }

    builder.setClassName(o.getClass().getName());

    try {
      builder
          .setMinOperands(o.getOperandCountRange().getMin())
          .setMaxOperands(o.getOperandCountRange().getMax());
    } catch (Exception ex) {
      // Some methods don't implement getOperandCountRange()
    }

    return builder.build();
  }

  private Optional<PSqlOperator> toProtoLegacy(SqlOperator o) {
    try {
      // We first try to route the call to the legacy serde for backwards compatibility:
      PSqlOperator result = legacySqlOperatorSerde.toProto(o);
      return Optional.of(result);
    } catch (Exception ex) {
      // If we failed to deserialize with the legacy deserializer, then we just try with the newer
      // one.
      logger.debug("Legacy Operator Serde failed to serialize: {}", o.getName(), ex);
      return Optional.empty();
    }
  }
}
