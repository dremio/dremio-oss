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
package com.dremio.exec.planner.sql.handlers.direct;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFamily;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.SchemaUtilities.TableWithPath;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection.MeasureType;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection.NameAndGranularity;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection.NameAndMeasures;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.accel.LayoutDefinition;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class AccelCreateReflectionHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelCreateReflectionHandler.class);

  private final Catalog catalog;
  private final AccelerationManager accel;
  private final ReflectionContext reflectionContext;
  private final boolean complexTypeSupport;

  public AccelCreateReflectionHandler(Catalog catalog, AccelerationManager accel, ReflectionContext reflectionContext, boolean complexTypeSupport) {
    this.catalog = catalog;
    this.accel = accel;
    this.reflectionContext = reflectionContext;
    this.complexTypeSupport = complexTypeSupport;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlCreateReflection addLayout = SqlNodeUtil.unwrap(sqlNode, SqlCreateReflection.class);
    final TableWithPath table = SchemaUtilities.verify(catalog, addLayout.getTblName());
    SqlIdentifier identifier = addLayout.getName();
    String name;
    if(identifier != null) {
      name = identifier.toString();
    } else {
      name = "Unnamed-" + ThreadLocalRandom.current().nextLong();
    }

    final LayoutDefinition layout = new LayoutDefinition(name,
        addLayout.isRaw() ? LayoutDefinition.Type.RAW : LayoutDefinition.Type.AGGREGATE,
        table.qualifyColumns(addLayout.getDisplayList()),
        qualifyColumnsWithGranularity(table.getTable(), addLayout.getDimensionList()),
        qualifyColumnsWithMeasures(table.getTable(), addLayout.getMeasureList(), complexTypeSupport),
        table.qualifyColumns(addLayout.getSortList()),
        table.qualifyColumns(addLayout.getDistributionList()),
        table.qualifyColumns(addLayout.getPartitionList()),
        addLayout.getArrowCachingEnabled(),
        addLayout.getPartitionDistributionStrategy()
    );
    accel.addLayout(table.getPath(), layout, reflectionContext);
    return Collections.singletonList(SimpleCommandResult.successful("Layout added."));
  }

  /**
   * Map of Valid types and default measure for each.
   */
  private static Map<SqlTypeFamily, TypeHandler> TYPE_ALLOWANCES = ImmutableMap.<SqlTypeFamily, TypeHandler>builder()
      .put(SqlTypeFamily.ANY, new TypeHandler(
          SqlTypeFamily.ANY,
          ImmutableSet.copyOf(MeasureType.values()),
          ImmutableList.of(MeasureType.COUNT, MeasureType.SUM)))
      .put(SqlTypeFamily.NUMERIC, new TypeHandler(
          SqlTypeFamily.NUMERIC,
          ImmutableSet.copyOf(MeasureType.values()),
          ImmutableList.of(MeasureType.COUNT, MeasureType.SUM)))
      .put(SqlTypeFamily.CHARACTER, new TypeHandler(
          SqlTypeFamily.CHARACTER,
          ImmutableSet.of(MeasureType.APPROX_COUNT_DISTINCT, MeasureType.COUNT, MeasureType.MIN, MeasureType.MAX),
          ImmutableList.of(MeasureType.COUNT)))
      .put(SqlTypeFamily.BINARY, new TypeHandler(
          SqlTypeFamily.BINARY,
          ImmutableSet.of(MeasureType.APPROX_COUNT_DISTINCT, MeasureType.COUNT, MeasureType.MIN, MeasureType.MAX),
          ImmutableList.of(MeasureType.COUNT)))
      .put(SqlTypeFamily.DATE, new TypeHandler(
          SqlTypeFamily.CHARACTER,
          ImmutableSet.of(MeasureType.APPROX_COUNT_DISTINCT, MeasureType.COUNT, MeasureType.MIN, MeasureType.MAX),
          ImmutableList.of(MeasureType.COUNT)))
      .put(SqlTypeFamily.TIME, new TypeHandler(
          SqlTypeFamily.TIME,
          ImmutableSet.of(MeasureType.APPROX_COUNT_DISTINCT, MeasureType.COUNT, MeasureType.MIN, MeasureType.MAX),
          ImmutableList.of(MeasureType.COUNT)))
      .put(SqlTypeFamily.DATETIME, new TypeHandler(
          SqlTypeFamily.DATETIME,
          ImmutableSet.of(MeasureType.APPROX_COUNT_DISTINCT, MeasureType.COUNT, MeasureType.MIN, MeasureType.MAX),
          ImmutableList.of(MeasureType.COUNT)))
      .put(SqlTypeFamily.TIMESTAMP, new TypeHandler(
          SqlTypeFamily.TIMESTAMP,
          ImmutableSet.of(MeasureType.APPROX_COUNT_DISTINCT, MeasureType.COUNT, MeasureType.MIN, MeasureType.MAX),
          ImmutableList.of(MeasureType.COUNT)))
      .put(SqlTypeFamily.INTERVAL_DAY_TIME, new TypeHandler(
          SqlTypeFamily.INTERVAL_DAY_TIME,
          ImmutableSet.of(MeasureType.APPROX_COUNT_DISTINCT, MeasureType.COUNT, MeasureType.MIN, MeasureType.MAX, MeasureType.SUM),
          ImmutableList.of(MeasureType.COUNT, MeasureType.SUM)))
      .put(SqlTypeFamily.INTERVAL_YEAR_MONTH, new TypeHandler(
          SqlTypeFamily.INTERVAL_YEAR_MONTH,
          ImmutableSet.of(MeasureType.APPROX_COUNT_DISTINCT, MeasureType.COUNT, MeasureType.MIN, MeasureType.MAX, MeasureType.SUM),
          ImmutableList.of(MeasureType.COUNT, MeasureType.SUM)))
      .put(SqlTypeFamily.BOOLEAN, new TypeHandler(
          SqlTypeFamily.BOOLEAN,
          ImmutableSet.of(MeasureType.APPROX_COUNT_DISTINCT, MeasureType.COUNT, MeasureType.MIN, MeasureType.MAX),
          ImmutableList.of(MeasureType.COUNT)))
      .build();

  private static class TypeHandler {

    private final SqlTypeFamily family;
    private final Set<MeasureType> validMeasures;
    private final List<MeasureType> defaultMeasures;

    public TypeHandler(
        SqlTypeFamily family,
        ImmutableSet<MeasureType> validMeasures,
        ImmutableList<MeasureType> defaultMeasure) {
      this.family = family;
      this.validMeasures = validMeasures;
      this.defaultMeasures = defaultMeasure;
    }

    public List<MeasureType> validate(String name, List<MeasureType> measures) {
      if(measures == null || measures.isEmpty()) {
        // if no measures set, use the default list.
        return defaultMeasures;
      }

      Set<MeasureType> requestedMeasures = ImmutableSet.copyOf(measures);
      Set<MeasureType> invalidTypes = Sets.difference(requestedMeasures, validMeasures);
      if(!invalidTypes.isEmpty()) {
        throw UserException.validationError()
        .message("The following measure type(s) were invalid for the field %s: %s. Valid measures for type %s include: %s.",
            name,
            invalidTypes,
            family,
            validMeasures
          ).build(logger);
      }

      return measures;
    }
  }

  private static List<NameAndGranularity> qualifyColumnsWithGranularity(DremioTable table, List<NameAndGranularity> strings){
    final RelDataType type = table.getRowType(JavaTypeFactoryImpl.INSTANCE);
    return strings.stream().map(input -> {
        RelDataTypeField field = type.getField(input.getName(), false, false);
        if(field == null){
          throw UserException.validationError()
            .message("Unable to find field %s in table %s. Available fields were: %s.",
                input.getName(),
                SqlUtils.quotedCompound(table.getPath().getPathComponents()),
                FluentIterable.from(type.getFieldNames()).transform(SqlUtils.QUOTER).join(Joiner.on(", "))
              ).build(logger);
        }

        return new NameAndGranularity(field.getName(), input.getGranularity());
      }).collect(Collectors.toList());
  }

  private static List<NameAndMeasures> qualifyColumnsWithMeasures(DremioTable table, List<NameAndMeasures> measures, boolean complexTypeSupport){
    // we are using getSchema() instead of getRowType() as it doesn't always report the correct field types for View tables
    final RelDataType type = CalciteArrowHelper.wrap(table.getSchema()).toCalciteRecordType(JavaTypeFactoryImpl.INSTANCE, complexTypeSupport);
    return measures.stream().map(input -> {
        RelDataTypeField field = type.getField(input.getName(), false, false);
        if(field == null){
          throw UserException.validationError()
            .message("Unable to find field %s in table %s. Available fields were: %s.",
                input.getName(),
                SqlUtils.quotedCompound(table.getPath().getPathComponents()),
                FluentIterable.from(type.getFieldNames()).transform(SqlUtils.QUOTER).join(Joiner.on(", "))
              ).build(logger);
        }

        final RelDataTypeFamily dataTypeFamily = field.getType().getFamily();
        TypeHandler handle = null;
        if(dataTypeFamily instanceof SqlTypeFamily) {
          handle = TYPE_ALLOWANCES.get(dataTypeFamily);
        }

        if(handle == null) {
          throw UserException.validationError()
          .message("Unable to configure reflection on field %s in table %s because it was type %s.",
              input.getName(),
              SqlUtils.quotedCompound(table.getPath().getPathComponents()),
              dataTypeFamily
            ).build(logger);
        }

        return new NameAndMeasures(field.getName(), handle.validate(field.getName(), input.getMeasureTypes()));
      }).collect(Collectors.toList());
  }

  public static List<MeasureType> validate(String fieldName, RelDataTypeFamily dataTypeFamily, List<MeasureType> measures) {
    TypeHandler handle = null;
    if(dataTypeFamily instanceof SqlTypeFamily) {
      handle = TYPE_ALLOWANCES.get(dataTypeFamily);
    }

    if(handle == null) {
      throw UserException.validationError()
      .message("Unable to configure reflection on field %s because it was type %s.",
          fieldName,
          dataTypeFamily
        ).build(logger);
    }

    return handle.validate(fieldName, measures);
  }

  public static Set<MeasureType> getValidMeasures(RelDataTypeFamily dataTypeFamily){
    TypeHandler handler = TYPE_ALLOWANCES.get(dataTypeFamily);
    if(handler == null) {
      return ImmutableSet.of();
    }

    return handler.validMeasures;
  }

  public static List<MeasureType> getDefaultMeasures(RelDataTypeFamily dataTypeFamily){
    TypeHandler handler = TYPE_ALLOWANCES.get(dataTypeFamily);
    if(handler == null) {
      return ImmutableList.of();
    }

    return handler.defaultMeasures;
  }

}
