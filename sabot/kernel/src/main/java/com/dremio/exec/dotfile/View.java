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
package com.dremio.exec.dotfile;

import static com.dremio.exec.store.Views.isComplexType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.planner.StarColumnHelper;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

@JsonTypeName("view")
public class View {

  private final String name;
  private final String sql;

  // Expresses the validated row type
  private final List<FieldType> fields;

  // Declared field names in view definition
  private final List<String> fieldNames;

  // flag to support inline update with nested schema support
  private boolean fieldUpdated;

  /* Current schema when view is created (not the schema to which view belongs to) */
  private final List<String> workspaceSchemaPath;

  @JsonInclude(Include.NON_NULL)
  public static class FieldType {

    private final String name;
    private final SqlTypeName type;
    private final Integer precision;
    private final Integer scale;
    private SqlIntervalQualifier intervalQualifier;
    private final Boolean isNullable;
    private final Field field;

    @JsonCreator
    public FieldType(
        @JsonProperty("name")                       String name,
        @JsonProperty("type")                       SqlTypeName type,
        @JsonProperty("precision")                  Integer precision,
        @JsonProperty("scale")                      Integer scale,
        @JsonProperty("startUnit")                  TimeUnit startUnit,
        @JsonProperty("endUnit")                    TimeUnit endUnit,
        @JsonProperty("fractionalSecondPrecision")  Integer fractionalSecondPrecision,
        @JsonProperty("isNullable")                 Boolean isNullable,
        @JsonProperty("field")                      Field field) {
      this.name = name;
      this.type = type;
      this.precision = precision;
      this.scale = scale;
      this.intervalQualifier =
          null == startUnit
          ? null
          : new SqlIntervalQualifier(
              startUnit, precision, endUnit, fractionalSecondPrecision, SqlParserPos.ZERO );

      // Property "isNullable" is not part of the initial view definition and
      // was added in DRILL-2342.  If the default value is null, consider it as
      // "true".  It is safe to default to "nullable" than "required" type.
      this.isNullable = isNullable == null ? true : isNullable;
      this.field = field;
    }

    public FieldType(String name, RelDataType dataType) {
      this.name = name;
      this.type = dataType.getSqlTypeName();
      this.field = getField(name, dataType);

      Integer p = null;
      Integer s = null;

      switch (dataType.getSqlTypeName()) {
      case CHAR:
      case BINARY:
      case VARBINARY:
      case VARCHAR:
        p = dataType.getPrecision();
        break;
      case DECIMAL:
        p = dataType.getPrecision();
        s = dataType.getScale();
        break;
      case TIME:
      case TIMESTAMP:
        p = dataType.getPrecision();
        break;
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        p = dataType.getIntervalQualifier().getStartPrecisionPreservingDefault();
      default:
        break;
      }

      this.precision = p;
      this.scale = s;
      this.intervalQualifier = dataType.getIntervalQualifier();
      this.isNullable = dataType.isNullable();
    }

    private Field getField(String name, RelDataType dataType) {
      if (dataType.isStruct()) {
        BatchSchema schema = CalciteArrowHelper.fromCalciteRowType(dataType);
        return new Field(name, true, new ArrowType.Struct(), schema.getFields());
      } else if(dataType.getSqlTypeName().equals(SqlTypeName.ARRAY)) {
        RelDataType componentType = dataType.getComponentType();
        if (componentType.isStruct() || componentType.getSqlTypeName() == SqlTypeName.ARRAY) {
          return new Field(name, true, new ArrowType.List(), Collections.singletonList(getField("$data$", componentType)));
        } else {
          ArrowType type = MajorTypeHelper.getArrowTypeForMajorType(Types.optional(TypeInferenceUtils.getMinorTypeFromCalciteType(componentType)));
          return new Field(name, true, new ArrowType.List(), Collections.singletonList(new Field("$data$", componentType.isNullable(), type, null)));
        }
      }
      return null;
    }

    /**
     * Gets the name of this field.
     */
    public String getName() {
      return name;
    }

    /**
     * Gets the data type of this field.
     * (Data type only; not full datatype descriptor.)
     */
    public SqlTypeName getType() {
      return type;
    }

    /**
     * Gets the precision of the data type descriptor of this field.
     * The precision is the precision for a numeric type, the length for a
     * string type, or the start unit precision for an interval type.
     * */
    public Integer getPrecision() {
      return precision;
    }

    /**
     * Gets the numeric scale of the data type descriptor of this field,
     * for numeric types.
     */
    public Integer getScale() {
      return scale;
    }

    /**
     * Gets the interval type qualifier of the interval data type descriptor of
     * this field (<i>iff</i> interval type). */
    @JsonIgnore
    public SqlIntervalQualifier getIntervalQualifier() {
      return intervalQualifier;
    }

    /**
     * Gets the time range start unit of the type qualifier of the interval data
     * type descriptor of this field (<i>iff</i> interval type).
     */
    public TimeUnit getStartUnit() {
      return null == intervalQualifier ? null : intervalQualifier.getStartUnit();
    }

    /**
     * Gets the time range end unit of the type qualifier of the interval data
     * type descriptor of this field (<i>iff</i> interval type).
     */
    public TimeUnit getEndUnit() {
      return null == intervalQualifier ? null : intervalQualifier.getEndUnit();
    }

    /**
     * Get Field.
     */
    public Field getField() {
      return field;
    }

    /**
     * Gets the fractional second precision of the type qualifier of the interval
     * data type descriptor of this field (<i>iff</i> interval type).
     * Gets the interval type descriptor's fractional second precision
     * (<i>iff</i> interval type).
     */
    public Integer getFractionalSecondPrecision() {
      return null == intervalQualifier ? null : intervalQualifier.getFractionalSecondPrecisionPreservingDefault();
    }

    /**
     * Gets the nullability of the data type desription of this field.
     */
    public Boolean getIsNullable() {
      return isNullable;
    }

  }

  public View(String name, String sql, RelDataType validatedRowType, List<String> fieldNames, List<String> workspaceSchemaPath) {
    this(name, sql, validatedRowType, fieldNames, workspaceSchemaPath, false);
  }

  public View(String name, String sql, RelDataType validatedRowType, List<String> fieldNames, List<String> workspaceSchemaPath, boolean fieldUpdated) {
    this.name = name;
    this.sql = sql;
    this.fieldUpdated = fieldUpdated;
    fields = Lists.newArrayList();
    List<String> validatedFieldNames = new ArrayList<>();
    for (RelDataTypeField f : validatedRowType.getFieldList()) {
      validatedFieldNames.add(f.getName());
      fields.add(new FieldType(f.getName(), f.getType()));
    }
    this.fieldNames = fieldNames;
    this.workspaceSchemaPath =
        workspaceSchemaPath == null ? ImmutableList.<String>of() : ImmutableList.copyOf(workspaceSchemaPath);
  }

  @JsonCreator
  public View(@JsonProperty("name") String name,
              @JsonProperty("sql") String sql,
              @JsonProperty("fields") List<FieldType> fields,
              @JsonProperty("fieldNames") List<String> fieldNames,
              @JsonProperty("workspaceSchemaPath") List<String> workspaceSchemaPath,
              @JsonProperty("fieldUpdated") boolean fieldUpdated){
    this.name = name;
    this.sql = sql;
    this.fieldNames = fieldNames;
    this.fields = fields;
    this.workspaceSchemaPath =
        workspaceSchemaPath == null ? ImmutableList.<String>of() : ImmutableList.copyOf(workspaceSchemaPath);
    this.fieldUpdated = fieldUpdated;
  }

  public boolean isFieldUpdated() {
    return fieldUpdated;
  }

  public RelDataType getRowType(RelDataTypeFactory factory) {

    List<RelDataType> types = Lists.newArrayList();
    List<String> names = Lists.newArrayList();

    for (FieldType field : fields) {
      names.add(field.getName());
      RelDataType type;
      if (   SqlTypeFamily.INTERVAL_YEAR_MONTH == field.getType().getFamily()
          || SqlTypeFamily.INTERVAL_DAY_TIME   == field.getType().getFamily() ) {
       type = factory.createSqlIntervalType( field.getIntervalQualifier() );
      } else if (isComplexType(field.getType())) {
        Field complexFieldType = field.getField();
        if (complexFieldType != null) {
          type = CalciteArrowHelper.toCalciteFieldType(complexFieldType, factory, true);
        } else {
          // Actual field information for complex type can be absent for the objects from previous version. In this case, use ANY type.
          type = factory.createSqlType(SqlTypeName.ANY);
        }
      } else if (field.getPrecision() == null && field.getScale() == null) {
        type = factory.createSqlType(field.getType());
      } else if (field.getPrecision() != null && field.getScale() == null) {
        type = factory.createSqlType(field.getType(), field.getPrecision());
      } else {
        type = factory.createSqlType(field.getType(), field.getPrecision(), field.getScale());
      }

      if (field.getIsNullable()) {
        types.add(factory.createTypeWithNullability(type, true));
      } else {
        types.add(type);
      }
    }
    return factory.createStructType(types, names);
  }

//  @JsonIgnore
//  public boolean isDynamic(){
//    return fields.isEmpty();
//  }

  @JsonIgnore
  public boolean hasStar() {
    for (FieldType field : fields) {
      if (StarColumnHelper.isNonPrefixedStarColumn(field.getName())) {
        return true;
      }
    }
    return false;
  }

  public String getSql() {
    return sql;
  }

  public String getName() {
    return name;
  }

  public List<FieldType> getFields() {
    return fields;
  }

  public List<String> getWorkspaceSchemaPath() {
    return workspaceSchemaPath;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public boolean hasDeclaredFieldNames() {
    return fieldNames != null && !fieldNames.isEmpty();
  }

  @Override
  public int hashCode() {
    return Objects.hash(Objects.hash(fields), Objects.hash(fieldNames), name, sql, workspaceSchemaPath);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    View other = (View) obj;

    return Objects.deepEquals(fields,  other.fields)
        && Objects.deepEquals(name, other.name)
        && Objects.deepEquals(sql, other.sql)
        && Objects.deepEquals(fieldNames, other.fieldNames)
        && Objects.deepEquals(workspaceSchemaPath, other.workspaceSchemaPath);
  }

  public View withRowType(RelDataType rowType) {
    return new View(name, sql, rowType, fieldNames, workspaceSchemaPath);
  }

}
