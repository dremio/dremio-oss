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
package com.dremio.sabot.op.fromjson;

import static com.dremio.exec.proto.UserBitShared.CoreOperatorType.CONVERT_FROM_JSON_VALUE;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Field;

@JsonTypeName("convert-from-json")
public class ConvertFromJsonPOP extends AbstractSingle {

  private List<ConversionColumn> columns;

  @JsonCreator
  public ConvertFromJsonPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("columns") List<ConversionColumn> columns) {
    super(props, child);
    this.columns = columns;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitConvertFromJson(this, value);
  }

  @Override
  public int getOperatorType() {
    return CONVERT_FROM_JSON_VALUE;
  }

  public List<ConversionColumn> getColumns() {
    return columns;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new ConvertFromJsonPOP(props, child, columns);
  }

  public static enum OriginType {
    LITERAL,
    RAW
  }

  public static class ConversionColumn {
    private final OriginType originType;
    private final List<String> originTable;
    private final String originField;
    private final CompleteType type;
    private final String inputField;

    @JsonCreator
    public ConversionColumn(
        @JsonProperty("originType") OriginType originType,
        @JsonProperty("originTable") List<String> originTable,
        @JsonProperty("originField") String originField,
        @JsonProperty("inputField") String inputField,
        @JsonProperty("type") CompleteType type) {
      super();
      this.originType = originType;
      this.originTable = originTable;
      this.originField = originField;
      this.inputField = inputField;
      this.type = type;
    }

    public OriginType getOriginType() {
      return originType;
    }

    public List<String> getOriginTable() {
      return originTable;
    }

    public String getInputField() {
      return inputField;
    }

    public String getOriginField() {
      return originField;
    }

    public CompleteType getType() {
      return type;
    }

    public Field asField(String name) {
      return type.toField(name);
    }

    @Override
    public String toString() {
      return String.format("originField='%s', inputField='%s'", originField, inputField);
    }
  }

  public BatchSchema getSchema(BatchSchema schema) {
    final Map<String, ConversionColumn> cMap = new HashMap<>();
    for (ConversionColumn c : columns) {
      cMap.put(c.getInputField().toLowerCase(), c);
    }

    final SchemaBuilder builder = BatchSchema.newBuilder();
    for (Field f : schema) {
      ConversionColumn conversion = cMap.get(f.getName().toLowerCase());
      if (conversion != null) {
        builder.addField(conversion.asField(f.getName()));
      } else {
        builder.addField(f);
      }
    }

    builder.setSelectionVectorMode(SelectionVectorMode.NONE);
    return builder.build();
  }
}
