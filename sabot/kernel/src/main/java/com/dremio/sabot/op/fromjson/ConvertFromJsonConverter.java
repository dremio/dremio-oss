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

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.visitor.BasePrelVisitor;
import com.dremio.exec.record.VectorAccessibleComplexWriter;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.easy.json.JsonProcessor.ReadState;
import com.dremio.exec.vector.complex.fn.JsonReader;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPOP.ConversionColumn;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPOP.OriginType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class ConvertFromJsonConverter extends BasePrelVisitor<Prel, Void, RuntimeException> {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ConvertFromJsonConverter.class);
  public static final String FAILURE_MSG =
      "Using CONVERT_FROM(*, 'JSON') is only supported against string literals and field references of types VARCHAR and VARBINARY.";

  private final RelMetadataQuery query;
  private final QueryContext context;
  private final RelDataTypeFactory factory;

  public ConvertFromJsonConverter(QueryContext context, RelOptCluster cluster) {
    this.context = context;
    this.query = cluster.getMetadataQuery();
    this.factory = cluster.getTypeFactory();
  }

  @Override
  public Prel visitProject(ProjectPrel topRel, Void voidValue) throws RuntimeException {
    final List<RexNode> expressions = topRel.getProjects();
    final RelNode inputRel = ((Prel) topRel.getInput()).accept(this, null);

    final List<ConversionColumn> conversions = new ArrayList<>();
    final List<RexNode> bottomExprs = new ArrayList<>();

    /*
     Since we already rewrote so that json convert from was an edge expression, we can just check
     if any of the expressions are RexCall with the correct name.
    */
    for (int fieldId = 0; fieldId < expressions.size(); fieldId++) {
      final RexNode n = expressions.get(fieldId);
      if (n instanceof RexCall) {
        RexCall call = (RexCall) n;
        if (call.getOperator().getName().equalsIgnoreCase("convert_fromjson")) {
          List<RexNode> args = call.getOperands();
          Preconditions.checkArgument(args.size() == 1);
          RexNode input = args.get(0);
          String inputFieldName = topRel.getRowType().getFieldNames().get(fieldId);
          InputFieldVisitor visitor = new InputFieldVisitor(input, inputRel, inputFieldName);
          conversions.add(input.accept(visitor));
          bottomExprs.add(input);
          continue;
        }
      }
      bottomExprs.add(n);
    }

    if (conversions.isEmpty()) {
      // no changes, don't replace.
      return (Prel) topRel.copy(topRel.getTraitSet(), Collections.singletonList((inputRel)));
    }

    final List<RelDataType> bottomProjectType =
        Lists.transform(
            bottomExprs,
            new Function<RexNode, RelDataType>() {
              @Override
              public RelDataType apply(RexNode input) {
                return input.getType();
              }
            });

    final RelDataType bottomType =
        factory.createStructType(bottomProjectType, topRel.getRowType().getFieldNames());
    final ProjectPrel newBottomProject =
        ProjectPrel.create(
            topRel.getCluster(), topRel.getTraitSet(), inputRel, bottomExprs, bottomType);

    return new ConvertFromJsonPrel(
        topRel.getCluster(),
        topRel.getTraitSet(),
        topRel.getRowType(),
        newBottomProject,
        conversions);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();

    for (Prel p : prel) {
      children.add(p.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  private static UserException failed() {
    return UserException.validationError().message(FAILURE_MSG).build(logger);
  }

  private static CompleteType getRawSchema(
      QueryContext context, final List<String> tableSchemaPath, final String fieldname) {
    // originTable may come from materialization cache and may not contain up-to-date
    // datasetFieldsList info.
    // Get datasetFieldsList from query context catalog instead.
    return Optional.ofNullable(context)
        .map(QueryContext::getCatalogService)
        .map(CatalogService::getSystemUserCatalog)
        .map(v -> v.getTable(new NamespaceKey(tableSchemaPath)))
        .map(DremioTable::getDatasetConfig)
        .map(DatasetConfig::getDatasetFieldsList)
        // do we have a known schema for the converted field ?
        .map(v -> Iterables.find(v, input -> fieldname.equals(input.getFieldName()), null))
        // yes we do
        .map(v -> CompleteType.deserialize(v.getFieldSchema().toByteArray()))
        // (no we don't) return a dummy type for now
        .orElse(new CompleteType(ArrowType.Struct.INSTANCE, Collections.<Field>emptyList()));
  }

  private static CompleteType getLiteralSchema(QueryContext context, byte[] bytes) {
    try (BufferAllocator allocator =
            context
                .getAllocator()
                .newChildAllocator("convert-from-json-sampling", 0, Long.MAX_VALUE);
        BufferManager bufferManager = new BufferManagerImpl(allocator);
        ArrowBuf data = allocator.buffer(bytes.length);
        VectorContainer container = new VectorContainer(allocator);
        VectorAccessibleComplexWriter vc = new VectorAccessibleComplexWriter(container)) {

      data.writeBytes(bytes);
      final int sizeLimit =
          Math.toIntExact(context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
      final int maxLeafLimit =
          Math.toIntExact(context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
      final int rowSizeLimit =
          Math.toIntExact(context.getOptions().getOption(ExecConstants.LIMIT_ROW_SIZE_BYTES));
      final boolean rowSizeLimitEnabled =
          context.getOptions().getOption(ExecConstants.ENABLE_ROW_SIZE_LIMIT_ENFORCEMENT);
      JsonReader jsonReader =
          new JsonReader(
              bufferManager.getManagedBuffer(),
              sizeLimit,
              rowSizeLimitEnabled,
              rowSizeLimit,
              maxLeafLimit,
              context.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR),
              false,
              false,
              context
                  .getOptions()
                  .getOption(PlannerSettings.ENFORCE_VALID_JSON_DATE_FORMAT_ENABLED));
      jsonReader.setSource(bytes);

      ComplexWriter writer = new ComplexWriterImpl("dummy", vc);
      writer.setPosition(0);
      ReadState state = jsonReader.write(writer);
      if (state == ReadState.END_OF_STREAM) {
        throw new EOFException("Unexpected arrival at end of JSON literal stream");
      }

      container.buildSchema();
      return CompleteType.fromField(container.getSchema().getFields().get(0));
    } catch (Exception ex) {
      throw UserException.validationError(ex)
          .message("Failure while trying to parse JSON literal.")
          .build(logger);
    }
  }

  private class InputFieldVisitor implements RexVisitor<ConversionColumn> {

    private final RexNode rexRoot;
    private final RelNode inputRel;
    private final String inputFieldName;

    public InputFieldVisitor(RexNode rexRoot, RelNode inputRel, String inputFieldName) {
      this.rexRoot = rexRoot;
      this.inputRel = inputRel;
      this.inputFieldName = inputFieldName;
    }

    @Override
    public ConversionColumn visitInputRef(RexInputRef rexInputRef) {
      Set<RelColumnOrigin> origins = query.getColumnOrigins(inputRel, rexInputRef.getIndex());
      if (origins == null || origins.size() != 1 || origins.iterator().next().isDerived()) {
        throw failed();
      }

      RelColumnOrigin origin = origins.iterator().next();
      RelOptTable originTable = origin.getOriginTable();

      List<String> tablePath = originTable.getQualifiedName();
      String fieldName =
          originTable.getRowType().getFieldNames().get(origin.getOriginColumnOrdinal());
      // only retrieve the discovered type info if the node we're visiting is the root of the
      // expression
      CompleteType type =
          rexInputRef == rexRoot ? getRawSchema(context, tablePath, fieldName) : null;
      return new ConversionColumn(OriginType.RAW, tablePath, fieldName, inputFieldName, type);
    }

    @Override
    public ConversionColumn visitFieldAccess(RexFieldAccess rexFieldAccess) {
      RexNode refExpr = rexFieldAccess.getReferenceExpr();
      if (!((refExpr instanceof RexFieldAccess) || (refExpr instanceof RexInputRef))) {
        throw failed();
      }

      ConversionColumn conversionColumn = refExpr.accept(this);
      String fieldName =
          conversionColumn.getOriginField() + "." + rexFieldAccess.getField().getName();
      // only retrieve the discovered type info if the node we're visiting is the root of the
      // expression
      CompleteType type =
          rexFieldAccess == rexRoot
              ? getRawSchema(context, conversionColumn.getOriginTable(), fieldName)
              : null;
      return new ConversionColumn(
          OriginType.RAW,
          conversionColumn.getOriginTable(),
          fieldName,
          conversionColumn.getInputField(),
          type);
    }

    @Override
    public ConversionColumn visitLocalRef(RexLocalRef rexLocalRef) {
      throw failed();
    }

    @Override
    public ConversionColumn visitLiteral(RexLiteral rexLiteral) {
      byte[] literalValue;
      SqlTypeFamily family = rexLiteral.getTypeName().getFamily();
      if (family.equals(SqlTypeFamily.CHARACTER)) {
        literalValue = ((String) rexLiteral.getValue2()).getBytes(StandardCharsets.UTF_8);
      } else if (family.equals(SqlTypeFamily.BINARY)) {
        literalValue = ((byte[]) rexLiteral.getValue2());
      } else {
        throw failed();
      }

      return new ConversionColumn(
          OriginType.LITERAL, null, null, inputFieldName, getLiteralSchema(context, literalValue));
    }

    @Override
    public ConversionColumn visitCall(RexCall rexCall) {
      throw failed();
    }

    @Override
    public ConversionColumn visitOver(RexOver rexOver) {
      throw failed();
    }

    @Override
    public ConversionColumn visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
      throw failed();
    }

    @Override
    public ConversionColumn visitDynamicParam(RexDynamicParam rexDynamicParam) {
      throw failed();
    }

    @Override
    public ConversionColumn visitRangeRef(RexRangeRef rexRangeRef) {
      throw failed();
    }

    @Override
    public ConversionColumn visitSubQuery(RexSubQuery rexSubQuery) {
      throw failed();
    }

    @Override
    public ConversionColumn visitTableInputRef(RexTableInputRef rexTableInputRef) {
      throw failed();
    }

    @Override
    public ConversionColumn visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
      throw failed();
    }
  }
}
