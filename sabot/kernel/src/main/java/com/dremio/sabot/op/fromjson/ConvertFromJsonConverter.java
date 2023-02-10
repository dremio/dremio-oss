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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.visitor.BasePrelVisitor;
import com.dremio.exec.record.VectorAccessibleComplexWriter;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.easy.json.JsonProcessor.ReadState;
import com.dremio.exec.vector.complex.fn.JsonReader;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPOP.ConversionColumn;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPOP.OriginType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class ConvertFromJsonConverter extends BasePrelVisitor<Prel, Void, RuntimeException> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConvertFromJsonConverter.class);
  public static final String FAILURE_MSG =
    "Using CONVERT_FROM(*, 'JSON') is only supported against string literals and direct table references of types VARCHAR and VARBINARY.";

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

    /**
     * Since we already rewrote so that json convert from was an edge expression, we can just check if any of the epxressions are RexCall with the correct name.
     */
    for(int fieldId = 0; fieldId < expressions.size(); fieldId++){
      final RexNode n = expressions.get(fieldId);
      if(n instanceof RexCall){
        RexCall call = (RexCall) n;

        if(call.getOperator().getName().equalsIgnoreCase("convert_fromjson")){
          List<RexNode> args = call.getOperands();
          Preconditions.checkArgument(args.size() == 1);
          final RexNode input = args.get(0);

          if(input instanceof RexLiteral){
            RexLiteral literal = (RexLiteral) input;
            SqlTypeFamily family = literal.getTypeName().getFamily();
            final byte[] value;
            if(family.equals(SqlTypeFamily.CHARACTER)){
              value = ((String)literal.getValue2()).getBytes(StandardCharsets.UTF_8);
            } else if(family.equals(SqlTypeFamily.BINARY)){
              value = ((byte[]) literal.getValue2());
            } else {
              throw failed();
            }
            final String inputField = topRel.getRowType().getFieldNames().get(fieldId);
            conversions.add(new ConversionColumn(OriginType.LITERAL, null, null, inputField, getLiteralSchema(context, value)));
            bottomExprs.add(literal);
            continue;
          } else if (input instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) input;

            Set<RelColumnOrigin> origins = query.getColumnOrigins(inputRel, inputRef.getIndex());
            if (origins == null || origins.size() != 1 || origins.iterator().next().isDerived()) {
              throw failed();
            }

            final RelColumnOrigin origin = origins.iterator().next();
            final RelOptTable originTable = origin.getOriginTable();

            final List<String> tableSchemaPath = originTable.getQualifiedName();
            final String tableFieldname = originTable.getRowType().getFieldNames().get(origin.getOriginColumnOrdinal());
            // we are using topRel to construct the newBottomProject rowType, make sure ConvertFromJson refers to that
            final String inputFieldname = topRel.getRowType().getFieldNames().get(fieldId);
              conversions.add(new ConversionColumn(
                OriginType.RAW, tableSchemaPath, tableFieldname, inputFieldname,
                getRawSchema(tableSchemaPath, tableFieldname)));

            bottomExprs.add(inputRef);
            continue;
          } else {
            throw failed();
          }
        }
      }
      bottomExprs.add(n);
    }

    if(conversions.isEmpty()){
      // no changes, don't replace.
      return (Prel) topRel.copy(topRel.getTraitSet(), Collections.singletonList((inputRel)));
    }

    final List<RelDataType> bottomProjectType = Lists.transform(bottomExprs, new Function<RexNode, RelDataType>(){
      @Override
      public RelDataType apply(RexNode input) {
        return input.getType();
      }});

    final RelDataType bottomType = factory.createStructType(bottomProjectType, topRel.getRowType().getFieldNames());
    final ProjectPrel newBottomProject = ProjectPrel.create(topRel.getCluster(), topRel.getTraitSet(), inputRel, bottomExprs, bottomType);

    return new ConvertFromJsonPrel(topRel.getCluster(), topRel.getTraitSet(), topRel.getRowType(), newBottomProject, conversions);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();

    for(Prel p : prel) {
      children.add(p.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }



  private static UserException failed(){
    return UserException.validationError().message(FAILURE_MSG).build(logger);
  }

  private CompleteType getRawSchema(final List<String> tableSchemaPath, final String fieldname) {
    // originTable may come from materialization cache and may not contain up-to-date datasetFieldsList info.
    // Get datasetFieldsList from query context catalog instead.
    return Optional.ofNullable(context)
      .map(v -> v.getCatalogService())
      .map(v -> v.getCatalog(MetadataRequestOptions.of(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())))
      .map(v -> v.getTable(new NamespaceKey(tableSchemaPath)))
      .map(v -> v.getDatasetConfig())
      .map(v -> v.getDatasetFieldsList())
      // do we have a known schema for the converted field ?
      .map(v -> Iterables.find(v, input -> fieldname.equals(input.getFieldName()), null))
      // yes we do
      .map(v -> CompleteType.deserialize(v.getFieldSchema().toByteArray()))
      // (no we don't) return a dummy type for now
      .orElse(new CompleteType(ArrowType.Struct.INSTANCE, Collections.<Field>emptyList()));
  }

  private static CompleteType getLiteralSchema(QueryContext context, byte[] bytes) {
    try(
        BufferAllocator allocator = context.getAllocator().newChildAllocator("convert-from-json-sampling", 0, Long.MAX_VALUE);
        BufferManager bufferManager = new BufferManagerImpl(allocator);
        ArrowBuf data = allocator.buffer(bytes.length);
        VectorContainer container = new VectorContainer(allocator);
        VectorAccessibleComplexWriter vc = new VectorAccessibleComplexWriter(container)

        ){
      data.writeBytes(bytes);
      final int sizeLimit = Math.toIntExact(context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
      final int maxLeafLimit = Math.toIntExact(context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
      JsonReader jsonReader = new JsonReader(bufferManager.getManagedBuffer(), sizeLimit, maxLeafLimit,
        context.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR), false, false);
      jsonReader.setSource(bytes);

        ComplexWriter writer = new ComplexWriterImpl("dummy", vc);
        writer.setPosition(0);
        ReadState state = jsonReader.write(writer);
        if(state == ReadState.END_OF_STREAM){
          throw new EOFException("Unexpected arrival at end of JSON literal stream");
        }

      container.buildSchema();
      return CompleteType.fromField(container.getSchema().getFields().get(0));
    }catch(Exception ex){
      throw UserException.validationError(ex).message("Failure while trying to parse JSON literal.").build(logger);
    }
  }
}
