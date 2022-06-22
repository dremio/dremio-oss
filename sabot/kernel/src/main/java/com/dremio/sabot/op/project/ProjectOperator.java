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
package com.dremio.sabot.op.project;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import com.carrotsearch.hppc.IntHashSet;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.fn.CastFunctions;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.expr.FunctionHolderExpr;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.fn.ComplexWriterFunctionHolder;
import com.dremio.exec.physical.config.ComplexToJson;
import com.dremio.exec.physical.config.Project;
import com.dremio.exec.proto.UserBitShared.OperatorProfileDetails;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorAccessibleComplexWriter;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.project.Projector.ComplexWriterCreator;
import com.dremio.sabot.op.project.ProjectorStats.Metric;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class ProjectOperator implements SingleInputOperator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectOperator.class);

  private final Project config;
  private final OperatorContext context;
  private final ExpressionEvaluationOptions projectorOptions;
  private final VectorContainer outgoing;

  private VectorAccessible incoming;
  private State state = State.NEEDS_SETUP;
  private Projector projector;
  private List<ValueVector> allocationVectors;
  private ExpressionSplitter splitter;
  private final List<ComplexWriter> complexWriters = new ArrayList<>();
  private int recordsConsumedCurrentBatch;
  private BatchSchema initialSchema;
  private Stopwatch javaCodeGenWatch = Stopwatch.createUnstarted();
  private Stopwatch gandivaCodeGenWatch = Stopwatch.createUnstarted();

  public static enum EvalMode {DIRECT, COMPLEX, EVAL};

  private static Set<ExpressionHashKey> exprHashSet = ConcurrentHashMap.newKeySet();

  public ProjectOperator(final OperatorContext context, final Project config) throws OutOfMemoryException {
    this.config = config;
    this.context = context;
    this.projectorOptions = new ExpressionEvaluationOptions(context.getOptions());
    this.projectorOptions.setCodeGenOption(context.getOptions().getOption(ExecConstants.QUERY_EXEC_OPTION.getOptionName()).getStringVal());
    this.outgoing = context.createOutputVectorContainer();
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) throws Exception {
    this.incoming = incoming;
    this.allocationVectors = Lists.newArrayList();
    final List<NamedExpression> exprs = getExpressionList();
    final List<TransferPair> transfers = new ArrayList<>();

    final ClassGenerator<Projector> cg = context.getClassProducer().createGenerator(Projector
      .TEMPLATE_DEFINITION).getRoot();

    final IntHashSet transferFieldIds = new IntHashSet();

    List<NamedExpression> nonDirectExprs = new ArrayList<>();
    splitter = createSplitterWithExpressions(incoming, exprs, transfers, cg, transferFieldIds,
      context, projectorOptions, outgoing, null, nonDirectExprs);

    outgoing.buildSchema(SelectionVectorMode.NONE);
    outgoing.setInitialCapacity(context.getTargetBatchSize());
    state = State.CAN_CONSUME;
    initialSchema = outgoing.getSchema();
    splitter.setupProjector(outgoing, javaCodeGenWatch, gandivaCodeGenWatch);
    javaCodeGenWatch.start();
    this.projector = cg.getCodeGenerator().getImplementationClass();
    projector.setup(
      context.getFunctionContext(),
      incoming,
      outgoing,
      transfers,
      new ComplexWriterCreator(){
        @Override
        public ComplexWriter addComplexWriter(String name) {
          VectorAccessibleComplexWriter vc = new VectorAccessibleComplexWriter(outgoing);
          ComplexWriter writer = new ComplexWriterImpl(name, vc);
          complexWriters.add(writer);
          return writer;
        }
      }
    );
    javaCodeGenWatch.stop();
    OperatorStats stats = context.getStats();
    stats.addLongStat(Metric.JAVA_BUILD_TIME, javaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    stats.addLongStat(Metric.GANDIVA_BUILD_TIME, gandivaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    stats.addLongStat(Metric.GANDIVA_EXPRESSIONS, splitter.getNumExprsInGandiva());
    stats.addLongStat(Metric.JAVA_EXPRESSIONS, splitter.getNumExprsInJava());
    stats.addLongStat(Metric.MIXED_EXPRESSIONS, splitter.getNumExprsInBoth());
    stats.addLongStat(Metric.MIXED_SPLITS, splitter.getNumSplitsInBoth());
    stats.setProfileDetails(OperatorProfileDetails
      .newBuilder()
      .addAllSplitInfos(splitter.getSplitInfos())
      .build()
    );
    gandivaCodeGenWatch.reset();
    javaCodeGenWatch.reset();

    if (nonDirectExprs.size() > 0) {
      // caching all expressions since whether an valuevectorread expression is transferred or evaluated is also determined by
      // other direct transfer expressions (if it is already transfered the expression is evaluated)
      cacheExpressions(exprs);
    }
    return outgoing;
  }

  /*
   *  Each file contains a BatchSchema and a list of NamedExpressions corresponding to a project operation
   *   serialized in json format as bytes.
   *  File Format:
   *  4 bytes : SchemaLen
   *  {SchemaLen} bytes : Serialized BatchSchema
   *  4 bytes : ExprsLen
   *  {ExprsLem} bytes : Serialized list of NamedExpressions
   */
  private void cacheExpressions(List<NamedExpression> nonDirectExprs) {
    if (context.getOptions().getOption(ExecConstants.EXEC_CODE_CACHE_SAVE_EXPR)
      && !nonDirectExprs.isEmpty()
      && (context.getFragmentHandle().getMinorFragmentId() == 0)) { // only save in one minor fragment to avoid duplicate files
      String loc = System.getProperty(ExecConstants.CODE_CACHE_LOCATION_PROP);
      if (loc != null && !loc.isEmpty()) {
        ExpressionHashKey expressionHashKey = new ExpressionHashKey(nonDirectExprs, incoming.getSchema());
        if (exprHashSet.add(expressionHashKey)) {
          Path directory = Paths.get(loc);
          if (Files.isDirectory(directory)) {
            try {
              String fileName = QueryIdHelper.getQueryIdentifier(context.getFragmentHandle()) + ":" + this.config.getId();
              Path file = Paths.get(directory.toString(), fileName);
              ObjectMapper objectMapper = new ObjectMapper();
              byte[] schemaBytes = objectMapper.writeValueAsBytes(incoming.getSchema());
              byte[] exprsBytes = objectMapper.writeValueAsBytes(nonDirectExprs);
              RandomAccessFile stream = new RandomAccessFile(file.toString(), "rw");
              FileChannel channel = stream.getChannel();
              ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + schemaBytes.length + Integer.BYTES + exprsBytes.length);
              byteBuffer.putInt(schemaBytes.length);
              byteBuffer.put(schemaBytes);
              byteBuffer.putInt(exprsBytes.length);
              byteBuffer.put(exprsBytes);
              byteBuffer.flip();
              channel.write(byteBuffer);
              channel.close();
              stream.close();
            } catch (Exception ex) {
              logger.error("Failed to save the expression", ex);
            }
          } else {
            logger.warn("Provided expression save directory is not a directory");
          }
        }
      } else {
        logger.warn("Prewarm cache directory to save expression is not set");
      }
    }
  }


  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);
    recordsConsumedCurrentBatch = records;
    state = State.CAN_PRODUCE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);
    allocateNew();

    splitter.projectRecords(recordsConsumedCurrentBatch, javaCodeGenWatch, gandivaCodeGenWatch);
    javaCodeGenWatch.start();
    projector.projectRecords(recordsConsumedCurrentBatch);
    javaCodeGenWatch.stop();

    setValueCount(recordsConsumedCurrentBatch);
    outgoing.setRecordCount(recordsConsumedCurrentBatch);

    state = State.CAN_CONSUME;

    if(!outgoing.hasSchema()){
      outgoing.buildSchema(incoming.getSchema().getSelectionVectorMode());
      throw UserException.schemaChangeError()
        .message("Schema changed during projection. Schema was \n%s\n but then changed to \n%s", initialSchema, outgoing.getSchema())
        .build(logger);
    }

    return recordsConsumedCurrentBatch;
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    state = State.DONE;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(outgoing, splitter);
    context.getStats().addLongStat(Metric.JAVA_EVALUATE_TIME, javaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    context.getStats().addLongStat(Metric.GANDIVA_EVALUATE_TIME, gandivaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    javaCodeGenWatch.reset();
    gandivaCodeGenWatch.reset();
  }


  private void allocateNew() {
    //Allocate vv in the allocationVectors.
    for (final ValueVector v : this.allocationVectors) {
      AllocationHelper.allocateNew(v, incoming.getRecordCount());
    }

    for (final ComplexWriter writer : complexWriters) {
      writer.allocate();
    }

  }

  private void setValueCount(final int count) {
    for (final ValueVector v : allocationVectors) {
      v.setValueCount(count);
    }

    for (final ComplexWriter writer : complexWriters) {
      writer.setValueCount(count);
    }
  }

  private List<NamedExpression> getExpressionList() {
    if (config.getExprs() != null) {
      return config.getExprs();
    }

    // project also supports the ComplexToJson operation. If we get here, we're in that state.
    final List<NamedExpression> exprs = Lists.newArrayList();
    for (final Field field : incoming.getSchema()) {
      CompleteType type = CompleteType.fromField(field);
      if (type.isComplex() || type.isUnion()) {
        final LogicalExpression convertToJson = FunctionCallFactory.createConvert(ConvertExpression.CONVERT_TO, "JSON", SchemaPath.getSimplePath(field.getName()));
        final String castFuncName = CastFunctions.getCastFunc(MinorType.VARCHAR);
        final List<LogicalExpression> castArgs = Lists.newArrayList();
        castArgs.add(convertToJson);  //input_expr
        /*
         * We are implicitly casting to VARCHAR so we don't have a max length,
         * using an arbitrary value. We trim down the size of the stored bytes
         * to the actual size so this size doesn't really matter.
         */
        castArgs.add(new ValueExpressions.LongExpression(TypeHelper.VARCHAR_DEFAULT_CAST_LEN)); //
        final FunctionCall castCall = new FunctionCall(castFuncName, castArgs);
        exprs.add(new NamedExpression(castCall, new FieldReference(field.getName())));
      } else {
        exprs.add(new NamedExpression(SchemaPath.getSimplePath(field.getName()), new FieldReference(field.getName())));
      }
    }
    return exprs;
  }

  public static EvalMode getEvalMode(VectorAccessible incoming, LogicalExpression expr, IntHashSet transferFieldIds){
    // add value vector to transfer if direct reference and this is allowed, otherwise, add to evaluation stack.
    final boolean canDirectTransfer =

      // the expression is a direct read.
      expr instanceof ValueVectorReadExpression

        // we aren't dealing with a selection vector.
        && incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE

        // the field doesn't have a red path (e.g. a single value out of a list)
        && !((ValueVectorReadExpression) expr).hasReadPath()

        // We aren't already transferring the field.
        && !transferFieldIds.contains(((ValueVectorReadExpression) expr).getFieldId().getFieldIds()[0]);

    if(canDirectTransfer){
      return EvalMode.DIRECT;
    }
    final boolean isComplex =
      expr instanceof FunctionHolderExpr
        && ((FunctionHolderExpr) expr).isComplexWriterFuncHolder();

    if(isComplex){
      return EvalMode.COMPLEX;
    }

    return EvalMode.EVAL;

  }

  public static class ProjectCreator implements SingleInputOperator.Creator<Project>{

    @Override
    public SingleInputOperator create(OperatorContext context, Project operator) throws ExecutionSetupException {
      return new ProjectOperator(context, operator);
    }

  }

  public static class ComplexToJsonCreator implements SingleInputOperator.Creator<ComplexToJson>{

    @Override
    public SingleInputOperator create(OperatorContext context, ComplexToJson operator) throws ExecutionSetupException {
      Project project = new Project(operator.getProps(), null, null);
      return new ProjectOperator(context, project);
    }

  }

  public static ExpressionSplitter createSplitterWithExpressions(VectorAccessible incoming,
                                                                 List<NamedExpression> exprs,
                                                                 List<TransferPair> transfers, ClassGenerator<Projector> cg,
                                                                 IntHashSet transferFieldIds, OperatorContext context,
                                                                 ExpressionEvaluationOptions options, VectorContainer outgoing,
                                                                 BatchSchema targetSchema) throws Exception {
    return createSplitterWithExpressions(incoming, exprs, transfers, cg, transferFieldIds, context, options, outgoing, targetSchema, null);
  }

    public static ExpressionSplitter createSplitterWithExpressions(VectorAccessible incoming,
                                                                 List<NamedExpression> exprs,
                                                                 List<TransferPair> transfers, ClassGenerator<Projector> cg,
                                                                 IntHashSet transferFieldIds, OperatorContext context,
                                                                 ExpressionEvaluationOptions options, VectorContainer outgoing,
                                                                 BatchSchema targetSchema, List<NamedExpression> nonDirectExprs) throws Exception {
    ExpressionSplitter splitter = new ExpressionSplitter(context, incoming,
            options, context.getClassProducer().getFunctionLookupContext().isDecimalV2Enabled());

    for (int i = 0; i < exprs.size(); i++) {
      final NamedExpression namedExpression = exprs.get(i);
      if (namedExpression == null) {
        continue;
      }
      // it is possible that a filter removed all output or the shard has no data, so we don't have any incoming vectors
      // applies only for coercion readers
      if (targetSchema != null && incoming.getValueVectorId(SchemaPath.getSimplePath(targetSchema
              .getFields().get(i).getName())) == null) {
        continue;
      }

      final LogicalExpression materializedExp = context.getClassProducer().materializeAndAllowComplex(namedExpression.getExpr(), incoming, true);
      switch (ProjectOperator.getEvalMode(incoming, materializedExp, transferFieldIds)) {

        case COMPLEX: {

          outgoing.addOrGet(materializedExp.getCompleteType().toField(namedExpression.getRef()));
          // The reference name will be passed to ComplexWriter, used as the name of the output vector from the writer.
          ((ComplexWriterFunctionHolder) ((FunctionHolderExpr) materializedExp).getHolder()).setReference(namedExpression.getRef());
            if (context.getOptions().getOption(ExecConstants.EXPRESSION_CODE_CACHE_ENABLED)) {
            cg.lazyAddExp(materializedExp, ClassGenerator.BlockCreateMode.NEW_IF_TOO_LARGE, true);
          } else {
            cg.addExpr(materializedExp, ClassGenerator.BlockCreateMode.NEW_IF_TOO_LARGE, true);
          }
          if (nonDirectExprs != null) {
            nonDirectExprs.add(namedExpression);
          }
          break;
        }

        case DIRECT: {
          final ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) materializedExp;
          final TypedFieldId id = vectorRead.getFieldId();
          final ValueVector vvIn = incoming.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();
          final FieldReference ref = namedExpression.getRef();
          final ValueVector vvOut = outgoing.addOrGet(vectorRead.getCompleteType().toField(ref));
          final TransferPair tp = vvIn.makeTransferPair(vvOut);
          transfers.add(tp);
          transferFieldIds.add(vectorRead.getFieldId().getFieldIds()[0]);
          break;
        }

        case EVAL: {
          splitter.addExpr(outgoing, new NamedExpression(materializedExp, namedExpression.getRef()));
          if (nonDirectExprs != null) {
            nonDirectExprs.add(namedExpression);
          }
          break;
        }
        default:
          throw new UnsupportedOperationException();
      }

    }
    return splitter;
  }

  class ExpressionHashKey {
    HashCode hash;
    int javaHashCode;

    ExpressionHashKey(List<NamedExpression> namedExpressionList, BatchSchema schema) {
      HashFunction hf = Hashing.murmur3_128();
      Hasher hasher = hf.newHasher();
      namedExpressionList.forEach(e -> {
        hasher.putUnencodedChars(e.toString());
      });
      hasher.putUnencodedChars(schema.toString());
      hash = hasher.hash();
      javaHashCode = hash.hashCode();
    }

    @Override
    public int hashCode() {
      return javaHashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ExpressionHashKey)) {
        return false;
      }
      return hash.equals(((ExpressionHashKey) obj).hash);
    }
  }
}
