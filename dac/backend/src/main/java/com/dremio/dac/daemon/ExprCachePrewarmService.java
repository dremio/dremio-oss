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
package com.dremio.dac.daemon;

import com.carrotsearch.hppc.IntHashSet;
import com.dremio.common.VM;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.project.Projector;
import com.dremio.service.Service;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExprCachePrewarmService prewarms the compiled code cache on executor startup from serialized
 * expressions stored in files in a configured directory
 */
public class ExprCachePrewarmService implements Service {
  private static final Logger logger = LoggerFactory.getLogger(ExprCachePrewarmService.class);

  private ExecutorService executorService;
  private final BufferAllocator allocator;
  private final Provider<OptionManager> optionManagerProvider;
  private final Provider<SabotContext> sabotContextProvider;

  private AtomicInteger numCached = new AtomicInteger(0);
  private List<Future<?>> projectSetupFutures = new ArrayList<>();

  public ExprCachePrewarmService(
      Provider<SabotContext> sabotContextProvider,
      Provider<OptionManager> optionManagerProvider,
      BufferAllocator allocator) {
    this.sabotContextProvider = sabotContextProvider;
    this.optionManagerProvider = optionManagerProvider;
    this.allocator = allocator;
  }

  @Override
  public void start() throws Exception {
    String prewarmCache = System.getProperty(ExecConstants.CODE_CACHE_PREWARM_PROP);
    if (prewarmCache == null || !"true".equals(prewarmCache.toLowerCase())) {
      return;
    }
    String location = System.getProperty(ExecConstants.CODE_CACHE_LOCATION_PROP);
    if (location == null || location.isEmpty()) {
      logger.warn("Location for cached expressions for prewarming is not set");
      return;
    }
    Path loc = Paths.get(location);
    if (!Files.exists(loc) || !Files.isDirectory(loc)) {
      logger.warn("Location for cached expressions is not a valid directory");
      return;
    }
    executorService = Executors.newFixedThreadPool(VM.availableProcessors());
    AtomicInteger numProjects = new AtomicInteger(0);
    try (Stream<Path> paths = Files.walk(Paths.get(location))) {
      paths
          .filter(Files::isRegularFile)
          .forEach(
              f -> {
                projectSetupFutures.add(executorService.submit(() -> buildProjector(f)));
                numProjects.addAndGet(1);
              });
    }
    logger.info(
        "Trying to build project expressions from {} files to prewarm the cache",
        numProjects.get());
  }

  @VisibleForTesting
  protected List<Future<?>> getProjectSetupFutures() {
    return projectSetupFutures;
  }

  @VisibleForTesting
  protected int numCached() {
    return numCached.get();
  }

  private void buildProjector(Path path) {
    try {
      RandomAccessFile reader = new RandomAccessFile(path.toString(), "r");
      FileChannel channel = reader.getChannel();

      int size = (int) channel.size();
      ByteBuffer byteBuffer = ByteBuffer.allocate(size);
      channel.read(byteBuffer);
      byteBuffer.flip();

      /*
       *  Each file contains a BatchSchema and a list of NamedExpressions corresponding to a project operation
       *   serialized in json format as bytes.
       *  File Format:
       *  4 bytes : SchemaLen
       *  {SchemaLen} bytes : Serialized BatchSchema
       *  4 bytes : ExprsLen
       *  {ExprsLem} bytes : Serialized list of NamedExpressions
       */
      if (size < Integer.BYTES) {
        logger.warn("Invalid file {}. Failed to read the schema length", path.toString());
        return;
      }
      int schemaLen = byteBuffer.getInt();
      size -= Integer.BYTES;

      if (size < schemaLen) {
        logger.warn("Invalid file {}. Failed to read the schema bytes", path.toString());
        return;
      }
      byte[] schemaBytes = new byte[schemaLen];
      byteBuffer.get(schemaBytes);
      size -= schemaLen;

      if (size < Integer.BYTES) {
        logger.warn("Invalid file {}. Failed to read the exprs length", path.toString());
        return;
      }
      int exprsLen = byteBuffer.getInt();
      size -= Integer.BYTES;

      if (size != exprsLen) {
        logger.warn("Invalid file {}. Failed to read the exprs", path.toString());
        return;
      }
      byte[] exprsBytes = new byte[exprsLen];
      byteBuffer.get(exprsBytes);

      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.registerModule(
          new SimpleModule().addDeserializer(LogicalExpression.class, new LogicalExpression.De()));

      BatchSchema schema = objectMapper.readValue(schemaBytes, BatchSchema.class);
      List<NamedExpression> exprs =
          objectMapper.readValue(exprsBytes, new TypeReference<List<NamedExpression>>() {});
      Preconditions.checkArgument(exprs != null);
      Preconditions.checkArgument(schema != null);

      try (BufferAllocator childAllocator =
              allocator.newChildAllocator("prewarm-cache-" + path.toString(), 0, Long.MAX_VALUE);
          VectorContainer incoming = new VectorContainer(childAllocator);
          VectorContainer output = new VectorContainer(childAllocator);
          OperatorContextImpl context = getContext(childAllocator)) {
        incoming.addSchema(schema);
        incoming.buildSchema();
        //  build the projector
        final ClassGenerator<Projector> cg =
            context.getClassProducer().createGenerator(Projector.TEMPLATE_DEFINITION).getRoot();
        final IntHashSet transferFieldIds = new IntHashSet();
        final List<TransferPair> transfers = Lists.newArrayList();

        Stopwatch javaCodeGenWatch = Stopwatch.createUnstarted();
        Stopwatch gandivaCodeGenWatch = Stopwatch.createUnstarted();
        try (ExpressionSplitter splitter =
            ProjectOperator.createSplitterWithExpressions(
                incoming,
                exprs,
                transfers,
                cg,
                transferFieldIds,
                context,
                new ExpressionEvaluationOptions(context.getOptions()),
                output,
                null)) {
          splitter.setupProjector(output, javaCodeGenWatch, gandivaCodeGenWatch);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        javaCodeGenWatch.start();
        Projector projector = cg.getCodeGenerator().getImplementationClass();
        projector.setup(context.getFunctionContext(), incoming, output, transfers, (name) -> null);
        javaCodeGenWatch.stop();

        logger.info("Successfully built the project expressions from file {}", path.toString());
        numCached.addAndGet(1);
      }
    } catch (Exception ex) {
      logger.warn("Failed to read the file {} to prewarm the cache", path.toString(), ex);
    }
  }

  private OperatorContextImpl getContext(BufferAllocator allocator) {
    // creating a dummy operatorcontext with only members required in building projector
    OptionManager optionManager = optionManagerProvider.get();
    SabotContext sabotContext = sabotContextProvider.get();
    CodeCompiler compiler = sabotContext.getCompiler();
    FunctionLookupContext functionLookupContext =
        optionManager.getOption(PlannerSettings.ENABLE_DECIMAL_V2)
            ? sabotContext.getDecimalFunctionImplementationRegistry()
            : sabotContext.getFunctionImplementationRegistry();
    OperatorStats stats = new OperatorStats(new OpProfileDef(0, 0, 0), allocator);
    return new OperatorContextImpl(
        null,
        null,
        null,
        null,
        allocator,
        allocator,
        compiler,
        stats,
        null,
        null,
        null,
        functionLookupContext,
        null,
        optionManager,
        null,
        null,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        sabotContext.getExpressionSplitCache(),
        null);
  }

  @Override
  public void close() throws Exception {
    if (executorService != null) {
      executorService.shutdown();
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
  }
}
