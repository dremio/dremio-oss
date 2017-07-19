/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic.planning.rels;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.calcite.logical.SampleCrel;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.SinglePrel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.handlers.PrelFinalizable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.plugins.elastic.planning.rules.StackFinder;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.StoragePluginId;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * A boundary Prel. Is used by Elastic rules to ensure that only the right set
 * of operations are pushed down into Elastic. Maintains state of what is pushed down.
 * All Elastic operations are pushed down using an operand of
 * <PrelToFind><ElasticsearchIntermediatePrel>. This ensures that no matching/changes
 * are done below the intermediate prel.
 *
 * This prel exists until we complete planning and is then finalized into a
 * ElasticFinalPrel which is a leaf prel for parallelization and execution purposes
 */
public class ElasticsearchIntermediatePrel extends SinglePrel implements PrelFinalizable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticsearchIntermediatePrel.class);

  private final Map<Class<? extends ElasticsearchPrel>, ElasticsearchPrel> nodes;
  private final StoragePluginId pluginId;
  private final FunctionLookupContext functionLookupContext;
  private final boolean hasTerminalPrel;

  public ElasticsearchIntermediatePrel(
      RelTraitSet traitSet,
      RelNode input,
      FunctionLookupContext functionLookupContext){
    super(input.getCluster(), traitSet, input);
    final Pointer<Boolean> hasTerminalPrel = new Pointer<Boolean>(false);
    List<ElasticsearchPrel> stack = StackFinder.getStack(input);
    nodes = FluentIterable.from(stack).uniqueIndex(new Function<ElasticsearchPrel, Class<? extends ElasticsearchPrel>>(){
      @Override
      public Class<? extends ElasticsearchPrel> apply(ElasticsearchPrel input) {
        if(ElasticTerminalPrel.class.isAssignableFrom(input.getClass())) {
          hasTerminalPrel.value = true;
        }
        return input.getClass();
      }});
    this.input = input;
    this.pluginId = stack.get(0).getPluginId();
    this.functionLookupContext = functionLookupContext;
    this.hasTerminalPrel = hasTerminalPrel.value;
  }

  public StoragePluginId getPluginId(){
    return pluginId;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    throw new UnsupportedOperationException("Must be finalized before retrieving physical operator.");
  }

  public boolean hasTerminalPrel() {
    return hasTerminalPrel;
  }

  public boolean contains(Class<? extends ElasticsearchPrel> clazz){
    return nodes.containsKey(clazz);
  }

  public <T extends ElasticsearchPrel> T get(Class<T> clazz){
    return Preconditions.checkNotNull(getNoCheck(clazz), "Unable to find %s in stack.", clazz.getName());
  }

  @SuppressWarnings("unchecked")
  public <T extends ElasticsearchPrel> T getNoCheck(Class<T> clazz){
    return (T) nodes.get(clazz);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    checkArgument(inputs.size() == 1, "must have one input: %s", inputs);
    return new ElasticsearchIntermediatePrel(traitSet, inputs.get(0), functionLookupContext);
  }

  public ElasticsearchIntermediatePrel withNewInput(ElasticsearchPrel input) {
    return new ElasticsearchIntermediatePrel(input.getTraitSet(), input, functionLookupContext);
  }

  public ElasticsearchIntermediatePrel filter(final Predicate<RelNode> predicate){
    return withNewInput((ElasticsearchPrel) getInput().accept(new MoreRelOptUtil.SubsetRemover()).accept(new MoreRelOptUtil.NodeRemover(predicate)));
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return copy(getTraitSet(), getInputs());
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    throw new UnsupportedOperationException("This needs to be finalized before using a PrelVisitor.");
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    int minDepth = nodes.size();
    if (minDepth == 0) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    /* Intermediate Prel doesn't really have any cost associated to it, but its cost is inversely proportional to the number of children it has
     * i.e. more pushdown the better.
     */
    return planner.getCostFactory().makeCost(Integer.MAX_VALUE, Integer.MAX_VALUE, 0).multiplyBy(1.0/minDepth);
  }

  /**
   * Finalize the stack. We'll do the following:
   *
   * - Push limit and sample below project (if either exists)
   * - Pop a project out of the stack if it is an edge project.
   * - Add an external Limit to the lesser of sample or limit.
   * - Add the popped project as a ProjectPrel outside the pushdown.
   */
  @Override
  public Prel finalizeRel() {
    List<ElasticsearchPrel> stack = StackFinder.getStack(input);

    if(stack.get(0) instanceof ElasticsearchSample || stack.get(0) instanceof ElasticsearchLimit){
      // push limit and sample below project.
      if(stack.get(1) instanceof ElasticsearchProject){
        List<ElasticsearchPrel> bottom = stack.subList(2, stack.size());
        ElasticsearchPrel sampleOrLimit = (ElasticsearchPrel) stack.get(0).copy(traitSet, ImmutableList.<RelNode>of(bottom.get(0)));
        ElasticsearchPrel project = (ElasticsearchPrel) stack.get(1).copy(sampleOrLimit.getTraitSet(), ImmutableList.<RelNode>of(sampleOrLimit));
        List<ElasticsearchPrel> top = ImmutableList.of(project, sampleOrLimit);
        stack = ImmutableList.copyOf(Iterables.concat(top, bottom));
      }

    }

    // Pop an edge project (pushing down projections is typically slower than executing in Dremio).
    ElasticsearchProject poppedProject = null;
    if(stack.get(0) instanceof ElasticsearchProject){
      poppedProject = (ElasticsearchProject) stack.get(0);
      stack = stack.subList(1, stack.size());
    }

    // build the elastic query
    final ScanBuilder builder = stack.get(0).newScanBuilder();
    builder.setup(stack, functionLookupContext);

    // create a final prel.
    final Prel finalPrel = new ElasticScanPrel(
        getCluster(),
        getTraitSet(),
        stack.get(0),
        builder,
        functionLookupContext);


    // apply outside limit as necessary (could be duplicative)
    Prel output = finalPrel;
    final ElasticsearchSample sample = getNoCheck(ElasticsearchSample.class);
    final ElasticsearchLimit limit = getNoCheck(ElasticsearchLimit.class);
    if(limit != null || sample != null){
      Long fetch = null;
      if(limit != null){
        fetch = (long) limit.getFetchSize();
      }

      if(sample != null){
        long sampleFetch = SampleCrel.getSampleSizeAndSetMinSampleSize(PrelUtil.getPlannerSettings(getCluster().getPlanner()), ElasticsearchSample.SAMPLE_SIZE_DENOMINATOR);
        fetch = fetch == null ? sampleFetch : Math.min(fetch,  sampleFetch);
      }

      output = PrelUtil.addLimitPrel(output, fetch);

    }

    // if project was popped, apply outside limit.
    if(poppedProject != null){
      output = new ProjectPrel(output.getCluster(), poppedProject.getTraitSet(), output, poppedProject.getChildExps(), poppedProject.getRowType());
    }

    return output;
  }

}
