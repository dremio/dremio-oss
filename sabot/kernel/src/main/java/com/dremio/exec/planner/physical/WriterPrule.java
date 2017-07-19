/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.exec.store.dfs.FileSystemCreateTableEntry;
import com.dremio.exec.store.dfs.FileSystemPlugin;

public class WriterPrule extends Prule{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlannerSettings.class);
  public static final RelOptRule INSTANCE = new WriterPrule();

  public WriterPrule() {
    super(RelOptHelper.some(WriterRel.class, Rel.LOGICAL, RelOptHelper.any(RelNode.class)),
        "Prel.WriterPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final WriterRel writer = call.rel(0);
    final RelNode input = call.rel(1);

    final WriterOptions options = writer.getCreateTableEntry().getOptions();

    final RelTraitSet requestedTraits;
    if(options.hasDistributions()) {
      final List<Integer> keys = WriterOptions.getFieldIndices(options.getDistributionColumns(), input);
      requestedTraits = input.getTraitSet().plus(Prel.PHYSICAL).plus(WriterOptions.getDistribution(keys));
    } else {
      if (options.hasPartitions()){
        if (options.isHashPartition() && writer.isSingleWriter()) {
          throw UserException.unsupportedError().message("Can't support single writer when hash partition is enabled.").build(logger);
        }
        requestedTraits = options.getTraitSetWithPartition(input.getTraitSet().plus(Prel.PHYSICAL), input);
      } else {
        requestedTraits = input.getTraitSet().plus(Prel.PHYSICAL);
      }
    }

    final RelNode convertedInput = convert(input, requestedTraits);

    if (!new WriteTraitPull(call).go(writer, convertedInput)) {
      call.transformTo(convertWriter(writer, convertedInput));
    }
  }


  private static class WriteTraitPull extends SubsetTransformer<WriterRel, RuntimeException> {

    public WriteTraitPull(RelOptRuleCall call) {
      super(call);
    }

    @Override
    public RelNode convertChild(WriterRel writer, RelNode rel) throws RuntimeException {
      return convertWriter(writer, rel);
    }
  }


  private static RelNode convertWriter(WriterRel writer, RelNode rel) {
    DistributionTrait childDist = rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);

    // Create the Writer with the child's distribution because the degree of parallelism for the writer
    // should correspond to the number of child minor fragments. The Writer itself is not concerned with
    // the collation of the child.  Note that the Writer's output RowType consists of
    // {fragment_id varchar(255), number_of_records_written bigint} which are very different from the
    // child's output RowType.
    final WriterPrel child = new WriterPrel(writer.getCluster(),
      writer.getTraitSet()
        .plus(writer.isSingleWriter() ? DistributionTrait.SINGLETON : childDist)
        .plus(Prel.PHYSICAL),
      rel, writer.getCreateTableEntry());

    if (!(child.getCreateTableEntry() instanceof FileSystemCreateTableEntry)) {
      // we can only rename using file system
      return child;
    }

    final FileSystemCreateTableEntry fileEntry = (FileSystemCreateTableEntry) child.getCreateTableEntry();

    // first, resolve our children.
    final String finalPath = fileEntry.getLocation();
    final String userName = fileEntry.getUserName();
    final Path finalStructuredPath = new Path(finalPath);

    final String tempPath = new Path(finalStructuredPath.getParent(), "." + finalStructuredPath.getName()).toString() + "-" + System.currentTimeMillis();
    final FileSystemPlugin plugin = fileEntry.getPlugin();

    final RelTraitSet traits = writer.getTraitSet()
      .plus(DistributionTrait.SINGLETON)
      .plus(Prel.PHYSICAL);

    final WriterPrel childWithTempPath = new WriterPrel(child.getCluster(),
      child.getTraitSet(),
      rel,
      ((FileSystemCreateTableEntry) child.getCreateTableEntry()).cloneWithNewLocation(tempPath)
    );

    final RelNode newChild = convert(childWithTempPath, traits);
    return new WriterCommitterPrel(writer.getCluster(), traits, newChild, plugin, tempPath, finalPath, userName);
  }
}
