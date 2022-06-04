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
package com.dremio.exec.planner.physical;

import java.util.Optional;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileSystemCreateTableEntry;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPrel;
import com.dremio.io.file.Path;
import com.google.common.collect.ImmutableList;

public class WriterPrule extends Prule {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlannerSettings.class);

  public static final RelOptRule INSTANCE = new WriterPrule();

  public WriterPrule() {
    super(RelOptHelper.some(WriterRel.class, Rel.LOGICAL, RelOptHelper.any(RelNode.class)),
        "Prel.WriterPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final WriterRel writer = call.rel(0);
    final RelNode input = call.rel(1);

    final RelTraitSet requestedTraits = writer.getCreateTableEntry()
        .getOptions()
        .inferTraits(input.getTraitSet(), input.getRowType());
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
    final boolean isSingleWriter = writer.getCreateTableEntry().getOptions().isSingleWriter();
    final RelTraitSet traits = writer.getTraitSet()
      .plus(DistributionTrait.SINGLETON)
      .plus(Prel.PHYSICAL);

    // Create the Writer with the child's distribution because the degree of parallelism for the writer
    // should correspond to the number of child minor fragments. The Writer itself is not concerned with
    // the collation of the child.  Note that the Writer's output RowType consists of
    // {fragment_id varchar(255), number_of_records_written bigint} which are very different from the
    // child's output RowType.
    final WriterPrel child = new WriterPrel(writer.getCluster(),
      writer.getTraitSet()
        .plus(isSingleWriter ? DistributionTrait.SINGLETON : childDist)
        .plus(Prel.PHYSICAL),
      isSingleWriter ? convert(rel, traits) : rel, writer.getCreateTableEntry(), writer.getExpectedInboundRowType());

    if (!(child.getCreateTableEntry() instanceof FileSystemCreateTableEntry)) {
      // we can only rename using file system
      return child;
    }

    final FileSystemCreateTableEntry fileEntry = (FileSystemCreateTableEntry) child.getCreateTableEntry();

    // first, resolve our children.
    final String finalPath = fileEntry.getLocation();
    final String userName = fileEntry.getUserName();
    final Path finalStructuredPath = Path.of(finalPath);
    final FileSystemPlugin<?> plugin = fileEntry.getPlugin();

    if (PrelUtil.getPlannerSettings(rel.getCluster()).options.getOption(PlannerSettings.WRITER_TEMP_FILE)) {

      final String tempPath = finalStructuredPath.getParent().resolve("." + finalStructuredPath.getName()).toString() + "-" + System.currentTimeMillis();
      FileSystemCreateTableEntry createTableEntryWithTempPath = ((FileSystemCreateTableEntry) child.getCreateTableEntry()).cloneWithNewLocation(tempPath);
      final WriterPrel childWithTempPath = new WriterPrel(child.getCluster(),
        child.getTraitSet(),
        isSingleWriter ? convert(rel, traits) : rel,
        createTableEntryWithTempPath,
        writer.getExpectedInboundRowType()
      );
      final RelNode newChild = getManifestWriterPrelIfNeeded(childWithTempPath, traits, writer, fileEntry, plugin, childDist);
      return new WriterCommitterPrel(writer.getCluster(),
        traits, newChild, plugin, tempPath, finalPath, userName, fileEntry, Optional.empty(), false, false);
    } else {
      final RelNode newChild = getManifestWriterPrelIfNeeded(child, traits, writer, fileEntry, plugin, childDist);
      return new WriterCommitterPrel(writer.getCluster(),
        traits, newChild, plugin, null, finalPath, userName, fileEntry, Optional.empty(), false, false);
    }
  }

  /***
   * This function is to get Iceberg Ctas Flow with ManifestWriter. This writer is responsible to consume data files output from ParquetWriter
   * create ManifestFile out of it and then send to Writter Committer Operator which will commit to iceberg table.
   */
  public static RelNode getManifestWriterPrelIfNeeded(RelNode child, RelTraitSet oldTraits, WriterRel writer, FileSystemCreateTableEntry fileEntry, FileSystemPlugin<?> plugin,
                                                      DistributionTrait childDist) {
    if(fileEntry.getIcebergTableProps() == null) {
      return convert(child, oldTraits);
    } else {
      DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(RecordWriter.SCHEMA.getFields().indexOf(RecordWriter.ICEBERG_METADATA));
      DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
      final RelTraitSet newTraits = writer.getTraitSet()
        .plus(distributionTrait)
        .plus(Prel.PHYSICAL);

      final RelNode newChild = new HashToRandomExchangePrel(
        child.getCluster(),
        newTraits,
        child,
        distributionTrait.getFields(),
        HashPrelUtil.DATA_FILE_DISTRIBUTE_HASH_FUNCTION_NAME,
        null);

      CreateTableEntry icebergCreateTableEntry = getCreateTableEntryForManifestWriter(fileEntry, plugin, fileEntry.getIcebergTableProps().getFullSchema(), fileEntry.getIcebergTableProps());
      final WriterPrel manifestWriterPrel = new IcebergManifestWriterPrel(writer.getCluster(),
        writer.getTraitSet()
          .plus(childDist)
          .plus(Prel.PHYSICAL),
        newChild, icebergCreateTableEntry);
      return convert(manifestWriterPrel, oldTraits);
    }
  }

  public static CreateTableEntry getCreateTableEntryForManifestWriter(FileSystemCreateTableEntry fileEntry, FileSystemPlugin<?> plugin, BatchSchema writeTableSchema, IcebergTableProps icebergTableProps) {
    WriterOptions oldOptions = fileEntry.getOptions();
    WriterOptions manifestWriterOption = new WriterOptions(null, null, null, null,
      null, false, oldOptions.getRecordLimit(), null, oldOptions.getExtendedProperty(), icebergTableProps, false);
    // IcebergTableProps is the only obj we need in manifestWriter
    return fileEntry.cloneWithFields(plugin.getFormatPlugin("iceberg"), manifestWriterOption);
  }
}
