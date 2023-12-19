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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;
import static com.dremio.exec.store.iceberg.IcebergUtils.getColumnName;
import static com.dremio.service.namespace.DatasetHelper.isInternalIcebergTable;
import static com.dremio.service.namespace.dataset.proto.PartitionProtobuf.IcebergTransformType;
import static org.apache.iceberg.PartitionSpec.builderFor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.util.Pair;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.types.Type;

import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.base.Preconditions;

/**
 * Helper functions for Calculating the affected partitions due to update and delete operations
 * on the base dataset, when deciding if a reflection can be incrementally refreshed or not.
 * This class supports delete, recalculate and add whole partitions.
 * This workflow is used in when doing incremental refresh presence of Delete and Update Iceberg operations.
 * The main idea is that instead of doing a full refresh, we will figure out which partitions are affected
 * by DML operations since the last snapshot of the base dataset. Then we will delete the data only the affected
 * partitions and recalculate the data for only the affected partitions.
 */
public final class IncrementalReflectionByPartitionUtils {

  /**
   * This constructor is marked private so that it is never called
   * IncrementalReflectionByPartitionUtils is a utility class and  no instance is necessary.
   * Adding a private constructor avoids a checkstyle warning
   */
  private IncrementalReflectionByPartitionUtils() {
  }

  /**
   * Check if the reflection is compatible with the base dataset for incremental refresh by partition
   * If compatible, update the snapshotDiffContext with reflectionTargetPartitionSpec and baseDatasetTargetPartitionSpec
   * @param reflectionIcebergTable Iceberg table for the reflection
   * @param reflectionRelationalAlgebra RelNode, containing the relational algebra representing the reflection calculation
   * @param snapshotDiffContext context to use to store the reflectionTargetPartitionSpec and baseDatasetTargetPartitionSpec found
   * @param baseDatasetTableMetadata table metadata for the base dataset
   * @return true if the partitions between the reflection and the base dataset are compatible, false otherwise
   */
  public static boolean buildCompatiblePartitions(final Table reflectionIcebergTable,final RelNode reflectionRelationalAlgebra,
                                                  final SnapshotDiffContext snapshotDiffContext,final TableMetadata baseDatasetTableMetadata) {
    //build a map between the reflectionPartitionColumns and Transform used
    //if there is a transform used here, it comes from the reflection partition transforms
    Map<String, TransformInfo> reflectionPartitionColumn2TransformInfo
      = buildReflectionPartitionColumn2TransformInfoMap(reflectionIcebergTable.spec());

    if(reflectionPartitionColumn2TransformInfo.isEmpty()){
      //the reflection is not partitioned at all
      return false;
    }
    //For each reflection partition column calculate the finalized transform and the column it is based on
    //here we say finalized, because it is possible that there are 2 different transforms
    //for example the reflection might be a view using cast(time_stamp_col as date)
    //and on top of that there might be a month transform
    //In effect we have a month transform on top of day transform
    //the resolved transform would be month
    buildValidTransformationAndBaseColumns(reflectionRelationalAlgebra, reflectionPartitionColumn2TransformInfo);

    final IcebergMetadata baseDatasetIcebergMetadata = baseDatasetTableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata();
    if(baseDatasetIcebergMetadata == null || baseDatasetIcebergMetadata.getJsonSchema() == null) {
      return false;
    }
    final Schema baseDatasetSchema = deserializedJsonAsSchema(baseDatasetIcebergMetadata.getJsonSchema());
    final PartitionSpec baseDatasetPartitionSpec = getLatestPartitionSpec(baseDatasetIcebergMetadata,baseDatasetSchema);

    //eliminate any candidates that are incompatible with the base dataset current partition spec
    //if all candidates are eliminated we will fail the incremental refresh and fall back to full refresh
    reflectionPartitionColumn2TransformInfo = eliminateInvalidPartitionCandidates(
            reflectionPartitionColumn2TransformInfo, baseDatasetPartitionSpec);
    if(reflectionPartitionColumn2TransformInfo.isEmpty()){
      //no partition fields were found that could be used for incremental refresh
      return false;
    }
    //if we get to here we are doing the optimization, now we have to figure out what fields to keep
    //it is possible we found both month(col1) and day(col1) as valid transforms
    //here we will keep the lowest transform for each column
    reflectionPartitionColumn2TransformInfo = eliminateSimilarPartitionCandidates(
            reflectionPartitionColumn2TransformInfo);

    final Schema reflectionSchema = reflectionIcebergTable.schema();

    final PartitionSpec.Builder partitionSpecBuilderBaseDataset = builderFor(baseDatasetSchema);
    final PartitionSpec.Builder partitionSpecBuilderReflection = builderFor(reflectionSchema);

    //Build the actual partition spec for the reflection and the base dataset
    //to be used for determining partitions to update
    //The specs need to be different because the column names could be different
    //in the base dataset and the reflection when a virtual dataset is used
    buildPartitionSpecs(reflectionPartitionColumn2TransformInfo,partitionSpecBuilderBaseDataset,partitionSpecBuilderReflection);

    snapshotDiffContext.setReflectionTargetPartitionSpec(partitionSpecBuilderReflection.build());
    snapshotDiffContext.setBaseDatasetTargetPartitionSpec(partitionSpecBuilderBaseDataset.build());

    return true;
  }

  /**
   * Build a partition spec with one field as specified by the arguments
   * @param schema schema to base the partition spec on
   * @param columnName column to add partition on
   * @param transformType what transform it is
   * @param argument any arguments needed by the transform such as number of buckets
   * @return partition spec
   */
  public static PartitionSpec buildPartitionSpec(final Schema schema,
                     final String columnName,
                     final IcebergTransformType transformType,
                     final Integer argument){
    final PartitionSpec.Builder partitionSpecBuilder = builderFor(schema);
    addPartitionColumn(partitionSpecBuilder, columnName, transformType, argument);
    return partitionSpecBuilder.build();
  }

  /**
   * Builds a map between each reflectionPartitionColumnName and TransformInfo
   * The transform info contains the transform type, and args list for the reflection
   * However, the base column is empty at this point and is populated later
   * @param partitionSpec partition spec for the reflection
   * @return a map between each reflectionPartitionColumnName and TransformInfo
   */
  public static Map<String, TransformInfo> buildReflectionPartitionColumn2TransformInfoMap(final PartitionSpec partitionSpec) {
    final Map<String, TransformInfo> reflectionPartitionColumn2TransformInfo = new HashMap<>();
    final List<PartitionField> reflectionPartitionFields = partitionSpec.fields();
    for(final PartitionField partitionField: reflectionPartitionFields){
      final Optional<IcebergTransformType> transformType = safeGetTransformType(partitionField.transform().toString());
      //ignore invalid transforms such as VOID
      //ignore unsupported transforms such as BUCKET
      if(transformType.isPresent() && !transformType.get().equals(IcebergTransformType.BUCKET)) {
        reflectionPartitionColumn2TransformInfo.put(
          getColumnName(partitionField, partitionSpec.schema()),
          new TransformInfo(transformType.get(),null, extractArgFromOneArgTransform(partitionSpec, partitionField),
                            partitionSpec.schema().findField(partitionField.sourceId()).type()));
      }
    }
    return reflectionPartitionColumn2TransformInfo;
  }

  /**
   * Extracts a single Integer argument from the transform
   * If the transform does not have any arguments returns null
   * @param spec partition spec containing partitionField
   * @param partitionField to extract args from
   * @return Integer argument
   */
  public static Integer extractArgFromOneArgTransform(final PartitionSpec spec, final PartitionField partitionField){
    final CollectArgsPartitionSpecVisitor collectArgsPartitionSpecVisitor = new CollectArgsPartitionSpecVisitor();
    try {
      return PartitionSpecVisitor.visit(spec.schema(), partitionField, collectArgsPartitionSpecVisitor);
    } catch (final UnsupportedOperationException e) {
      return null;
    }
  }

  /**
   * Build two partition specs to use for Incremental refresh by partition
   * One is for the base dataset and the other one for the reflection
   * The specs need to be different for the base dataset and the reflection because the column names
   * could be different in the base dataset and the reflection when a virtual dataset is used
   *
   * @param reflectionPartitionColumn2TransformInfo target spec information
   * @param partitionSpecBuilderBaseDataset         partition spec for the base dataset
   * @param partitionSpecBuilderReflection          partition spec for the reflection
   */
  private static void buildPartitionSpecs(
          final Map<String, TransformInfo> reflectionPartitionColumn2TransformInfo,
          final PartitionSpec.Builder partitionSpecBuilderBaseDataset,
          final PartitionSpec.Builder partitionSpecBuilderReflection) {
    for (final Map.Entry<String, TransformInfo> entry :
            reflectionPartitionColumn2TransformInfo.entrySet()) {
      final String baseColumnName = entry.getValue().getBaseColumnName();
      final IcebergTransformType reflectionTransformType = entry.getValue().getTransformType();
      final String reflectionColumnName = entry.getKey();
      addPartitionColumn(partitionSpecBuilderBaseDataset, baseColumnName, reflectionTransformType, entry.getValue().getArgument());
      addPartitionColumn(partitionSpecBuilderReflection, reflectionColumnName, reflectionTransformType, entry.getValue().getArgument());
    }
  }

  /**
   * Return true if topTransformType can be applied on top of bottomTransformType
   * Example1: topTransformType = month, bottomTransformType = day, we will return true,
   * because the data transformed with day can be transformed with month,
   * and it is equivalent to transforming the original data with month
   * Example2: topTransformType = bucket, bottomTransformType = day, we will return false,
   * because bucket transform on base data and bucket transform on data transformed by day will have different results
   *
   * @param topTransformType    the top transform function
   * @param bottomTransformType the bottom transform function
   * @param dataType the data type of the column
   * @return true if topTransformType transform can be applied on top of bottomTransformType, false otherwise
   */
  public static boolean isTransformTypeSupportedBy(final IcebergTransformType topTransformType,
                                                   final IcebergTransformType bottomTransformType,
                                                   final Integer topTransformArgs,
                                                   final Integer bottomTransformArgs,
                                                   final Type dataType){
    if(topTransformType == null || bottomTransformType == null){
      return false;
    }
    switch (topTransformType) {
      case YEAR:
        return  IcebergTransformType.YEAR.equals(bottomTransformType)
              || IcebergTransformType.MONTH.equals(bottomTransformType)
              || IcebergTransformType.DAY.equals(bottomTransformType)
              || IcebergTransformType.HOUR.equals(bottomTransformType)
              || IcebergTransformType.IDENTITY.equals(bottomTransformType);
      case MONTH:
        return IcebergTransformType.MONTH.equals(bottomTransformType)
            || IcebergTransformType.DAY.equals(bottomTransformType)
            || IcebergTransformType.HOUR.equals(bottomTransformType)
            || IcebergTransformType.IDENTITY.equals(bottomTransformType);
      case DAY:
        return IcebergTransformType.DAY.equals(bottomTransformType)
              || IcebergTransformType.HOUR.equals(bottomTransformType)
              || IcebergTransformType.IDENTITY.equals(bottomTransformType);
      case HOUR:
        return IcebergTransformType.HOUR.equals(bottomTransformType)
              || IcebergTransformType.IDENTITY.equals(bottomTransformType);
      case TRUNCATE:
        return (IcebergTransformType.TRUNCATE.equals(bottomTransformType) && truncateTransformsCompatible(topTransformArgs, bottomTransformArgs, dataType))||
          IcebergTransformType.IDENTITY.equals(bottomTransformType);
      case IDENTITY:
        return IcebergTransformType.IDENTITY.equals(bottomTransformType);
      default:
        //we don't support BUCKET transform for this feature for now
        return false;
    }
  }

  /**
   * Determines if applying Top Truncate transform on top of Bottom Truncate Transform is
   * equivalent to just applying Top Truncate transform
   * @param topTransformArgs arguments for the top truncate transform
   * @param bottomTransformArgs arguments for the bottom truncate transform
   * @param type data type to apply the truncate on
   * @return true if applying Top Truncate transform on top of Bottom Truncate Transform is
   * equivalent to just applying Top Truncate transform, false otherwise
   */
  public static boolean truncateTransformsCompatible(final Integer topTransformArgs,
                                                     final Integer bottomTransformArgs,
                                                     final org.apache.iceberg.types.Type type){
  if( topTransformArgs == null
      || bottomTransformArgs == null
      || type == null){
    return false;
  }
  switch (type.typeId()){
    case STRING:
      //for string, it is OK if the reflection truncates to the same or fewer characters than the base dataset
      //example
      //truncate(3, "dremio") = dre
      //truncate(3, truncate(4, "dremio")) = dre
      return topTransformArgs <= bottomTransformArgs;
    case INTEGER:
    case LONG:
      //Example1:
      //For int and long we cannot use X<=Y
      //This is because truncate(X) is not the same as truncate(X , truncate (Y, number)) when X <= Y.
      //truncate(2, truncate (5, 7))  is 4,  because truncate (5, 7) = 5 and truncate (2,5) = 4
      //but truncate(2,7) is 6
      //However, we can use Y % X == 0;
      //Example2: If you round a number to the nearest 5, and then you round the remainder to the nearest 25,
      // you would get the same result if you round to the nearest 25 to begin with
      //truncate(25, truncate(5,33)) = 25, because truncate (5,30) = 25
      //truncate(25, 33)) = 25
      //It works in this case
      //Example3: However, the opposite is not true
      //truncate(5, truncate(25,33)) = 25, because truncate (5,25) = 25
      //truncate(5, 33)) = 30
      return topTransformArgs % bottomTransformArgs == 0;
    default:
      //For other data types we require an exact match
      return topTransformArgs.equals(bottomTransformArgs);

  }
}
  /**
   * Keep only the lowest valid Reflection Transform candidate that is compatible with the base dataset
   * @param reflectionPartitionColumn2TransformAndBaseColumnName list of transforms in the reflection, all compatible with the base dataset
   * @return a subset of reflectionPartitionColumn2TransformAndBaseColumnName, possibly with some transforms removed
   */
  public static Map<String, TransformInfo> eliminateSimilarPartitionCandidates(
    final Map<String, TransformInfo> reflectionPartitionColumn2TransformAndBaseColumnName) {
    //Suppose that in the base dataset we have transform by Hour(col1)
    //The reflection has 3 different columns doing Day(col1), Month(col1),Year(col1). Those are all valid candidates.
    //however, it doesn't make sense to put all 3 in the target partitions.
    //it is best if we just use Day(col1) so we can eliminate the maximum amount of files to scan
    //here we implement this logic
    final Map<String, TransformInfo> baseColumnName2LowestTransformation = new HashMap<>();

    //find the lowest transformation for each base dataset column
    for(final Map.Entry<String, TransformInfo> entry:
            reflectionPartitionColumn2TransformAndBaseColumnName.entrySet()){
      final String baseColumnNameInReflection = entry.getValue().getBaseColumnName();
      final IcebergTransformType reflectionTransformType = entry.getValue().getTransformType();
      final  TransformInfo found = baseColumnName2LowestTransformation.get(baseColumnNameInReflection);

      //only one TRUNCATE is expected, so we just save it
      //if not found, also we save the entry as it is the first one
      if(reflectionTransformType == IcebergTransformType.TRUNCATE || found == null){
        baseColumnName2LowestTransformation.put(baseColumnNameInReflection, entry.getValue());
      } else if(isTransformTypeSupportedBy(found.getTransformType(), reflectionTransformType, null, null, null)){
        baseColumnName2LowestTransformation.replace(baseColumnNameInReflection, entry.getValue());
      }
      //else keep the old entry
    }

    //keep only the entries of reflectionPartitionColumn2TransformAndBaseColumnName
    //that have the lowest transformation for each column
    //so if we have Day(col1), Month(col1),Year(col1), here we would only keep Day(col1)
    final Map<String, TransformInfo> result = new HashMap<>();
    for(final Map.Entry<String, TransformInfo> entry:
            reflectionPartitionColumn2TransformAndBaseColumnName.entrySet()) {
      final String baseColumnNameInReflection = entry.getValue().getBaseColumnName();
      final IcebergTransformType reflectionTransformType = entry.getValue().getTransformType();
      final TransformInfo found = baseColumnName2LowestTransformation.get(baseColumnNameInReflection);
      if(found != null && found.getTransformType().equals(reflectionTransformType)){
        //it is possible that the input contains multiples of the same type, such as  Day(col1),Day(col1),Day(col1)
        //remove from baseColumnName2LowestTransformation to make sure only one entry is added
        baseColumnName2LowestTransformation.remove(baseColumnNameInReflection);
        result.put(entry.getKey(),
          new TransformInfo(entry.getValue().getTransformType(),
          entry.getValue().getBaseColumnName(),
          entry.getValue().getArgument(),
          entry.getValue().getDataType()));

      }

    }
    return result;
  }

  /**
   * Keep only reflection partition candidates that are compatible with the base dataset
   * @param reflectionPartitionColumn2TransformInfo partition candidates from the reflection
   * @param baseDatasetPartitionSpec base dataset partition spec
   * @return a subset of reflectionPartitionColumn2TransformInfo, that is compatible with base dataset
   */
  public static Map<String, TransformInfo> eliminateInvalidPartitionCandidates(
    final Map<String, TransformInfo> reflectionPartitionColumn2TransformInfo,
    final PartitionSpec baseDatasetPartitionSpec) {
    final Map<String, TransformInfo> result = new HashMap<>();
    for(final Map.Entry<String, TransformInfo> entry:
            reflectionPartitionColumn2TransformInfo.entrySet()){
      if(entry.getKey() == null || entry.getValue() == null || entry.getValue().getTransformType() == null || entry.getValue().getBaseColumnName() == null){
        continue;
      }
      final String baseColumnNameInReflection = entry.getValue().getBaseColumnName();
      final IcebergTransformType reflectionTransformType = entry.getValue().getTransformType();
      for(final PartitionField partitionField:baseDatasetPartitionSpec.fields()){
        final Optional<IcebergTransformType> baseTransformType = safeGetTransformType(partitionField.transform().toString());
        final String columnName = getColumnName(partitionField, baseDatasetPartitionSpec.schema());
        if(columnName.equals(baseColumnNameInReflection) && baseTransformType.isPresent()
                && isTransformTypeSupportedBy(reflectionTransformType,
                                              baseTransformType.get(),
                                              entry.getValue().getArgument(),
                                              extractArgFromOneArgTransform(baseDatasetPartitionSpec, partitionField),
                                              baseDatasetPartitionSpec.schema().findField(partitionField.sourceId()).type())){
          //we stop at the first match
          //if we get here it means that we can convert from the base transformation to the target transformation
          result.put(entry.getKey(),
            new TransformInfo(entry.getValue().getTransformType(),
              entry.getValue().getBaseColumnName(),
              entry.getValue().getArgument(),
              entry.getValue().getDataType()));
          break;
        }
      }
      //if we didn't find a match it is OK, nothing was added to the result
      //we will later check that there is at least one match in the result
    }
    return result;
  }

  /**
   * Returns the latest partition spec in baseDatasetIcebergMetadata
   * @param baseDatasetIcebergMetadata base dataset Iceberg Metadata
   * @param baseDatasetSchema base dataset Schema
   * @return the latest partition spec in baseDatasetIcebergMetadata
   */
  private static PartitionSpec getLatestPartitionSpec(
                      final IcebergMetadata baseDatasetIcebergMetadata,
                      final Schema baseDatasetSchema) {
    final Map<Integer, PartitionSpec> partitionSpecMap =
            getPartitionSpecMap(baseDatasetIcebergMetadata, baseDatasetSchema);
    return getLatestPartitionSpecFromMap(partitionSpecMap);
  }

  /**
   * Returns a map between the spec sequence number and the PartitionSpec
   * for all the current and previous partition evolutions on baseDatasetIcebergMetadata
   */
  public static Map<Integer, PartitionSpec> getPartitionSpecMap(
                    final IcebergMetadata baseDatasetIcebergMetadata,
                    final Schema baseDatasetSchema) {
    Map<Integer, PartitionSpec> partitionSpecMap = null;
    if (baseDatasetIcebergMetadata.getPartitionSpecsJsonMap() != null) {
      partitionSpecMap = IcebergSerDe.deserializeJsonPartitionSpecMap(
        baseDatasetSchema, baseDatasetIcebergMetadata.getPartitionSpecsJsonMap().toByteArray());
    } else if (baseDatasetIcebergMetadata.getPartitionSpecs() != null) {
      partitionSpecMap = IcebergSerDe.deserializePartitionSpecMap(baseDatasetIcebergMetadata.getPartitionSpecs().toByteArray());
    }
    return partitionSpecMap;
  }

  /**
   * Given a partitionSpecMap, return the latest partition spec
   * @param partitionSpecMap map containing multiple partition specs
   * @return the latest partition spec
   */
  public static PartitionSpec getLatestPartitionSpecFromMap(final Map<Integer, PartitionSpec> partitionSpecMap) {
    //the latest partition spec should have the highest key number
    if(partitionSpecMap == null) {
      return null;
    }
    final int maxKeyInMap = (Collections.max(partitionSpecMap.keySet()));
    return partitionSpecMap.get(maxKeyInMap);
  }

  /**
   * Given a list of Names of Columns in the reflection,
   * populate the transformation and the column used for each one
   * Leave transformation and column used NULL, if the expression is too complex
   * or does not correspond to an Iceberg transformation
   *
   * @param relNode                  Relational algebra tree for the current reflection evaluation
   * @param partitionName2Expression names of columns used in transformation partition spec
   */
  private static void buildValidTransformationAndBaseColumns(final RelNode relNode,
                                                             final Map<String, TransformInfo> partitionName2Expression) {
    int inputRefCounter = 0;
    final RelMetadataQuery relMetadataQuery = relNode.getCluster().getMetadataQuery();
    for(final RelDataTypeField relDataTypeField: relNode.getRowType().getFieldList()) {
      final RexBuilder b = relNode.getCluster().getRexBuilder();
      final TransformInfo transformInfo = partitionName2Expression.get(relDataTypeField.getName());
      if(transformInfo != null){
        final Set<RexNode> rexNodeSet = relMetadataQuery.getExpressionLineage(relNode, b.makeInputRef(relNode, inputRefCounter));
        if(rexNodeSet != null && rexNodeSet.size() == 1){
          final RexNode expression = rexNodeSet.iterator().next();
          final TransformInfo newTransformInfo = generateResolvedReflectionTransformInfo(transformInfo, expression);
          if(newTransformInfo != null) {
            partitionName2Expression.replace(relDataTypeField.getName(), newTransformInfo);
          }
          //else the transform did not parse successfully, we will not consider this column
          //for determining what partitions are compatible between the reflection and the base dataset
        }
      }
      inputRefCounter++;
    }
  }

  /**
   * Given an incomplete Transform info and an expression representing the reflection view definition
   * compute a resolved transform info with the base dataset column and transform resolved
   * There could be a transform in the view definition of the VDS the reflection is based on
   * and there could be a transform in the reflection itself. Here we reconcile those two transforms too.
   */
  public static TransformInfo generateResolvedReflectionTransformInfo(
                                            final TransformInfo incompleteTransformInfo,
                                            final RexNode expression) {
    if(expression == null){
      return null;
    }
    final Pointer<IcebergTransformType> bottomIcebergTransformationPointer= new Pointer<>();
    final Pointer<String> baseColumnNamePointer = new Pointer<>();
    //here we parse the expression definition to get transform used
    //as part of the VDS definition of the dataset the reflection is based on
    //if there is no transform we will get identity
    //if the expression is not a valid transform parseTransformationAndBaseColumn will return false
    final List<Integer> bottomTransformArgs = new ArrayList<>();
    if(!parseTransformationAndBaseColumn(expression, bottomIcebergTransformationPointer, baseColumnNamePointer, bottomTransformArgs)) {
      //we could not parse the definition of this column successfully
      //it is possible VDS contained Dremio functions that are not compatible with Iceberg transforms
      return null;
    }
    //If we are here, we did parse the definition of this column successfully
    //However, it is possible to have a transform on top of transform, and not all combinations are supported
    //Example1: we have an agg reflection with granularity set to date, but we chose to do a month transform on top of it
    //here we will compare if the 2 transforms are compatible and pick month for this case, as effectively we are doing month transform
    //Example2: the reflection partition transform is set to identity,
    //and we have granularity date, in effect we are doing a date transform
    //so the resulting transform would be date
    //Example3: Reflection has default granularity, but has Year transform
    //the resulting transform would be year
    //Example4: It is possible no valid transform is found too
    //For example we have an agg reflection with granularity set to date, but on top of it we did a bucket transform
    //In this case, resolvedTransform will be null, and we will not set the column name either
    //Example5: Reflection partitioned by truncate, identity column
    //the resulting transform would be truncate, we use the length from the reflection
    final Pair<IcebergTransformType, Integer> resolvedTransform = resolveTransformOnTopOfTransform(incompleteTransformInfo.getTransformType(),
      bottomIcebergTransformationPointer.value,
      incompleteTransformInfo.getArgument(),
      (bottomTransformArgs != null && bottomTransformArgs.size() == 1) ? bottomTransformArgs.get(0) : null,
      incompleteTransformInfo.getDataType());
    if (resolvedTransform != null) {
      return new TransformInfo(resolvedTransform.getKey(), baseColumnNamePointer.value, resolvedTransform.getValue(), incompleteTransformInfo.getDataType());
    }
    return null;
  }

  /**
   * Given 2 transforms on top of each other, figure out what is the equivalent single transform if any
   * @param topTransform the top transform
   * @param bottomTransform the bottom transform
   * @return a transform equivalent to topTransform applied on bottomTransform
   */
  public static Pair<IcebergTransformType, Integer>  resolveTransformOnTopOfTransform(
                                          final IcebergTransformType topTransform,
                                          final IcebergTransformType bottomTransform,
                                          final Integer topTransformArg,
                                          final Integer bottomTransformArg,
                                          final Type dataType) {
    if(topTransform == null || IcebergTransformType.IDENTITY.equals(topTransform)) {
      return Pair.of(bottomTransform, bottomTransformArg);
    }
    if(bottomTransform == null || IcebergTransformType.IDENTITY.equals(bottomTransform)){
      return Pair.of(topTransform, topTransformArg);
    }
    if(isTransformTypeSupportedBy(topTransform, bottomTransform, topTransformArg, bottomTransformArg, dataType)){
      return Pair.of(topTransform, topTransformArg);
    }
    return null;
  }

  /**
   * Given an expression containing Dremio functions, does it represent a valid Iceberg transform
   * We support different Dremio functions that are similar to an Iceberg transform such as
   * CAST (column AS DATE) => day transform
   * TO_DATE(column) => day transform
   * DATE_TRUNC('HOUR', column) => hour transform
   * DATE_TRUNC('DAY', column) => day transform
   * DATE_TRUNC('MONTH', column) => month transform
   * DATE_TRUNC('YEAR', column) => year transform
   * LEFT(column, 5) => truncate (5, column)
   * SUBSTR(column, 0, 10) => truncate (10, column)
   * SUBSTRING(column, 0, 10) => truncate (10, column)
   * @param expression expression, possibly containing a Dremio function similar to an Iceberg transform
   * @param icebergTransformationPointer Output, equivalent Iceberg Transform
   * @param baseColumnNamePointer Output, column name the transform is applied to
   * @return true, if expression represents a valid transform, false otherwise
   */
  private static boolean parseTransformationAndBaseColumn(final RexNode expression,
                                                          final Pointer<IcebergTransformType> icebergTransformationPointer,
                                                          final Pointer<String> baseColumnNamePointer,
                                                          final List<Integer> transformArgs) {

    if(expression instanceof RexCall){
      final RexCall rexCall = (RexCall) expression;
      final String operatorName = rexCall.getOperator().getName().toUpperCase();
      switch (operatorName) {
        case "DATE_TRUNC":
          //first operand is the transformation
          icebergTransformationPointer.value = parseDateTruncTransformation(rexCall.operands.get(0));
          //second operand is the column
          baseColumnNamePointer.value = parseBaseColumn(rexCall.operands.get(1));
          break;
        case "CAST":
          if(rexCall.getType().toString().equals("DATE"))
          {
            icebergTransformationPointer.value = IcebergTransformType.DAY;
            baseColumnNamePointer.value = parseBaseColumn(rexCall.operands.get(0));
          }
          break;
        case "TO_DATE":
          icebergTransformationPointer.value = IcebergTransformType.DAY;
          baseColumnNamePointer.value = parseBaseColumn(rexCall.operands.get(0));
          break;
        case "LEFT":
          final Integer offset = parseIntegerFromRexLiteral(rexCall.operands.get(1));
          if(offset != null && offset > 0) {
            icebergTransformationPointer.value = IcebergTransformType.TRUNCATE;
            baseColumnNamePointer.value = parseBaseColumn(rexCall.operands.get(0));
            transformArgs.add(offset);
          }
          break;
        case "SUBSTR":
        case "SUBSTRING":
          final Integer beginningOffset = parseIntegerFromRexLiteral(rexCall.operands.get(1));
          final Integer length = parseIntegerFromRexLiteral(rexCall.operands.get(2));
          //we only support substr that starts at position 0
          //otherwise it is not equivalent to truncate transform
          //in addition, length should be positive, otherwise the meaning is different from truncate
          if(beginningOffset != null && length != null && beginningOffset.equals(0) && length > 0) {
            icebergTransformationPointer.value = IcebergTransformType.TRUNCATE;
            baseColumnNamePointer.value = parseBaseColumn(rexCall.operands.get(0));
            transformArgs.add(length);
          }
          break;
        default:
          //do nothing
      }

    } else if (expression instanceof RexTableInputRef) {
      icebergTransformationPointer.value = IcebergTransformType.IDENTITY;
      baseColumnNamePointer.value = parseBaseColumn(expression);
    }
    return icebergTransformationPointer.value != null
      && baseColumnNamePointer.value != null;
  }

  /**
   * Given a RexNode check if it is RexLiteral representing an Integer
   * If it is, return the Integer, otherwise return null
   */
  private static Integer parseIntegerFromRexLiteral(final RexNode rexNode){
    if(! (rexNode instanceof RexLiteral)){
      return null;
    }
    try {
      return ((RexLiteral) rexNode).getValueAs(Integer.class);
    } catch (final Exception e){
      //error, parse is not successful, return null
      return null;
    }
  }
  private static String parseBaseColumn(final RexNode rexNode) {
    if(rexNode instanceof RexTableInputRef){
      final RexTableInputRef rexTableInputRef = (RexTableInputRef) rexNode;
      return rexTableInputRef.getTableRef().getTable().getRowType().getFieldList().get(rexTableInputRef.getIndex()).getName();
    }
    return null;
  }

  /**
   * Given a string representing the Truncate type of DATE_TRUNC Dremio function, return Iceberg Transform with similar effect
   * For example if we have DATE_TRUNC('MONTH', col1) we will return Month transform
   * @param rexNode Node representing the Truncate type of DATE_TRUNC Dremio function
   * @return Iceberg Transform with similar effect
   */
  private static IcebergTransformType parseDateTruncTransformation(final RexNode rexNode) {
    if(rexNode instanceof RexLiteral){
      final RexLiteral rexLiteral = (RexLiteral)rexNode;
      final String valueUpper = RexLiteral.stringValue(rexLiteral).toUpperCase();
      switch (valueUpper){
        case "HOUR":
          return IcebergTransformType.HOUR;
        case "DAY":
          return IcebergTransformType.DAY;
        case "MONTH":
          return IcebergTransformType.MONTH;
        case "YEAR":
          return IcebergTransformType.YEAR;
        default:
          return null;
      }
    }
    return null;
  }

  /**
   * Gets the TransformType from PartitionField
   * Returns null if invalid transform is used
   * Example: VOID transform or newly added Iceberg transform that is not supported in Dremio yet
   * @param transformName input transform name
   * @return TransformType from TransformType
   */
  public static Optional<PartitionProtobuf.IcebergTransformType> safeGetTransformType(String transformName) {
    try {
      //special handle
      final int index = transformName.indexOf("[");
      if (index != -1){
        transformName = transformName.substring(0,index);
      }
      return Optional.of(PartitionProtobuf.IcebergTransformType.valueOf(transformName.toUpperCase()));
    } catch (final IllegalArgumentException e){
      return Optional.empty();
    }
  }

  /**
   * Adds a new partition transform to a partition spec
   * @param partitionSpecBuilder builder to use
   * @param columnName           the name of the column
   * @param transformType        the transform type for the column
   * @param argument arguments for Truncate Transform
   */
  private static void addPartitionColumn(final PartitionSpec.Builder partitionSpecBuilder,
                                         final String columnName,
                                         final IcebergTransformType transformType,
                                         final Integer argument) {
    switch (transformType){
      case IDENTITY:
        partitionSpecBuilder.identity(columnName);
        break;
      case HOUR:
        partitionSpecBuilder.hour(columnName);
        break;
      case DAY:
        partitionSpecBuilder.day(columnName);
        break;
      case MONTH:
        partitionSpecBuilder.month(columnName);
        break;
      case YEAR:
        partitionSpecBuilder.year(columnName);
        break;
      case TRUNCATE:
        partitionSpecBuilder.truncate(columnName, argument);
        break;
      default:
        throw new RuntimeException("Unsupported partition transform function" + transformType.name() + " for column " + columnName);
    }
  }

  /**
   * Generate a string from PartitionSpec for display purposes.
   * It might not contain all the information from the PartitionSpec
   * @param partitionSpec input spec we want to display
   * @return resulting display string
   */
  public static String toDisplayString(final PartitionSpec partitionSpec) {
    final StringBuilder stringBuilder = new StringBuilder();
    boolean isFirst = true;
    for(final PartitionField field:partitionSpec.fields()){
      final String result = toDisplayString(field, partitionSpec);
      if(result.isEmpty()){
        continue;
      }
      if(isFirst){
        isFirst = false;
      }else{
        stringBuilder.append(", ");
      }
      stringBuilder.append(result);
    }
    return stringBuilder.toString();
  }

  /**
   * Generate a string from PartitionField for display purposes.
   * It might not contain all the information from the PartitionField
   * @param field field we are generating display string for
   * @param spec partition spec containing  the PartitionField
   * @return display string for the PartitionField
   */
  public static String toDisplayString(final PartitionField field, final PartitionSpec spec){
    final Optional<IcebergTransformType> baseTransform = safeGetTransformType(field.transform().toString());
    if(!baseTransform.isPresent()){
      return "";
    }
    final String columnName = getColumnName(field,spec.schema());
    final Integer arg = extractArgFromOneArgTransform(spec, field);
    final String argsString = (arg == null) ? "" : arg + ", ";
    return baseTransform.get() + "(" + argsString + columnName + ")";
  }

  public static boolean isUnlimitedSplitIncrementalRefresh(SnapshotDiffContext snapshotDiffContext, TableMetadata datasetPointer) {
    return snapshotDiffContext != null && snapshotDiffContext.isEnabled() && isInternalIcebergTable(datasetPointer.getDatasetConfig());
  }

  /**
   * A class representing information for a transform, such as
   * its transformType, what column it is on in the base dataset and any arguments
   */
  public static class TransformInfo{
    private final IcebergTransformType transformType;
    private String baseColumnName;
    private final Integer argument;
    private final Type dataType;
    public TransformInfo(final IcebergTransformType transformType,
                         final String baseColumnName,
                         final Integer argument,
                         final Type dataType){
      this.transformType = Preconditions.checkNotNull(transformType, "Transform type cannot be null");
      this.baseColumnName = baseColumnName;
      this.argument = argument;
      this.dataType = dataType;
    }

    @Override
    public boolean equals(final Object obj) {
      if(! (obj instanceof TransformInfo)){
        return false;
      }
      final TransformInfo right = (TransformInfo) obj;
      return transformType.equals(right.transformType)
        && (dataType == null && right.dataType == null ||
              dataType != null && dataType.equals(right.dataType))
        && (baseColumnName == null && right.baseColumnName == null ||
              baseColumnName != null && baseColumnName.equals(right.baseColumnName))
        && (argument == null && right.argument == null ||
              argument != null && argument.equals(right.argument));
    }

    @Override
    public int hashCode() {
      int hashCode = transformType.hashCode();
      if(dataType != null) {
        hashCode ^= dataType.hashCode();
      }
      if(baseColumnName != null){
        hashCode ^= baseColumnName.hashCode();
      }
      if(argument != null){
        hashCode ^= argument.hashCode();
      }
      return hashCode;
    }

    public Integer getArgument() {
      return argument;
    }

    public String getBaseColumnName() {
      return baseColumnName;
    }

    public IcebergTransformType getTransformType() {
      return transformType;
    }

    public void setBaseColumnName(final String baseColumnName){
      this.baseColumnName = baseColumnName;
    }

    public Type getDataType(){
      return dataType;
    }
  }
}
