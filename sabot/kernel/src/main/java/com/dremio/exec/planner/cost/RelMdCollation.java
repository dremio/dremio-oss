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
package com.dremio.exec.planner.cost;

import java.util.List;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import com.dremio.exec.planner.common.LimitRelBase;
import com.dremio.exec.planner.physical.HashToMergeExchangePrel;
import com.dremio.exec.planner.physical.StreamAggPrel;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * a metadata handler for collations.
 */
public class RelMdCollation implements MetadataHandler<BuiltInMetadata.Collation> {
  private static final RelMdCollation INSTANCE =
      new RelMdCollation();

  private static ImmutableList<RelCollation> EMPTY = ImmutableList.of(RelCollations.EMPTY);

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.COLLATIONS.method, INSTANCE);

  @Override
  public MetadataDef<BuiltInMetadata.Collation> getDef() {
    return BuiltInMetadata.Collation.DEF;
  }

  // Replace Calcite implementation to normalize empty collation
  public ImmutableList<RelCollation> collations(RelSubset rel,
      RelMetadataQuery mq) {
    List<RelCollation> collations =
        Preconditions.checkNotNull(
            rel.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE));
    // Single rels always return an empty list but Relsubset defaults to a list with one empty
    // element. Normalizing the output to prevent mismatches
    return EMPTY.equals(collations) ? ImmutableList.of() : ImmutableList.copyOf(collations);
  }

  public ImmutableList<RelCollation> collations(StreamAggPrel rel,
      RelMetadataQuery mq) {

    return ImmutableList.of(StreamAggPrel.collation(rel.getGroupSet()));
  }

  public ImmutableList<RelCollation> collations(LimitRelBase rel, RelMetadataQuery mq) {
    return ImmutableList.of();
  }

  public ImmutableList<RelCollation> collations(HashToMergeExchangePrel rel,
      RelMetadataQuery mq) {
    return ImmutableList.of(rel.getCollation());
  }
}
