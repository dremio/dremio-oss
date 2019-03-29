/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import com.dremio.exec.planner.physical.HashToMergeExchangePrel;
import com.dremio.exec.planner.physical.StreamAggPrel;
import com.google.common.collect.ImmutableList;

/**
 * Provide collation metadata for Dremio specific nodes
 */
public class RelMdCollation implements MetadataHandler<BuiltInMetadata.Collation> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.COLLATIONS.method, new RelMdCollation());

  private RelMdCollation() {}

  @Override
  public MetadataDef<BuiltInMetadata.Collation> getDef() {
    return BuiltInMetadata.Collation.DEF;
  }

  public ImmutableList<RelCollation> collations(StreamAggPrel rel,
      RelMetadataQuery mq) {

    return ImmutableList.of(StreamAggPrel.collation(rel.getGroupSet()));
  }

  public ImmutableList<RelCollation> collations(HashToMergeExchangePrel rel,
      RelMetadataQuery mq) {
    return ImmutableList.of(rel.getCollation());
  }

}
