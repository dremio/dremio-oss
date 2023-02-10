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

import java.util.Set;

import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.util.BuiltInMethod;

import com.dremio.exec.planner.physical.SelectionVectorRemoverPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.google.common.collect.Sets;

/**
 * Override {@link RelMdTableReferences}
 */
public class RelMdTableReferences extends org.apache.calcite.rel.metadata.RelMdTableReferences{
  private static final RelMdTableReferences INSTANCE = new RelMdTableReferences();

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.TABLE_REFERENCES.method, INSTANCE);
  /**
   * TableFunctionPrel table reference.
   */
  public Set<RexTableInputRef.RelTableRef> getTableReferences(TableFunctionPrel rel, RelMetadataQuery mq) {
    return Sets.newHashSet(RexTableInputRef.RelTableRef.of(rel.getTable(), 0));
  }

  public Set<RexTableInputRef.RelTableRef> getTableReferences(SelectionVectorRemoverPrel rel, RelMetadataQuery mq) {
    return mq.getTableReferences(rel.getInput());
  }
}
