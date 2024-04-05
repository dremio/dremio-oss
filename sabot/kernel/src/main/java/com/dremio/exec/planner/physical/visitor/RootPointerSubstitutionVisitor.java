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
package com.dremio.exec.planner.physical.visitor;

import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.planner.common.TableMetadataConsumer;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergManifestListPrel;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.calcite.rel.RelNode;

/**
 * {@link Prel} visitor for replacing Iceberg root pointer metadata within a tree. Callers should
 * ensure that this is a valid operation and outputs a valid tree.
 */
public class RootPointerSubstitutionVisitor
    extends BasePrelVisitor<Prel, Collection<DatasetConfig>, RuntimeException> {

  private final Map<DatasetConfig, DatasetConfig> newPointers;

  public RootPointerSubstitutionVisitor(final Map<DatasetConfig, DatasetConfig> newPointers) {
    this.newPointers = newPointers;
  }

  /**
   * Searches for {@link IcebergManifestListPrel} nodes within a tree and will attempt to replace
   * {@link DatasetConfig} from provided map. Replaced metadata is added to the provided collection
   * so the caller can easily determine what has been replaced.
   */
  public static Prel substitute(
      final Prel prel,
      final Map<DatasetConfig, DatasetConfig> newPointers,
      final Collection<DatasetConfig> replaced) {
    return prel.accept(new RootPointerSubstitutionVisitor(newPointers), replaced);
  }

  @Override
  public Prel visitLeaf(LeafPrel prel, Collection<DatasetConfig> replaced) throws RuntimeException {
    if (prel instanceof IcebergManifestListPrel) {
      final IcebergManifestListPrel manifestListPrel = (IcebergManifestListPrel) prel;
      final TableMetadata tableMetadata = manifestListPrel.getTableMetadata();
      final DatasetConfig datasetConfig = tableMetadata.getDatasetConfig();
      if (newPointers.containsKey(datasetConfig)) {
        final DatasetConfig newDatasetConfig = newPointers.get(datasetConfig);
        final TableMetadataConsumer newManifestListPrel =
            manifestListPrel.applyTableMetadata(
                new TableMetadataImpl(
                    tableMetadata.getStoragePluginId(),
                    newDatasetConfig,
                    tableMetadata.getUser(),
                    (SplitsPointer) tableMetadata.getSplitsKey(),
                    tableMetadata.getPrimaryKey()));
        prel = (LeafPrel) newManifestListPrel;
        replaced.add(newDatasetConfig);
      }
    }
    return prel;
  }

  @Override
  public Prel visitPrel(Prel prel, Collection<DatasetConfig> replaced) throws RuntimeException {
    final boolean[] mutated = {false};
    final ArrayList<RelNode> inputs = new ArrayList<>();
    prel.forEach(
        input -> {
          final Prel mutatedInput = input.accept(this, replaced);
          mutated[0] |= input != mutatedInput;
          inputs.add(mutatedInput);
        });
    return mutated[0] ? (Prel) prel.copy(prel.getTraitSet(), inputs) : prel;
  }
}
