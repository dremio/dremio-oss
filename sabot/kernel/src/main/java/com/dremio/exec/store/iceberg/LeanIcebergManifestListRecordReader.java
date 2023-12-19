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

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.iceberg.DremioManifestListReaderUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * Lean version of {@link IcebergManifestListRecordReader} which doesn't load the TableMetadata, rather reads manifest
 * list avro directly for the manifest entries.
 *
 * Unsupported cases-
 * <li> Push-down filter expressions </li>
 * <li> Specific checks for Iceberg V2 checks </li>
 * <li> Listing specific manifest types (DELETE/DATA). All the manifests are listed by this reader </li>
 * <li> In case schema is not available, the class will ignore setting ColID maps. </li>
 * <li> Setting partition stats </li>
 */
public class LeanIcebergManifestListRecordReader extends IcebergManifestListRecordReader {

  private final String manifestListPath;

  public LeanIcebergManifestListRecordReader(OperatorContext context, String manifestListPath,
                                             SupportsIcebergRootPointer pluginForIceberg, List<String> dataset,
                                             String dataSourcePluginId, BatchSchema fullSchema, OpProps props,
                                             List<String> partitionCols, Optional<Schema> icebergTableSchema) throws IOException {
    super(context, null, pluginForIceberg, dataset, dataSourcePluginId, fullSchema, props, partitionCols,
      createEmptyProp(), ManifestContentType.ALL);

    this.manifestListPath = ObjectUtils.requireNonEmpty(manifestListPath, "manifestListPath is null");
    this.icebergTableSchema = icebergTableSchema.orElse(null);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;

    initializeDatasetXAttr();
    initializeOutVectors();

    FileIO io = createIO(manifestListPath);
    manifestFileIterator = DremioManifestListReaderUtils.read(io, manifestListPath).iterator();
  }

  @Override
  protected void initializeDatasetXAttr() {
    if (icebergTableSchema != null) {
      super.initializeDatasetXAttr();
    } else {
      setColIds = outIdx -> {};
    }
  }

  private static IcebergExtendedProp createEmptyProp() throws IOException {
    return new IcebergExtendedProp(
      null,
      IcebergSerDe.serializeToByteArray(Expressions.alwaysTrue()),
      -1,
      null
    );
  }
}
