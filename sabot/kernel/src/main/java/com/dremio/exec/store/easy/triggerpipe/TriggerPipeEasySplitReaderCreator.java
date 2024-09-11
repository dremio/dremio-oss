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
package com.dremio.exec.store.easy.triggerpipe;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties.PropertyKey;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties.Util;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.copyinto.IngestionProperties;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.easy.EasySplitReaderCreator;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;
import io.protostuff.ByteString;
import java.util.List;
import java.util.Optional;

/**
 * This class extends {@link EasySplitReaderCreator} and is responsible for creating split readers
 * for trigger pipe scans.
 */
public class TriggerPipeEasySplitReaderCreator extends EasySplitReaderCreator {

  private final IngestionProperties ingestionProperties;

  /**
   * Constructs a new {@code TriggerPipeEasySplitReaderCreator} instance.
   *
   * @param context The operator context.
   * @param fs The file system.
   * @param splitInfo The split and partition information.
   * @param ingestionProperties The ingestion properties.
   * @param tablePath The table path.
   * @param columns The columns.
   * @param formatPlugin The format plugin.
   * @param compressionCodecFactory The compression codec factory.
   * @param extendedFormatOptions The extended format options.
   * @param extendedProperty The extended property.
   */
  public TriggerPipeEasySplitReaderCreator(
      OperatorContext context,
      FileSystem fs,
      SplitAndPartitionInfo splitInfo,
      IngestionProperties ingestionProperties,
      List<List<String>> tablePath,
      List<SchemaPath> columns,
      FormatPlugin formatPlugin,
      CompressionCodecFactory compressionCodecFactory,
      ExtendedFormatOptions extendedFormatOptions,
      ByteString extendedProperty) {
    super(
        context,
        fs,
        splitInfo,
        tablePath,
        columns,
        formatPlugin,
        compressionCodecFactory,
        extendedFormatOptions,
        extendedProperty);
    this.ingestionProperties = ingestionProperties;
  }

  @Override
  protected ByteString getExtendedProperty() {
    Optional<CopyIntoExtendedProperties> copyIntoExtendedPropertiesOptional =
        Util.getProperties(super.getExtendedProperty());
    Preconditions.checkState(
        copyIntoExtendedPropertiesOptional.isPresent(),
        "Copy into extended properties must be present at this point.");
    copyIntoExtendedPropertiesOptional
        .get()
        .setProperty(PropertyKey.INGESTION_PROPERTIES, ingestionProperties);
    return CopyIntoExtendedProperties.Util.getByteString(copyIntoExtendedPropertiesOptional.get());
  }
}
