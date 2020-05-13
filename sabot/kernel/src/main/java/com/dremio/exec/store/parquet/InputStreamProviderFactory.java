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
package com.dremio.exec.store.parquet;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.exec.ExecConstants;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 *
 */
public interface InputStreamProviderFactory {

  String KEY = "dremio.plugins.parquet.input_stream_factory";

  InputStreamProvider create(FileSystem fs, OperatorContext context,
                             Path path, long fileLength, long splitSize, ParquetScanProjectedColumns projectedColumns,
                             ParquetMetadata footerIfKnown, Function<ParquetMetadata, Integer> rowGroupIndexProvider,
                             BiConsumer<Path, ParquetMetadata> depletionListener, boolean readFullFile,
                             List<String> dataset, long mTime) throws IOException;

  InputStreamProviderFactory DEFAULT = new InputStreamProviderFactory() {
    @Override
    public InputStreamProvider create(FileSystem fs, OperatorContext context, Path path, long fileLength, long splitSize, ParquetScanProjectedColumns projectedColumns, ParquetMetadata footerIfKnown, Function<ParquetMetadata, Integer> rowGroupIndexProvider, BiConsumer<Path, ParquetMetadata> depletionListener, boolean readFullFile, List<String> dataset, long mTime) throws IOException {
      OptionManager options = context.getOptions();
      boolean useSingleStream =
        // option is set for single stream
        options.getOption(ExecConstants.PARQUET_SINGLE_STREAM) ||
          // number of columns is above threshold
          projectedColumns.size() >= options.getOption(ExecConstants.PARQUET_SINGLE_STREAM_COLUMN_THRESHOLD) ||
          // split size is below multi stream size limit and the limit is enabled
          (options.getOption(ExecConstants.PARQUET_MULTI_STREAM_SIZE_LIMIT_ENABLE) &&
            splitSize < options.getOption(ExecConstants.PARQUET_MULTI_STREAM_SIZE_LIMIT)) ||
                // if full file is read, it should be a single stream
              readFullFile;

      final long maxFooterLen = context.getOptions().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR);
      return useSingleStream
        ? new SingleStreamProvider(fs, path, fileLength, maxFooterLen, readFullFile, context)
        : new StreamPerColumnProvider(fs, path, fileLength, maxFooterLen, context.getStats());
    }
  };

}
