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
package com.dremio.exec.store.parquet;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.collect.Maps;

public class GlobalDictionaries implements AutoCloseable {
  private final Map<String, VectorContainer> dictionaries; // key is full column path (dotted)

  public static GlobalDictionaries create(OperatorContext context,
                                          FileSystemWrapper fs,
                                          List<GlobalDictionaryFieldInfo> globalDictionaryColumns) throws ExecutionSetupException {
    if (globalDictionaryColumns != null && !globalDictionaryColumns.isEmpty()) {
      final Map<String, VectorContainer> dictionaries = Maps.newHashMap();
      context.getStats().startProcessing();
      try {
        for (GlobalDictionaryFieldInfo field : globalDictionaryColumns) {
          dictionaries.put(field.getFieldName(), ParquetFormatPlugin.loadDictionary(fs, new Path(field.getDictionaryPath()), context.getAllocator()));
        }
        return new GlobalDictionaries(dictionaries);
      } catch (IOException ioe) {
        throw new ExecutionSetupException(ioe);
      } finally {
        context.getStats().stopProcessing();
      }
    }
    return null; // if no columns should be global dictionary encoded.
  }

  private GlobalDictionaries(Map<String, VectorContainer> dictionaries) {
    this.dictionaries = dictionaries;
  }

  @Override
  public void close() throws Exception {
    try {
      AutoCloseables.close(dictionaries.values());
    } finally {
      dictionaries.clear();
    }
  }

  public Map<String, VectorContainer> getDictionaries() {
    return dictionaries;
  }

}