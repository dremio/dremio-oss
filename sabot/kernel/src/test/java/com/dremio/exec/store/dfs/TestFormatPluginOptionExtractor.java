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
package com.dremio.exec.store.dfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collection;

import org.junit.Test;

import com.dremio.exec.ExecTest;
import com.dremio.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import com.fasterxml.jackson.annotation.JsonTypeName;


public class TestFormatPluginOptionExtractor extends ExecTest {

  @Test
  public void test() {
    FormatPluginOptionExtractor e = new FormatPluginOptionExtractor(CLASSPATH_SCAN_RESULT);
    Collection<FormatPluginOptionsDescriptor> options = e.getOptions();
    for (FormatPluginOptionsDescriptor d : options) {
      assertEquals(d.pluginConfigClass.getAnnotation(JsonTypeName.class).value(), d.typeName);
      switch (d.typeName) {
        case "text":
          assertEquals(TextFormatConfig.class, d.pluginConfigClass);
          assertEquals(
              "(type: String, lineDelimiter: String, fieldDelimiter: String, quote: String, escape: String, " +
                  "comment: String, skipFirstLine: boolean, extractHeader: boolean, " +
                  "autoGenerateColumnNames: boolean, trimHeader: boolean, outputExtension: String)",
              d.presentParams()
          );
          break;
        case "named":
          assertEquals(NamedFormatPluginConfig.class, d.pluginConfigClass);
          assertEquals("(type: String, name: String)", d.presentParams());
          break;
        case "json":
          assertEquals(d.typeName, "(type: String, outputExtension: String, prettyPrint: boolean)", d.presentParams());
          break;
        case "parquet":
          assertEquals(d.typeName, "(type: String, autoCorrectCorruptDates: boolean, outputExtension: String)", d.presentParams());
          break;
        case "arrow":
          assertEquals(d.typeName, "(type: String, outputExtension: String)", d.presentParams());
          break;
        case "sequencefile":
        case "avro":
          assertEquals(d.typeName, "(type: String)", d.presentParams());
          break;
        case "excel":
          assertEquals(d.typeName, "(type: String, sheet: String, extractHeader: boolean, hasMergedCells: boolean, xls: boolean)",
              d.presentParams());
          break;
        case "iceberg":
          assertEquals(d.typeName, "(type: String, outputExtension: String, metaStoreType: IcebergMetaStoreType, dataFormatType: FileType, dataFormatConfig: FormatPluginConfig)",
            d.presentParams());
          break;
        case "delta":
          assertEquals(d.typeName, "(type: String, dataFormatType: FileType)", d.presentParams());
          break;
        default:
          fail("add validation for format plugin type " + d.typeName);
      }
    }
  }
}
