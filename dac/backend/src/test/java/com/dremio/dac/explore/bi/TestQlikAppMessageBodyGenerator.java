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
package com.dremio.dac.explore.bi;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.lang.annotation.Annotation;
import java.util.Arrays;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.dremio.dac.explore.DatasetTool;
import com.dremio.dac.server.WebServer;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;

/**
 * Unit tests for {@link QlikAppMessageBodyGenerator}
 */
public class TestQlikAppMessageBodyGenerator {

  @Test
  public void testGeneratedOutput() throws Exception {
    QlikAppMessageBodyGenerator generator = new QlikAppMessageBodyGenerator();

    VirtualDataset dataset = new VirtualDataset()
        .setSqlFieldsList(Arrays.asList(
            new ViewFieldType("testdimension", "VARCHAR"),
            new ViewFieldType("testdimension2", "CHAR"),
            new ViewFieldType("testmeasure", "INTEGER"),
            new ViewFieldType("testmeasure2", "REAL"),
            new ViewFieldType("testdetail", "MAP"),
            new ViewFieldType("testdetail2", "STRUCTURED")));

    DatasetConfig datasetConfig = new DatasetConfig()
        .setName("UNTITLED")
        .setType(DatasetType.VIRTUAL_DATASET)
        .setFullPathList(DatasetTool.TMP_DATASET_PATH.toPathList())
        .setVirtualDataset(dataset);

    MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(generator.isWriteable(datasetConfig.getClass(), null, null, WebServer.MediaType.TEXT_PLAIN_QLIK_APP_TYPE));
    generator.writeTo(datasetConfig, DatasetConfig.class, null, new Annotation[] {}, WebServer.MediaType.TEXT_PLAIN_QLIK_APP_TYPE, httpHeaders, baos);

    String script = new String(baos.toByteArray(), UTF_8);

    assertTrue(script.contains(" DIMENSION \"testdimension\", \"testdimension2\""));
    assertTrue(script.contains(" MEASURE \"testmeasure\", \"testmeasure2\""));
    assertTrue(script.contains(" DETAIL \"testdetail\", \"testdetail2\""));
    assertTrue(script.contains(" FROM \"tmp\".\"UNTITLED\""));
  }

  @Test
  public void testQuoting() throws Exception {
    QlikAppMessageBodyGenerator generator = new QlikAppMessageBodyGenerator();

    VirtualDataset dataset = new VirtualDataset()
        .setSqlFieldsList(Arrays.asList(new ViewFieldType("testdimension", "VARCHAR"), new ViewFieldType("testmeasure", "INTEGER")));

    DatasetConfig datasetConfig = new DatasetConfig()
        .setName("UNTITLED")
        .setType(DatasetType.VIRTUAL_DATASET)
        .setFullPathList(Arrays.asList("space", "folder.ext", "UNTITLED"))
        .setVirtualDataset(dataset);

    MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(generator.isWriteable(datasetConfig.getClass(), null, null, WebServer.MediaType.TEXT_PLAIN_QLIK_APP_TYPE));
    generator.writeTo(datasetConfig, DatasetConfig.class, null, new Annotation[] {}, WebServer.MediaType.TEXT_PLAIN_QLIK_APP_TYPE, httpHeaders, baos);

    String script = new String(baos.toByteArray(), UTF_8);

    assertTrue(script.contains(" DIMENSION \"testdimension\""));
    assertTrue(script.contains(" MEASURE \"testmeasure\""));
    assertTrue(script.contains(" FROM \"space\".\"folder.ext\".\"UNTITLED\""));
  }

  @Test
  public void testFieldNamesWithSpaceQuoting() throws Exception {
    QlikAppMessageBodyGenerator generator = new QlikAppMessageBodyGenerator();

    VirtualDataset dataset = new VirtualDataset()
      .setSqlFieldsList(Arrays.asList(new ViewFieldType("test dimension", "VARCHAR"), new ViewFieldType("test \" measure", "INTEGER")));

    DatasetConfig datasetConfig = new DatasetConfig()
      .setName("UNTITLED")
      .setType(DatasetType.VIRTUAL_DATASET)
      .setFullPathList(Arrays.asList("space", "folder.ext", "UNTITLED"))
      .setVirtualDataset(dataset);

    MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(generator.isWriteable(datasetConfig.getClass(), null, null, WebServer.MediaType.TEXT_PLAIN_QLIK_APP_TYPE));
    generator.writeTo(datasetConfig, DatasetConfig.class, null, new Annotation[] {}, WebServer.MediaType.TEXT_PLAIN_QLIK_APP_TYPE, httpHeaders, baos);

    String script = new String(baos.toByteArray(), UTF_8);

    // make sure everything is escaped correctly
    assertTrue(script.contains(" DIMENSION \"test dimension\""));
    assertTrue(script.contains(" MEASURE \"test \"\" measure\""));
    assertTrue(script.contains(" FROM \"space\".\"folder.ext\".\"UNTITLED\""));
  }

  @Test
  public void testQuoteInPathAndDatasetName() throws Exception {
    QlikAppMessageBodyGenerator generator = new QlikAppMessageBodyGenerator();

    VirtualDataset dataset = new VirtualDataset()
      .setSqlFieldsList(Arrays.asList(new ViewFieldType("test dimension", "VARCHAR"), new ViewFieldType("test \" measure", "INTEGER")));

    DatasetConfig datasetConfig = new DatasetConfig()
      .setName("UNTITLED")
      .setType(DatasetType.VIRTUAL_DATASET)
      .setFullPathList(Arrays.asList("@dremio", "fol\"der.ext", "foo", "bar"))
      .setVirtualDataset(dataset);

    MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(generator.isWriteable(datasetConfig.getClass(), null, null, WebServer.MediaType.TEXT_PLAIN_QLIK_APP_TYPE));
    generator.writeTo(datasetConfig, DatasetConfig.class, null, new Annotation[] {}, WebServer.MediaType.TEXT_PLAIN_QLIK_APP_TYPE, httpHeaders, baos);

    String script = new String(baos.toByteArray(), UTF_8);

    // make sure everything is escaped correctly
    assertTrue(script.contains(" DIMENSION \"test dimension\""));
    assertTrue(script.contains(" MEASURE \"test \"\" measure\""));
    assertTrue(script.contains(" FROM \"@dremio\".\"fol\"\"der.ext\".\"foo\".\"bar\""));
  }

  @Test
  public void testSanitizing() throws Exception {
    QlikAppMessageBodyGenerator generator = new QlikAppMessageBodyGenerator();

    VirtualDataset dataset = new VirtualDataset()
      .setSqlFieldsList(Arrays.asList(new ViewFieldType("testdetail2", "STRUCTURED")));

    DatasetConfig datasetConfig = new DatasetConfig()
      .setName("evil /!@ *-=+{}<>,~ characters")
      .setType(DatasetType.VIRTUAL_DATASET)
      .setFullPathList(DatasetTool.TMP_DATASET_PATH.toPathList())
      .setVirtualDataset(dataset);

    MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(generator.isWriteable(datasetConfig.getClass(), null, null, WebServer.MediaType.TEXT_PLAIN_QLIK_APP_TYPE));
    generator.writeTo(datasetConfig, DatasetConfig.class, null, new Annotation[] {}, WebServer.MediaType.TEXT_PLAIN_QLIK_APP_TYPE, httpHeaders, baos);

    String script = new String(baos.toByteArray(), UTF_8);

    assertTrue(script.contains("evil_characters: DIRECT QUERY"));
  }
}
