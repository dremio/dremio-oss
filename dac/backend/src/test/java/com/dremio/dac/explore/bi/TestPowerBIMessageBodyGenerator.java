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
package com.dremio.dac.explore.bi;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Unit tests for {@link PowerBIMessageBodyGenerator}
 */
@RunWith(Parameterized.class)
public class TestPowerBIMessageBodyGenerator {
  @Parameterized.Parameters(name = "{0}")
  public static Object[][] getTestCases() {
    return new Object[][]{
      new Object[]{"schemaWithPeriods", "localhost", new String[]{"Samples", "samples.dremio.com", "SF_incidents"}, "Samples.samples.dremio.com", "SF_incidents"},
      new Object[]{"tableWithPeriod", "localhost", new String[]{"Samples", "samples", "SF_incidents.json"}, "Samples.samples", "SF_incidents.json"},
      new Object[]{"deeplyNested", "localhost", new String[]{"Samples", "l1", "l2", "SF_incidents"}, "Samples.l1.l2", "SF_incidents"},
      new Object[]{"ipAsServer", "127.0.0.1", new String[]{"Samples", "dummySchema", "SF_incidents"}, "Samples.dummySchema", "SF_incidents"},
      new Object[]{"atInSchema", "127.0.0.1", new String[]{"@dremio", "dummySchema", "SF_incidents"}, "@dremio.dummySchema", "SF_incidents"},
      new Object[]{"oneSchemaElement", "localhost", new String[]{"Samples", "SF_incidents.json"}, "Samples", "SF_incidents.json"},
      new Object[]{"ampersandSchema", "localhost", new String[]{ "ampersand&schema", "sfjson"}, "ampersand&schema", "sfjson"},
      new Object[]{"angleSchema", "localhost", new String[]{ "angle<>schema", "sfjson"}, "angle<>schema", "sfjson"},
      new Object[]{"asteriskSchema", "localhost", new String[]{ "asterisk*schema", "sfjson"}, "asterisk*schema", "sfjson"},
      new Object[]{"atSchema", "localhost", new String[]{ "at@schema", "sfjson"}, "at@schema", "sfjson"},
      // TODO: DX-23587: backslashes in identifiers actually fail in Power BI. Determine why, and if this test needs to be corrected.
      new Object[]{"backslashSchema", "localhost", new String[]{ "backslash\\schema", "sfjson"}, "backslash\\schema", "sfjson"},
      new Object[]{"backtickSchema", "localhost", new String[]{ "backtick`schema", "sfjson"}, "backtick`schema", "sfjson"},
      new Object[]{"bracketSchema", "localhost", new String[]{ "bracket[]schema", "sfjson"}, "bracket[]schema", "sfjson"},
      new Object[]{"carratSchema", "localhost", new String[]{ "carrat^schema", "sfjson"}, "carrat^schema", "sfjson"},
      new Object[]{"commaSchema", "localhost", new String[]{ "comma,schema", "sfjson"}, "comma,schema", "sfjson"},
      new Object[]{"curlyBraceSchema", "localhost", new String[]{ "curlybrace{}schema", "sfjson"}, "curlybrace{}schema", "sfjson"},
      new Object[]{"dollarSchema", "localhost", new String[]{ "dollar$schema", "sfjson"}, "dollar$schema", "sfjson"},
      new Object[]{"equalsSchema", "localhost", new String[]{ "equals=schema", "sfjson"}, "equals=schema", "sfjson"},
      new Object[]{"exclamationSchema", "localhost", new String[]{ "exclamation_schema!", "sfjson"}, "exclamation_schema!", "sfjson"},
      new Object[]{"hyphenSchema", "localhost", new String[]{ "hyphen-schema", "sfjson"}, "hyphen-schema", "sfjson"},
      new Object[]{"parenthesesSchema", "localhost", new String[]{ "parentheses()schema", "sfjson"}, "parentheses()schema", "sfjson"},
      new Object[]{"periodSchema", "localhost", new String[]{ "period.schema", "sfjson"}, "period.schema", "sfjson"},
      new Object[]{"pipeSchema", "localhost", new String[]{ "pipe|schema", "sfjson"}, "pipe|schema", "sfjson"},
      new Object[]{"plusSchema", "localhost", new String[]{ "plus+schema", "sfjson"}, "plus+schema", "sfjson"},
      new Object[]{"questionSchema", "localhost", new String[]{ "question?schema", "sfjson"}, "question?schema", "sfjson"},
      new Object[]{"quoteSchema", "localhost", new String[]{ "quote'schema", "sfjson"}, "quote'schema", "sfjson"},
      new Object[]{"slashSchema", "localhost", new String[]{ "slash/schema", "sfjson"}, "slash/schema", "sfjson"},
      new Object[]{"tildeSchema", "localhost", new String[]{ "tilde~schema", "sfjson"}, "tilde~schema", "sfjson"},
      new Object[]{"hashSchema", "localhost", new String[]{ "hash#schema", "sfjson"}, "hash#schema", "sfjson"},
      new Object[]{"colonSchema", "localhost", new String[]{ "colon:schema", "sfjson"}, "colon:schema", "sfjson"},
      new Object[]{"semiColonSchema", "localhost", new String[]{ "semi-colon;schema", "sfjson"}, "semi-colon;schema", "sfjson"},
      new Object[]{"umlautSchema", "localhost", new String[]{ "umlautüschema", "sfjson"}, "umlautüschema", "sfjson"},
      new Object[]{"number1Schema", "localhost", new String[]{ "number1schema", "sfjson"}, "number1schema", "sfjson"},
      new Object[]{"ampersandTbl", "localhost", new String[]{ "@dremio", "ampersand&tbl"}, "@dremio", "ampersand&tbl"},
      new Object[]{"angleTbl", "localhost", new String[]{ "@dremio", "angle<>tbl"}, "@dremio", "angle<>tbl"},
      new Object[]{"asteriskTbl", "localhost", new String[]{ "@dremio", "asterisk*tbl"}, "@dremio", "asterisk*tbl"},
      new Object[]{"atTbl", "localhost", new String[]{ "@dremio", "at@tbl"}, "@dremio", "at@tbl"},
      // TODO: DX-23587: backslashes in identifiers actually fail in Power BI. Determine why, and if this test needs to be corrected.
      new Object[]{"backslashTbl", "localhost", new String[]{ "@dremio", "backslash\\tbl"}, "@dremio", "backslash\\tbl"},
      new Object[]{"backtickSTbl", "localhost", new String[]{ "@dremio", "backtick`tbl"}, "@dremio", "backtick`tbl"},
      new Object[]{"bracketTbl", "localhost", new String[]{ "@dremio", "bracket[]tbl"}, "@dremio", "bracket[]tbl"},
      new Object[]{"carratTbl", "localhost", new String[]{ "@dremio", "carrat^tbl"}, "@dremio", "carrat^tbl"},
      new Object[]{"commaTbl", "localhost", new String[]{ "@dremio", "comma,tbl"}, "@dremio", "comma,tbl"},
      new Object[]{"curlyBraceTbl", "localhost", new String[]{ "@dremio", "curlybrace{}tbl"}, "@dremio", "curlybrace{}tbl"},
      new Object[]{"dollarTbl", "localhost", new String[]{ "@dremio", "dollar$tbl"}, "@dremio", "dollar$tbl"},
      new Object[]{"equalsTbl", "localhost", new String[]{ "@dremio", "equals=tbl"}, "@dremio", "equals=tbl"},
      new Object[]{"excalmationTbl", "localhost", new String[]{ "@dremio", "exclamation_tbl!"}, "@dremio", "exclamation_tbl!"},
      new Object[]{"hyphenTbl", "localhost", new String[]{ "@dremio", "hyphen-tbl"}, "@dremio", "hyphen-tbl"},
      new Object[]{"parenthesesTbl", "localhost", new String[]{ "@dremio", "parentheses()tbl"}, "@dremio", "parentheses()tbl"},
      new Object[]{"periodTbl", "localhost", new String[]{ "@dremio", "period.tbl"}, "@dremio", "period.tbl"},
      new Object[]{"pipeTbl", "localhost", new String[]{ "@dremio", "pipe|tbl"}, "@dremio", "pipe|tbl"},
      new Object[]{"plusTbl", "localhost", new String[]{ "@dremio", "plus+tbl"}, "@dremio", "plus+tbl"},
      new Object[]{"questionTbl", "localhost", new String[]{ "@dremio", "question?tbl"}, "@dremio", "question?tbl"},
      new Object[]{"quoteTbl", "localhost", new String[]{ "@dremio", "quote'tbl"}, "@dremio", "quote'tbl"},
      new Object[]{"slashTbl", "localhost", new String[]{ "@dremio", "slash/tbl"}, "@dremio", "slash/tbl"},
      new Object[]{"tildeTbl", "localhost", new String[]{ "@dremio", "tilde~tbl"}, "@dremio", "tilde~tbl"},
      new Object[]{"hashTbl", "localhost", new String[]{ "@dremio", "hash#tbl"}, "@dremio", "hash#tbl"},
      new Object[]{"colonTbl", "localhost", new String[]{ "@dremio", "colon:tbl"}, "@dremio", "colon:tbl"},
      new Object[]{"semiColonTbl", "localhost", new String[]{ "@dremio", "semi-colon;tbl"}, "@dremio", "semi-colon;tbl"},
      new Object[]{"umlautTbl", "localhost", new String[]{ "@dremio", "umlautütbl"}, "@dremio", "umlautütbl"},
      new Object[]{"number1Tbl", "localhost", new String[]{ "@dremio", "number1tbl"}, "@dremio", "number1tbl"}
    };
  }

  private final String server;
  private final String expectedSchema;
  private final String expectedTable;
  private final DatasetConfig datasetConfig;

  public TestPowerBIMessageBodyGenerator(String testName, String server,
                                         String[] datasetPathComponents,
                                         String expectedSchema, String expectedTable) {
    this.server = server;
    this.expectedSchema = expectedSchema;
    this.expectedTable = expectedTable;
    this.datasetConfig = new DatasetConfig();
    this.datasetConfig.setFullPathList(Arrays.asList(datasetPathComponents));
  }

  @Test
  public void verifyDSRFile() {
    final PowerBIMessageBodyGenerator.DSRFile dsrFile =
      PowerBIMessageBodyGenerator.createDSRFile(server, datasetConfig);

    assertEquals("0.1", dsrFile.getVersion());

    final PowerBIMessageBodyGenerator.Connection connection =
      dsrFile.getConnections()[0];

    assertEquals("DirectQuery", connection.getMode());

    final PowerBIMessageBodyGenerator.DataSourceReference details = connection.getDSR();
    assertEquals("dremio", details.getProtocol());

    final PowerBIMessageBodyGenerator.DSRConnectionInfo address = details.getAddress();
    assertEquals(server, address.getServer());
    assertEquals(expectedSchema, address.getSchema());
    assertEquals(expectedTable, address.getTable());
  }
}
