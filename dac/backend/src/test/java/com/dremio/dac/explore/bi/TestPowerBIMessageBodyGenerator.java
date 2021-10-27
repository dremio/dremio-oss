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

import javax.ws.rs.core.Configuration;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Unit tests for {@link PowerBIMessageBodyGenerator}
 */
@RunWith(Parameterized.class)
public class TestPowerBIMessageBodyGenerator {
  @Parameterized.Parameters(name = "{0}")
  public static Object[][] getTestCases() {
    return new Object[][]{
      new Object[]{"schemaWithPeriods", "localhost", 31010, new String[]{"Samples", "samples.dremio.com", "SF_incidents"}, "Samples.samples.dremio.com", "SF_incidents"},
      new Object[]{"tableWithPeriod", "localhost", 31010, new String[]{"Samples", "samples", "SF_incidents.json"}, "Samples.samples", "SF_incidents.json"},
      new Object[]{"deeplyNested", "localhost", 31010, new String[]{"Samples", "l1", "l2", "SF_incidents"}, "Samples.l1.l2", "SF_incidents"},
      new Object[]{"ipAsServer", "127.0.0.1", 31010, new String[]{"Samples", "dummySchema", "SF_incidents"}, "Samples.dummySchema", "SF_incidents"},
      new Object[]{"atInSchema", "127.0.0.1", 31010, new String[]{"@dremio", "dummySchema", "SF_incidents"}, "@dremio.dummySchema", "SF_incidents"},
      new Object[]{"oneSchemaElement", "localhost", 31010, new String[]{"Samples", "SF_incidents.json"}, "Samples", "SF_incidents.json"},
      new Object[]{"ampersandSchema", "localhost", 31010, new String[]{ "ampersand&schema", "sfjson"}, "ampersand&schema", "sfjson"},
      new Object[]{"angleSchema", "localhost", 31010, new String[]{ "angle<>schema", "sfjson"}, "angle<>schema", "sfjson"},
      new Object[]{"asteriskSchema", "localhost", 31010, new String[]{ "asterisk*schema", "sfjson"}, "asterisk*schema", "sfjson"},
      new Object[]{"atSchema", "localhost", 31010, new String[]{ "at@schema", "sfjson"}, "at@schema", "sfjson"},
      // TODO: DX-23587: backslashes in identifiers actually fail in Power BI. Determine why, and if this test needs to be corrected.
      new Object[]{"backslashSchema", "localhost", 31010, new String[]{ "backslash\\schema", "sfjson"}, "backslash\\schema", "sfjson"},
      new Object[]{"backtickSchema", "localhost", 31010, new String[]{ "backtick`schema", "sfjson"}, "backtick`schema", "sfjson"},
      new Object[]{"bracketSchema", "localhost", 31010, new String[]{ "bracket[]schema", "sfjson"}, "bracket[]schema", "sfjson"},
      new Object[]{"carratSchema", "localhost", 31010, new String[]{ "carrat^schema", "sfjson"}, "carrat^schema", "sfjson"},
      new Object[]{"commaSchema", "localhost", 31010, new String[]{ "comma,schema", "sfjson"}, "comma,schema", "sfjson"},
      new Object[]{"curlyBraceSchema", "localhost", 31010, new String[]{ "curlybrace{}schema", "sfjson"}, "curlybrace{}schema", "sfjson"},
      new Object[]{"dollarSchema", "localhost", 31010, new String[]{ "dollar$schema", "sfjson"}, "dollar$schema", "sfjson"},
      new Object[]{"equalsSchema", "localhost", 31010, new String[]{ "equals=schema", "sfjson"}, "equals=schema", "sfjson"},
      new Object[]{"exclamationSchema", "localhost", 31010, new String[]{ "exclamation_schema!", "sfjson"}, "exclamation_schema!", "sfjson"},
      new Object[]{"hyphenSchema", "localhost", 31010, new String[]{ "hyphen-schema", "sfjson"}, "hyphen-schema", "sfjson"},
      new Object[]{"parenthesesSchema", "localhost", 31010, new String[]{ "parentheses()schema", "sfjson"}, "parentheses()schema", "sfjson"},
      new Object[]{"periodSchema", "localhost", 31010, new String[]{ "period.schema", "sfjson"}, "period.schema", "sfjson"},
      new Object[]{"pipeSchema", "localhost", 31010, new String[]{ "pipe|schema", "sfjson"}, "pipe|schema", "sfjson"},
      new Object[]{"plusSchema", "localhost", 31010, new String[]{ "plus+schema", "sfjson"}, "plus+schema", "sfjson"},
      new Object[]{"questionSchema", "localhost", 31010, new String[]{ "question?schema", "sfjson"}, "question?schema", "sfjson"},
      new Object[]{"quoteSchema", "localhost", 31010, new String[]{ "quote'schema", "sfjson"}, "quote'schema", "sfjson"},
      new Object[]{"slashSchema", "localhost", 31010, new String[]{ "slash/schema", "sfjson"}, "slash/schema", "sfjson"},
      new Object[]{"tildeSchema", "localhost", 31010, new String[]{ "tilde~schema", "sfjson"}, "tilde~schema", "sfjson"},
      new Object[]{"hashSchema", "localhost", 31010, new String[]{ "hash#schema", "sfjson"}, "hash#schema", "sfjson"},
      new Object[]{"colonSchema", "localhost", 31010, new String[]{ "colon:schema", "sfjson"}, "colon:schema", "sfjson"},
      new Object[]{"semiColonSchema", "localhost", 31010, new String[]{ "semi-colon;schema", "sfjson"}, "semi-colon;schema", "sfjson"},
      new Object[]{"umlautSchema", "localhost", 31010, new String[]{ "umlautüschema", "sfjson"}, "umlautüschema", "sfjson"},
      new Object[]{"number1Schema", "localhost", 31010, new String[]{ "number1schema", "sfjson"}, "number1schema", "sfjson"},
      new Object[]{"ampersandTbl", "localhost", 31010, new String[]{ "@dremio", "ampersand&tbl"}, "@dremio", "ampersand&tbl"},
      new Object[]{"angleTbl", "localhost", 31010, new String[]{ "@dremio", "angle<>tbl"}, "@dremio", "angle<>tbl"},
      new Object[]{"asteriskTbl", "localhost", 31010, new String[]{ "@dremio", "asterisk*tbl"}, "@dremio", "asterisk*tbl"},
      new Object[]{"atTbl", "localhost", 31010, new String[]{ "@dremio", "at@tbl"}, "@dremio", "at@tbl"},
      // TODO: DX-23587: backslashes in identifiers actually fail in Power BI. Determine why, and if this test needs to be corrected.
      new Object[]{"backslashTbl", "localhost", 31010, new String[]{ "@dremio", "backslash\\tbl"}, "@dremio", "backslash\\tbl"},
      new Object[]{"backtickSTbl", "localhost", 31010, new String[]{ "@dremio", "backtick`tbl"}, "@dremio", "backtick`tbl"},
      new Object[]{"bracketTbl", "localhost", 31010, new String[]{ "@dremio", "bracket[]tbl"}, "@dremio", "bracket[]tbl"},
      new Object[]{"carratTbl", "localhost", 31010, new String[]{ "@dremio", "carrat^tbl"}, "@dremio", "carrat^tbl"},
      new Object[]{"commaTbl", "localhost", 31010, new String[]{ "@dremio", "comma,tbl"}, "@dremio", "comma,tbl"},
      new Object[]{"curlyBraceTbl", "localhost", 31010, new String[]{ "@dremio", "curlybrace{}tbl"}, "@dremio", "curlybrace{}tbl" },
      new Object[]{"dollarTbl", "localhost", 31010, new String[]{ "@dremio", "dollar$tbl"}, "@dremio", "dollar$tbl"},
      new Object[]{"equalsTbl", "localhost", 31010, new String[]{ "@dremio", "equals=tbl"}, "@dremio", "equals=tbl"},
      new Object[]{"excalmationTbl", "localhost", 31010, new String[]{ "@dremio", "exclamation_tbl!"}, "@dremio", "exclamation_tbl!"},
      new Object[]{"hyphenTbl", "localhost", 31010, new String[]{ "@dremio", "hyphen-tbl"}, "@dremio", "hyphen-tbl"},
      new Object[]{"parenthesesTbl", "localhost", 31010, new String[]{ "@dremio", "parentheses()tbl"}, "@dremio", "parentheses()tbl"},
      new Object[]{"periodTbl", "localhost", 31010, new String[]{ "@dremio", "period.tbl"}, "@dremio", "period.tbl"},
      new Object[]{"pipeTbl", "localhost", 31010, new String[]{ "@dremio", "pipe|tbl"}, "@dremio", "pipe|tbl"},
      new Object[]{"plusTbl", "localhost", 31010, new String[]{ "@dremio", "plus+tbl"}, "@dremio", "plus+tbl"},
      new Object[]{"questionTbl", "localhost", 31010, new String[]{ "@dremio", "question?tbl"}, "@dremio", "question?tbl"},
      new Object[]{"quoteTbl", "localhost", 31010, new String[]{ "@dremio", "quote'tbl"}, "@dremio", "quote'tbl"},
      new Object[]{"slashTbl", "localhost", 31010, new String[]{ "@dremio", "slash/tbl"}, "@dremio", "slash/tbl"},
      new Object[]{"tildeTbl", "localhost", 31010, new String[]{ "@dremio", "tilde~tbl"}, "@dremio", "tilde~tbl"},
      new Object[]{"hashTbl", "localhost", 31010, new String[]{ "@dremio", "hash#tbl"}, "@dremio", "hash#tbl"},
      new Object[]{"colonTbl", "localhost", 31010, new String[]{ "@dremio", "colon:tbl"}, "@dremio", "colon:tbl"},
      new Object[]{"semiColonTbl", "localhost", 31010, new String[]{ "@dremio", "semi-colon;tbl"}, "@dremio", "semi-colon;tbl"},
      new Object[]{"umlautTbl", "localhost", 31010, new String[]{ "@dremio", "umlautütbl"}, "@dremio", "umlautütbl"},
      new Object[]{"number1Tbl", "localhost", 31010, new String[]{ "@dremio", "number1tbl"}, "@dremio", "number1tbl"}
    };
  }

  @Mock
  protected OptionManager mockOptionManager;

  @Mock
  protected Configuration mockConfiguration;

  protected final String server;
  protected final int port;
  protected final String expectedSchema;
  protected final String expectedTable;
  protected final DatasetConfig datasetConfig;
  private final NodeEndpoint endpoint;

  public TestPowerBIMessageBodyGenerator(String testName, String server, int port,
                                         String[] datasetPathComponents,
                                         String expectedSchema, String expectedTable) {
    this.server = server;
    this.port = port;
    this.expectedSchema = expectedSchema;
    this.expectedTable = expectedTable;
    this.datasetConfig = new DatasetConfig();
    this.datasetConfig.setFullPathList(Arrays.asList(datasetPathComponents));
    this.endpoint = NodeEndpoint.newBuilder().setAddress(server).setUserPort(port).build();
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void verifyDSRFile() {
    verifyDSRFileContents();
  }

  protected void verifyDSRFileContents() {
    final PowerBIMessageBodyGenerator powerBIMessageBodyGenerator = new PowerBIMessageBodyGenerator(mockConfiguration,
      endpoint,
      mockOptionManager);
    verifyDSRFileAttributes(powerBIMessageBodyGenerator);
  }

  private String serverPort(String server, int port) {
    return String.format("%s:%d",server, port);
  }

  protected void verifyDSRFileAttributes(PowerBIMessageBodyGenerator powerBIMessageBodyGenerator) {
    final PowerBIMessageBodyGenerator.DSRFile dsrFile =
      powerBIMessageBodyGenerator.createDSRFile(server, datasetConfig);

    assertEquals("0.1", dsrFile.getVersion());

    final PowerBIMessageBodyGenerator.Connection connection =
      dsrFile.getConnections()[0];

    assertEquals("DirectQuery", connection.getMode());

    final PowerBIMessageBodyGenerator.DataSourceReference details = connection.getDSR();
    final PowerBIMessageBodyGenerator.DSRConnectionInfo address = details.getAddress();

    assertEquals(expectedSchema, address.getSchema());
    assertEquals(expectedTable, address.getTable());
    assertEquals(serverPort(server, port), address.getServer());

    verifyExtraDSRFileAttributes(address, details);
  }

  protected void verifyExtraDSRFileAttributes(PowerBIMessageBodyGenerator.DSRConnectionInfo address, PowerBIMessageBodyGenerator.DataSourceReference details) {
    assertEquals("dremio", details.getProtocol());
  }
}
