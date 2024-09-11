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

import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.EXTRA_CONNECTION_PROPERTIES;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.EXTRA_FLIGHT_CONNECTION_PROPERTIES;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.EXTRA_NATIVE_CONNECTION_PROPERTIES;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TABLEAU_EXPORT_TYPE;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TABLEAU_VERSION;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.TABLEAU_TYPE_NOMINAL;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.TABLEAU_TYPE_ORDINAL;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.TABLEAU_TYPE_QUANTITATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.dremio.config.DremioConfig;
import com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauExportType;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.WebServer;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.xml.SafeXMLFactories;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.text.ParseException;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.glassfish.jersey.media.multipart.ContentDisposition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/** Unit tests for {@link TableauMessageBodyGenerator} */
@RunWith(Parameterized.class)
public class TestTableauMessageBodyGenerator {
  @Parameters(name = "{0}")
  public static Object[] getTestCases() {
    return new Object[] {
      new String[] {"basic", "UNTITLED.tmp", "[UNTITLED].[tmp]", ""},
      new String[] {"basic-with-custom-properties", "UNTITLED.tmp", "[UNTITLED].[tmp]", "FOO=BAR"},
      new String[] {"subfolder", "spaceA.foo.tmp", "[spaceA.foo].[tmp]", ""},
      new String[] {"dot-in-name", "spaceA.\"tmp.json\"", "[spaceA].[tmp.json]", ""},
      new String[] {"home-dataset", "@dremio.tmp", "[@dremio].[tmp]", ""},
      new String[] {"weird-name", "spaceA.[foo][bar]", "[spaceA].[[foo]][bar]]]", ""},
      new String[] {"weird-schema", "spaceA.[whynot].tmp", "[spaceA.[whynot]]].[tmp]", ""}
    };
  }

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final NodeEndpoint ENDPOINT =
      NodeEndpoint.newBuilder().setAddress("foo").setUserPort(12345).build();
  public static final String NATIVE = "NATIVE";
  public static final String FLIGHT = "FLIGHT";
  public static final String SSL_TRUE = "ssl=true";
  public static final String FLIGHT_SSL_TRUE = "useencryption=true";
  public static final String DISABLE_CERT_VERIFICATION = "disablecertificateverification=true";

  private final DatasetPath path;
  private final String tableName;
  private final String customProperties;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @Mock
  protected Configuration configuration;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @Mock
  protected OptionManager optionManager;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @Mock
  protected DremioConfig config;

  public TestTableauMessageBodyGenerator(
      String testName, String path, String tableName, String customProperties) {
    this.path = new DatasetPath(path);
    this.tableName = tableName;
    this.customProperties = customProperties;
  }

  protected TableauMessageBodyGenerator buildGenerator() {
    return new TableauMessageBodyGenerator(configuration, ENDPOINT, optionManager, config);
  }

  @Test
  public void verifyODBCOutput()
      throws IOException, SAXException, ParserConfigurationException, ParseException {
    when(optionManager.getOption(EXTRA_CONNECTION_PROPERTIES)).thenReturn(customProperties);
    when(optionManager.getOption(TABLEAU_EXPORT_TYPE))
        .thenReturn(TableauExportType.ODBC.toString());
    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setFullPathList(path.toPathList());
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    final BatchSchema schema = generateBatchSchema();
    datasetConfig.setRecordSchema(schema.toByteString());

    final MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    final TableauMessageBodyGenerator generator = buildGenerator();
    assertTrue(
        generator.isWriteable(
            datasetConfig.getClass(), null, null, WebServer.MediaType.APPLICATION_TDS_TYPE));
    generator.writeTo(
        datasetConfig,
        DatasetConfig.class,
        null,
        new Annotation[] {},
        WebServer.MediaType.APPLICATION_TDS_TYPE,
        httpHeaders,
        baos);

    // Convert the baos into a DOM Tree to verify content
    final DocumentBuilderFactory factory = SafeXMLFactories.newSafeDocumentBuilderFactory();
    final Document document =
        factory.newDocumentBuilder().parse(new ByteArrayInputStream(baos.toByteArray()));

    assertEquals(TABLEAU_VERSION, document.getDocumentElement().getAttribute("version"));

    final NodeList connections = document.getDocumentElement().getElementsByTagName("connection");

    assertEquals(1, connections.getLength());
    final Element connection = (Element) connections.item(0);
    assertEquals("genericodbc", connection.getAttribute("class"));
    assertEquals("Dremio Connector", connection.getAttribute("odbc-driver"));

    String expectedExtras =
        "AUTHENTICATIONTYPE=Basic Authentication;CONNECTIONTYPE=Direct;HOST="
            + generator.getEndpoint().getAddress();
    String actualExtras = connection.getAttribute("odbc-connect-string-extras");
    if (customProperties.isEmpty()) {
      assertEquals(expectedExtras, actualExtras);
    } else {
      assertEquals(customProperties + ";" + expectedExtras, actualExtras);
    }
    assertEquals("DREMIO", connection.getAttribute("dbname"));
    assertEquals(path.toParentPath(), connection.getAttribute("schema"));

    verifyRelationElement(connection);

    // test column aliases, column element and attributes
    verifyAliasesElement(document);
    final NodeList columnAliases = document.getDocumentElement().getElementsByTagName("column");
    assertEquals(columnAliases.getLength(), schema.getFieldCount());
    verifyBatchSchema(columnAliases);

    // Also check that Content-Disposition header is set with a filename ending by tds
    final ContentDisposition contentDisposition =
        new ContentDisposition((String) httpHeaders.getFirst(HttpHeaders.CONTENT_DISPOSITION));
    assertTrue("filename should end with .tds", contentDisposition.getFileName().endsWith(".tds"));
  }

  private void verifyRelationElement(Element connection) {
    final NodeList relations = connection.getElementsByTagName("relation");
    assertEquals(1, relations.getLength());
    final Element relation = (Element) relations.item(0);
    assertEquals("table", relation.getAttribute("type"));
    assertEquals(tableName, relation.getAttribute("table"));
  }

  private void verifyAliasesElement(Document document) {
    final NodeList aliases = document.getDocumentElement().getElementsByTagName("aliases");
    assertEquals(1, aliases.getLength());
    final Element alias = (Element) aliases.item(0);
    assertEquals("yes", alias.getAttribute("enabled"));
  }

  @Test
  public void verifySdkOutputSslOff()
      throws IOException, SAXException, ParserConfigurationException, ParseException {
    verifyOutput("", "", NATIVE);
  }

  @Test
  public void verifySdkOutputSslOn()
      throws IOException, SAXException, ParserConfigurationException, ParseException {
    verifyOutput(SSL_TRUE, TableauSDKConstants.REQUIRE, NATIVE);
  }

  @Test
  public void verifySdkOutputSslOnConfig()
      throws IOException, SAXException, ParserConfigurationException, ParseException {
    when(config.hasPath("services.coordinator.client-endpoint.ssl.enabled")).thenReturn(true);
    when(config.getBoolean("services.coordinator.client-endpoint.ssl.enabled")).thenReturn(true);
    verifyOutput(SSL_TRUE, TableauSDKConstants.REQUIRE, NATIVE);
  }

  @Test
  public void verifyFlightOutputSslOff()
      throws IOException, SAXException, ParserConfigurationException, ParseException {
    verifyOutput("", "", FLIGHT);
  }

  @Test
  public void verifyFlightOutputSslOn()
      throws IOException, SAXException, ParserConfigurationException, ParseException {
    verifyOutput(FLIGHT_SSL_TRUE, TableauSDKConstants.REQUIRE, FLIGHT);
  }

  @Test
  public void verifyDisableCertVerificationOutput()
      throws IOException, SAXException, ParserConfigurationException, ParseException {
    when(config.hasPath("services.coordinator.client-endpoint.ssl.enabled")).thenReturn(true);
    when(config.getBoolean("services.coordinator.client-endpoint.ssl.enabled")).thenReturn(true);
    verifyOutput(DISABLE_CERT_VERIFICATION, TableauSDKConstants.REQUIRE, FLIGHT);
  }

  @Test
  public void verifyFlightOutputSslOnConfig()
      throws IOException, SAXException, ParserConfigurationException, ParseException {
    when(config.hasPath("services.coordinator.client-endpoint.ssl.enabled")).thenReturn(true);
    when(config.getBoolean("services.coordinator.client-endpoint.ssl.enabled")).thenReturn(true);
    verifyOutput("", TableauSDKConstants.REQUIRE, FLIGHT);
  }

  protected void verifySdkCustomOutput(Element connection) {
    assertEquals("", connection.getAttribute(TableauSDKConstants.QUEUE));
    assertEquals("", connection.getAttribute(TableauSDKConstants.ENGINE));
    assertEquals("", connection.getAttribute(TableauSDKConstants.TAG));
  }

  protected void verifySdkAuthenticationOutput(Element connection) {
    assertEquals(
        TableauSDKConstants.BASIC, connection.getAttribute(TableauSDKConstants.AUTHENTICATION));
  }

  protected void verifySdkSSLOutput(Element connection, String sslmode) {
    assertEquals(sslmode, connection.getAttribute(TableauSDKConstants.SSL));
  }

  protected void verifySdkProduct(Element connection) {
    assertEquals(
        TableauSDKConstants.SOFTWARE, connection.getAttribute(TableauSDKConstants.PRODUCT));
  }

  protected void verifyFlightDriver(Element connection) {
    assertEquals("true", connection.getAttribute(TableauSDKConstants.USE_FLIGHT_DRIVER));
  }

  protected void verifyCertVerification(Element connection) {
    assertEquals("", connection.getAttribute(TableauSDKConstants.DISABLE_CERT_VERIFICATION));
  }

  protected void verifyDisableCertVerification(Element connection) {
    assertEquals("true", connection.getAttribute(TableauSDKConstants.DISABLE_CERT_VERIFICATION));
  }

  protected Element verifyOutput(String properties, String sslmode, String tableauExportType)
      throws IOException, SAXException, ParserConfigurationException, ParseException {
    when(optionManager.getOption(EXTRA_FLIGHT_CONNECTION_PROPERTIES)).thenReturn(properties);
    when(optionManager.getOption(EXTRA_NATIVE_CONNECTION_PROPERTIES)).thenReturn(properties);
    when(optionManager.getOption(TABLEAU_EXPORT_TYPE)).thenReturn(tableauExportType);
    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setFullPathList(path.toPathList());
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    final BatchSchema schema = generateBatchSchema();
    datasetConfig.setRecordSchema(schema.toByteString());

    final MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    final TableauMessageBodyGenerator tableauMessageBodyGenerator = buildGenerator();
    assertTrue(
        tableauMessageBodyGenerator.isWriteable(
            datasetConfig.getClass(), null, null, WebServer.MediaType.APPLICATION_TDS_TYPE));
    tableauMessageBodyGenerator.writeTo(
        datasetConfig,
        DatasetConfig.class,
        null,
        new Annotation[] {},
        WebServer.MediaType.APPLICATION_TDS_TYPE,
        httpHeaders,
        baos);

    // Convert the baos into a DOM Tree to verify content
    final DocumentBuilderFactory factory = SafeXMLFactories.newSafeDocumentBuilderFactory();
    final Document document =
        factory.newDocumentBuilder().parse(new ByteArrayInputStream(baos.toByteArray()));

    assertEquals(TABLEAU_VERSION, document.getDocumentElement().getAttribute("version"));

    final NodeList connections =
        document.getDocumentElement().getElementsByTagName(TableauSDKConstants.CONN_ATTR);

    assertEquals(1, connections.getLength());
    final Element connection = (Element) connections.item(0);
    assertEquals("dremio", connection.getAttribute(TableauSDKConstants.CLASS));
    assertEquals("DREMIO", connection.getAttribute(TableauSDKConstants.DBNAME));
    assertEquals(path.toParentPath(), connection.getAttribute(TableauSDKConstants.SCHEMA));

    verifySdkAuthenticationOutput(connection);
    verifySdkSSLOutput(connection, sslmode);
    verifySdkCustomOutput(connection);
    verifySdkProduct(connection);
    if (FLIGHT.equals(tableauExportType)) {
      verifyFlightDriver(connection);
    }
    if (properties.contains("disablecertificateverification")) {
      verifyDisableCertVerification(connection);
    } else {
      verifyCertVerification(connection);
    }

    verifyRelationElement(connection);

    // test column aliases, column element and attributes
    verifyAliasesElement(document);
    final NodeList columnAliases = document.getDocumentElement().getElementsByTagName("column");
    assertEquals(columnAliases.getLength(), schema.getFieldCount());
    verifyBatchSchema(columnAliases);

    // Also check that Content-Disposition header is set with a filename ending by tds
    final ContentDisposition contentDisposition =
        new ContentDisposition((String) httpHeaders.getFirst(HttpHeaders.CONTENT_DISPOSITION));
    assertTrue("filename should end with .tds", contentDisposition.getFileName().endsWith(".tds"));

    return connection;
  }

  @Test
  public void verifyNativeOutput()
      throws IOException, SAXException, ParserConfigurationException, ParseException {
    when(optionManager.getOption(TABLEAU_EXPORT_TYPE))
        .thenReturn(TableauExportType.ODBC.toString());
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setFullPathList(path.toPathList());

    // create a schema to test the metadata output for native connectors
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    final BatchSchema schema = generateBatchSchema();
    datasetConfig.setRecordSchema(schema.toByteString());

    final MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    final TableauMessageBodyGenerator generator = buildGenerator();
    assertTrue(
        generator.isWriteable(
            datasetConfig.getClass(), null, null, WebServer.MediaType.APPLICATION_TDS_DRILL_TYPE));
    generator.writeTo(
        datasetConfig,
        DatasetConfig.class,
        null,
        new Annotation[] {},
        WebServer.MediaType.APPLICATION_TDS_DRILL_TYPE,
        httpHeaders,
        baos);

    // Convert the baos into a DOM Tree to verify content
    final DocumentBuilderFactory factory = SafeXMLFactories.newSafeDocumentBuilderFactory();
    final Document document =
        factory.newDocumentBuilder().parse(new ByteArrayInputStream(baos.toByteArray()));

    final NodeList connections = document.getDocumentElement().getElementsByTagName("connection");

    assertEquals(1, connections.getLength());
    final Element connection = (Element) connections.item(0);

    assertEquals("drill", connection.getAttribute("class"));
    assertEquals("Direct", connection.getAttribute("connection-type"));
    assertEquals(generator.getEndpoint().getAddress(), connection.getAttribute("server"));
    assertEquals(
        generator.getEndpoint().getUserPort(), Integer.parseInt(connection.getAttribute("port")));
    assertEquals(path.toParentPath(), connection.getAttribute("schema"));

    verifyRelationElement(connection);
    // test column aliases, column element and attributes
    verifyAliasesElement(document);
    final NodeList columnAliases = document.getDocumentElement().getElementsByTagName("column");
    assertEquals(columnAliases.getLength(), schema.getFieldCount());
    verifyBatchSchema(columnAliases);

    // metadata tests
    final NodeList metadataRecords =
        document.getDocumentElement().getElementsByTagName("metadata-record");

    assertEquals(metadataRecords.getLength(), schema.getFieldCount());
    assertEqualsMetadataRecord(metadataRecords.item(0), "[col_string]", "string");
    assertEqualsMetadataRecord(metadataRecords.item(1), "[BOOLEAN]", "boolean");
    assertEqualsMetadataRecord(metadataRecords.item(2), "[Col_decimal]", "real");
    assertEqualsMetadataRecord(metadataRecords.item(3), "[col_INT]", "integer");
    assertEqualsMetadataRecord(metadataRecords.item(4), "[COL_DATE]", "date");
    assertEqualsMetadataRecord(metadataRecords.item(5), "[COL_Time]", "datetime");
    assertEqualsMetadataRecord(metadataRecords.item(6), "[id_int]", "integer");
    assertEqualsMetadataRecord(metadataRecords.item(7), "[Code_int]", "integer");
    assertEqualsMetadataRecord(metadataRecords.item(8), "[KEY_int]", "integer");
    assertEqualsMetadataRecord(metadataRecords.item(9), "[float_id]", "real");
    assertEqualsMetadataRecord(metadataRecords.item(10), "[float_Code]", "real");
    assertEqualsMetadataRecord(metadataRecords.item(11), "[float_KEY]", "real");
    assertEqualsMetadataRecord(metadataRecords.item(12), "[decimal_number]", "real");
    assertEqualsMetadataRecord(metadataRecords.item(13), "[decimal_Num]", "real");
    assertEqualsMetadataRecord(metadataRecords.item(14), "[decimal_NBR]", "real");

    // Also check that Content-Disposition header is set with a filename ending by tds
    final ContentDisposition contentDisposition =
        new ContentDisposition((String) httpHeaders.getFirst(HttpHeaders.CONTENT_DISPOSITION));
    assertTrue("filename should end with .tds", contentDisposition.getFileName().endsWith(".tds"));
  }

  private BatchSchema generateBatchSchema() {
    // create a schema to test the column element with various attributes
    return BatchSchema.newBuilder()
        .addField(new Field("col_string", FieldType.nullable(ArrowType.Utf8.INSTANCE), null))
        .addField(new Field("BOOLEAN", FieldType.nullable(ArrowType.Bool.INSTANCE), null))
        .addField(
            new Field("Col_decimal", FieldType.nullable(new ArrowType.Decimal(0, 0, 128)), null))
        .addField(new Field("col_INT", FieldType.nullable(new ArrowType.Int(8, false)), null))
        .addField(
            new Field(
                "COL_DATE", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null))
        .addField(
            new Field(
                "COL_Time", FieldType.nullable(new ArrowType.Time(TimeUnit.MILLISECOND, 8)), null))
        .addField(new Field("id_int", FieldType.nullable(new ArrowType.Int(8, false)), null))
        .addField(new Field("Code_int", FieldType.nullable(new ArrowType.Int(8, false)), null))
        .addField(new Field("KEY_int", FieldType.nullable(new ArrowType.Int(8, false)), null))
        .addField(
            new Field(
                "float_id",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                null))
        .addField(
            new Field(
                "float_Code",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                null))
        .addField(
            new Field(
                "float_KEY",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                null))
        .addField(
            new Field("decimal_number", FieldType.nullable(new ArrowType.Decimal(0, 0, 128)), null))
        .addField(
            new Field("decimal_Num", FieldType.nullable(new ArrowType.Decimal(0, 0, 128)), null))
        .addField(
            new Field("decimal_NBR", FieldType.nullable(new ArrowType.Decimal(0, 0, 128)), null))
        .build();
  }

  private void verifyBatchSchema(NodeList columnAliases) {
    assertEqualsColumnAlias(
        columnAliases.item(0),
        "Col String",
        "string",
        "[col_string]",
        "dimension",
        TABLEAU_TYPE_NOMINAL);
    assertEqualsColumnAlias(
        columnAliases.item(1),
        "Boolean",
        "boolean",
        "[BOOLEAN]",
        "dimension",
        TABLEAU_TYPE_NOMINAL);
    assertEqualsColumnAlias(
        columnAliases.item(2),
        "Col Decimal",
        "real",
        "[Col_decimal]",
        "measure",
        TABLEAU_TYPE_QUANTITATIVE);
    assertEqualsColumnAlias(
        columnAliases.item(3),
        "Col Int",
        "integer",
        "[col_INT]",
        "measure",
        TABLEAU_TYPE_QUANTITATIVE);
    assertEqualsColumnAlias(
        columnAliases.item(4), "Col Date", "date", "[COL_DATE]", "dimension", TABLEAU_TYPE_NOMINAL);
    assertEqualsColumnAlias(
        columnAliases.item(5),
        "Col Time",
        "datetime",
        "[COL_Time]",
        "dimension",
        TABLEAU_TYPE_NOMINAL);
    assertEqualsColumnAlias(
        columnAliases.item(6), "Id Int", "integer", "[id_int]", "dimension", TABLEAU_TYPE_ORDINAL);
    assertEqualsColumnAlias(
        columnAliases.item(7),
        "Code Int",
        "integer",
        "[Code_int]",
        "dimension",
        TABLEAU_TYPE_ORDINAL);
    assertEqualsColumnAlias(
        columnAliases.item(8),
        "Key Int",
        "integer",
        "[KEY_int]",
        "dimension",
        TABLEAU_TYPE_ORDINAL);
    assertEqualsColumnAlias(
        columnAliases.item(9), "Float Id", "real", "[float_id]", "dimension", TABLEAU_TYPE_ORDINAL);
    assertEqualsColumnAlias(
        columnAliases.item(10),
        "Float Code",
        "real",
        "[float_Code]",
        "dimension",
        TABLEAU_TYPE_ORDINAL);
    assertEqualsColumnAlias(
        columnAliases.item(11),
        "Float Key",
        "real",
        "[float_KEY]",
        "dimension",
        TABLEAU_TYPE_ORDINAL);
    assertEqualsColumnAlias(
        columnAliases.item(12),
        "Decimal Number",
        "real",
        "[decimal_number]",
        "dimension",
        TABLEAU_TYPE_ORDINAL);
    assertEqualsColumnAlias(
        columnAliases.item(13),
        "Decimal Num",
        "real",
        "[decimal_Num]",
        "dimension",
        TABLEAU_TYPE_ORDINAL);
    assertEqualsColumnAlias(
        columnAliases.item(14),
        "Decimal Nbr",
        "real",
        "[decimal_NBR]",
        "dimension",
        TABLEAU_TYPE_ORDINAL);
  }

  private void assertEqualsMetadataRecord(Node node, String fieldName, String fieldType) {
    Node child = node.getChildNodes().item(0);
    assertEquals(child.getNodeName(), "local-name");
    assertEquals(child.getTextContent(), fieldName);

    child = node.getChildNodes().item(1);
    assertEquals(child.getNodeName(), "local-type");
    assertEquals(child.getTextContent(), fieldType);
  }

  private void assertEqualsColumnAlias(
      Node node, String caption, String dataType, String name, String role, String type) {
    Node attribute = node.getAttributes().item(0);
    assertEquals(attribute.getNodeName(), "caption");
    assertEquals(attribute.getTextContent(), caption);

    attribute = node.getAttributes().item(1);
    assertEquals(attribute.getNodeName(), "datatype");
    assertEquals(attribute.getTextContent(), dataType);

    attribute = node.getAttributes().item(2);
    assertEquals(attribute.getNodeName(), "name");
    assertEquals(attribute.getTextContent(), name);

    attribute = node.getAttributes().item(3);
    assertEquals(attribute.getNodeName(), "role");
    assertEquals(attribute.getTextContent(), role);

    attribute = node.getAttributes().item(4);
    assertEquals(attribute.getNodeName(), "type");
    assertEquals(attribute.getTextContent(), type);
  }
}
