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
package com.dremio.xml;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collections;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import com.dremio.test.DremioTest;

/**
 * Tests for {@code SafeXMLFactories}
 */
@RunWith(Parameterized.class)
public class TestSafeXMLFactories extends DremioTest {
  private static final String EXTERNAL_ENTITY_PLACEHOLDER = "@@external@@";
  private static final String VULNERABLE_MARKER = "VULNERABLE";

  enum Result {
    SUCCESS,
    FAILURE,
    FAILURE_OR_DTD_NOT_PARSED
  }

  private static final String VALID_XML = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
      + "        <foo>no xxe</foo>\n"
      + "";

  private static final String VULNERABLE_XML = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
      + "    <!DOCTYPE foo [ <!ELEMENT foo ANY >\n"
      + "        <!ENTITY % xxe SYSTEM \"" + EXTERNAL_ENTITY_PLACEHOLDER + "\" >]>\n"
      + "        <foo>&xxe;</foo>\n"
      + "";

  private static final String VULNERABLE_XML_2 = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
      + "    <!DOCTYPE foo [ <!ELEMENT foo ANY >\n"
      + "        <!ENTITY % xxe SYSTEM \"" + EXTERNAL_ENTITY_PLACEHOLDER + "\"> %xxe; ]>\n"
      + "        <foo></foo>\n"
      + "";

  private final Result expected;
  private final String xml;


  @Parameters(name = "{index}: {0}")
  public static final Iterable<Object[]> testCases() throws IOException {
    // Cannot use TemporaryFolder as this method is invoked before the test is run
    final Path externalEntityPath = Files.createTempFile("", "");
    Files.write(externalEntityPath, Arrays.asList(VULNERABLE_MARKER), UTF_8);
    Files.setPosixFilePermissions(externalEntityPath, Collections.singleton(PosixFilePermission.OWNER_WRITE));

    return Arrays.<Object[]>asList(
        new Object[] {"valid xml", Result.SUCCESS, VALID_XML },
        new Object[] {"external entity", Result.FAILURE, VULNERABLE_XML.replace(EXTERNAL_ENTITY_PLACEHOLDER, externalEntityPath.toUri().toString()) },
        new Object[] {"external entity in doctype", Result.FAILURE_OR_DTD_NOT_PARSED, VULNERABLE_XML_2.replace(EXTERNAL_ENTITY_PLACEHOLDER, externalEntityPath.toUri().toString()) }
        );
  }

  public TestSafeXMLFactories(String caseName, Result expected, String xml) {
    this.expected = expected;
    this.xml = xml;
  }


  @Test
  public void testSAXParser() throws Exception {
    SAXParserFactory saxParserFactory = SafeXMLFactories.newSafeSAXParserFactory();
    SAXParser saxParser = saxParserFactory.newSAXParser();

    // Setting the exception post factory instantiation
    switch (expected) {
    case FAILURE:
    case FAILURE_OR_DTD_NOT_PARSED:
      thrownException.expect(SAXParseException.class);

    default:
    }

    saxParser.parse(new InputSource(new StringReader(xml)), new DefaultHandler());
  }

  @Test
  public void testSTaxParser() throws Exception {
    XMLInputFactory xmlInputFactory = SafeXMLFactories.newSafeXMLInputFactory();
    XMLEventReader eventReader = xmlInputFactory.createXMLEventReader(new StringReader(xml));

    // Setting the exception post factory instantiation
    if (expected == Result.FAILURE) {
      thrownException.expect(XMLStreamException.class);
    }

    while (eventReader.hasNext()) {
      XMLEvent event = eventReader.nextEvent();
      assertThat(event.toString(), not(containsString(VULNERABLE_MARKER)));
    }
  }

  @Test
  public void testDocumentBuilderFactory() throws Exception {
    DocumentBuilderFactory documentBuilderFactory = SafeXMLFactories.newSafeDocumentBuilderFactory();
    DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();

    // Setting the exception post factory instantiation
    switch (expected) {
    case FAILURE:
    case FAILURE_OR_DTD_NOT_PARSED:
      thrownException.expect(SAXParseException.class);

    default:
    }

    documentBuilder.parse(new InputSource(new StringReader(xml)));
  }

  @Test
  public void testTransformerFactory() throws Exception {
    TransformerFactory transformerFactory = SafeXMLFactories.newSafeTransformerFactory();
    Transformer transformer = transformerFactory.newTransformer();

    // Setting the exception post factory instantiation
    switch (expected) {
    case FAILURE:
    case FAILURE_OR_DTD_NOT_PARSED:
      thrownException.expect(TransformerException.class);

    default:
    }

    transformer.transform(new StreamSource(new StringReader(xml)), new StreamResult(new StringWriter()));
  }
}
