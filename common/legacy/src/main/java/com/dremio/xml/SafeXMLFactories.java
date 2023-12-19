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

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

import com.dremio.common.SuppressForbidden;

/**
 * A set of XML factory methods to create XML parsers not vulnerable against XXE
 * (XML eXternal Entity) and DoS attacks.
 *
 * More details can be found in <a href=
 * "https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html">OWASP
 * XML Prevention Cheat Sheet</a>
 *
 */
public final class SafeXMLFactories {
  private static final Logger LOGGER = LoggerFactory.getLogger(SafeXMLFactories.class);

  private SafeXMLFactories() {}

  /**
   * Creates a {@code SAXParserFactory} instance which does not try to access
   * external entities or resolve DTD.
   *
   * @return a new {@code SAXParserFactory} instance
   */
  @SuppressForbidden
  public static SAXParserFactory newSafeSAXParserFactory() {
    final SAXParserFactory factory = SAXParserFactory.newInstance();
    try {
      // Enable secure processing which should be supported by all implementations
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);

      // Set extra properties to disable XXE
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
      factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

      factory.setXIncludeAware(false);
    } catch (ParserConfigurationException | SAXNotSupportedException | SAXNotRecognizedException | IllegalArgumentException e) {
      LOGGER.warn("XML SAX parser factory does not support disabling DTD/External Entities support which might cause some security issue", e);
      return factory;
    }

    return new SafeSAXParserFactory(factory);
  }

  /**
   * Creates a {@code DocumentBuilderFactory} instance which does not try to access
   * external entities or resolve DTD.
   *
   * @return a new {@code DocumentBuilderFactory} instance
   */
  @SuppressForbidden
  public static DocumentBuilderFactory newSafeDocumentBuilderFactory() {
    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    try {
      // Enable secure processing which should be supported by all implementations
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");

      // Set extra properties to disable XXE
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
      factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

      factory.setXIncludeAware(false);
      factory.setExpandEntityReferences(false);
    } catch (ParserConfigurationException  | IllegalArgumentException e) {
      LOGGER.warn("XML DOM parser factory does not support disabling DTD/External Entities support which might cause some security issue", e);
    }

    return factory;
  }

  /**
   * Creates a {@code DocumentBuilderFactory} instance which does not try to access
   * external entities or resolve DTD.
   *
   * @return a new {@code DocumentBuilderFactory} instance
   */
  @SuppressForbidden
  public static TransformerFactory newSafeTransformerFactory() {
    final TransformerFactory factory = TransformerFactory.newInstance();
    try {
      // Enable secure processing which should be supported by all implementations
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
    } catch (TransformerConfigurationException  | IllegalArgumentException e) {
      LOGGER.warn("XML Transformer factory does not support disabling DTD/External Entities support which might cause some security issue", e);
    }

    return factory;
  }

  /**
   * Creates a {@code XMLInputFactory} instance which does not try to access
   * external entities or resolve DTD.
   *
   * @return a new {@code XMLInputFactory} instance
   */
  @SuppressForbidden
  public static XMLInputFactory newSafeXMLInputFactory() {
    final XMLInputFactory factory = XMLInputFactory.newFactory();
    try {
      factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
      factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    } catch (IllegalArgumentException e) {
      LOGGER.warn("XML StaX parser factory does not support disabling DTD/External Entities support which might cause some security issue", e);
    }
    return factory;
  }

  private static final class SafeSAXParserFactory extends SAXParserFactory {
    private final SAXParserFactory factory;

    private SafeSAXParserFactory(SAXParserFactory factory) {
      this.factory = factory;
    }

    @Override
    public void setFeature(String name, boolean value)
        throws ParserConfigurationException, SAXNotRecognizedException, SAXNotSupportedException {
      factory.setFeature(name, value);
    }

    @Override
    public boolean getFeature(String name)
        throws ParserConfigurationException, SAXNotRecognizedException, SAXNotSupportedException {
      return factory.getFeature(name);
    }

    @Override
    public SAXParser newSAXParser() throws ParserConfigurationException, SAXException {
      final SAXParser parser = factory.newSAXParser();
      try {
        // Enable secure processing which should be supported by all implementations
        parser.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        parser.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
      } catch (SAXNotRecognizedException | SAXNotSupportedException | IllegalArgumentException e) {
        LOGGER.warn("XML DOM parser does not support disabling DTD/External Entities support which might cause some security issue", e);
      }

      return parser;
    }
  }
}
