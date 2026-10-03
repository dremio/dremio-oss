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
package com.dremio.plugins.icebergcatalog.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dremio.exec.catalog.conf.SourceType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.protostuff.Tag;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Consistency checks for {@code restcatalog-layout.json}, the UI form layout referenced by
 * {@code @SourceType(uiConfig = ...)} on {@link RestIcebergCatalogPluginConfig}.
 */
public class TestRestCatalogLayout {

  private static final String CONFIG_PREFIX = "config.";

  private static String layoutResourceName;
  private static String layoutText;
  private static JsonNode layout;

  @BeforeClass
  public static void loadLayout() throws IOException {
    SourceType sourceType = RestIcebergCatalogPluginConfig.class.getAnnotation(SourceType.class);
    assertNotNull("RestIcebergCatalogPluginConfig must be annotated with @SourceType", sourceType);
    layoutResourceName = sourceType.uiConfig();
    assertEquals("restcatalog-layout.json", layoutResourceName);

    URL url = RestIcebergCatalogPluginConfig.class.getClassLoader().getResource(layoutResourceName);
    assertNotNull("layout not found on classpath: " + layoutResourceName, url);
    try (InputStream in = url.openStream()) {
      layoutText = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    layout = new ObjectMapper().readTree(layoutText);
  }

  /** Collects every value of the given key found anywhere in the tree. */
  private static void collect(JsonNode node, String key, List<JsonNode> found) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      JsonNode value = node.get(key);
      if (value != null) {
        found.add(value);
      }
      Iterator<JsonNode> children = node.elements();
      while (children.hasNext()) {
        collect(children.next(), key, found);
      }
    } else if (node.isArray()) {
      for (JsonNode child : node) {
        collect(child, key, found);
      }
    }
  }

  /** Collects every element object (a node carrying "propName") in the tree. */
  private static void collectElements(JsonNode node, List<JsonNode> found) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      if (node.has("propName")) {
        found.add(node);
      }
      Iterator<JsonNode> children = node.elements();
      while (children.hasNext()) {
        collectElements(children.next(), found);
      }
    } else if (node.isArray()) {
      for (JsonNode child : node) {
        collectElements(child, found);
      }
    }
  }

  private static List<JsonNode> elements() {
    List<JsonNode> elements = new ArrayList<>();
    collectElements(layout, elements);
    return elements;
  }

  private static JsonNode elementFor(String propName) {
    for (JsonNode element : elements()) {
      if (propName.equals(element.get("propName").asText())) {
        return element;
      }
    }
    return null;
  }

  /** Field names of all {@code @Tag}-ged fields of the config class and its superclasses. */
  private static Set<String> taggedFieldNames() {
    Set<String> names = new TreeSet<>();
    for (Class<?> c = RestIcebergCatalogPluginConfig.class; c != null; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        if (field.isAnnotationPresent(Tag.class)) {
          names.add(field.getName());
        }
      }
    }
    return names;
  }

  /** "config.allowedNamespaces[]" -> "allowedNamespaces". */
  private static String toFieldName(String propName) {
    String name = propName.substring(CONFIG_PREFIX.length());
    if (name.endsWith("[]")) {
      name = name.substring(0, name.length() - 2);
    }
    return name;
  }

  private static Set<String> configPropNames() {
    Set<String> propNames = new LinkedHashSet<>();
    for (JsonNode element : elements()) {
      String propName = element.get("propName").asText();
      if (propName.startsWith(CONFIG_PREFIX)) {
        propNames.add(propName);
      }
    }
    return propNames;
  }

  @Test
  public void testSourceTypeMatchesAnnotation() {
    assertTrue(layout.isObject());
    JsonNode sourceType = layout.get("sourceType");
    assertNotNull("layout must declare sourceType", sourceType);
    assertEquals(
        RestIcebergCatalogPluginConfig.class.getAnnotation(SourceType.class).value(),
        sourceType.asText());
    assertEquals("RESTCATALOG", sourceType.asText());
  }

  @Test
  public void testMetadataRefreshIsPresent() {
    // The add/edit source modals dereference metadataRefresh; a missing block breaks the UI.
    JsonNode metadataRefresh = layout.get("metadataRefresh");
    assertNotNull("layout must declare metadataRefresh", metadataRefresh);
    assertTrue(metadataRefresh.isObject());
    assertTrue(metadataRefresh.has("datasetDiscovery"));
    assertTrue(metadataRefresh.get("datasetDiscovery").isBoolean());
    assertTrue(metadataRefresh.get("datasetDiscovery").asBoolean());
    assertTrue(metadataRefresh.has("authorization"));
    assertFalse(metadataRefresh.get("authorization").asBoolean());
  }

  @Test
  public void testTabsIncludeGeneralAndAdvancedOptions() {
    JsonNode tabs = layout.path("form").path("tabs");
    assertTrue("form.tabs must be an array", tabs.isArray());
    boolean hasGeneral = false;
    boolean hasAdvanced = false;
    for (JsonNode tab : tabs) {
      String name = tab.path("name").asText();
      if ("General".equals(name)) {
        hasGeneral = true;
        assertTrue("General tab must be isGeneral", tab.path("isGeneral").asBoolean(false));
      }
      // addAlwaysPresent() looks this tab up by its exact name.
      if ("Advanced Options".equals(name)) {
        hasAdvanced = true;
      }
    }
    assertTrue("missing General tab", hasGeneral);
    assertTrue("missing tab named exactly 'Advanced Options'", hasAdvanced);
  }

  @Test
  public void testEveryConfigPropNameMatchesATaggedField() {
    Set<String> tagged = taggedFieldNames();
    Set<String> propNames = configPropNames();
    assertFalse("layout has no config.* elements", propNames.isEmpty());
    for (String propName : propNames) {
      assertTrue(
          "layout propName "
              + propName
              + " has no @Tag field in RestIcebergCatalogPluginConfig "
              + tagged,
          tagged.contains(toFieldName(propName)));
    }
  }

  @Test
  public void testEveryTaggedFieldIsInLayout() {
    Set<String> inLayout = new TreeSet<>();
    for (String propName : configPropNames()) {
      inLayout.add(toFieldName(propName));
    }
    assertEquals(taggedFieldNames(), inLayout);
  }

  @Test
  public void testContractPropNamesPresent() {
    Set<String> propNames = configPropNames();
    String[] expected = {
      "config.restEndpointUri",
      "config.isUsingVendedCredentials",
      "config.allowedNamespaces[]",
      "config.isRecursiveAllowedNamespaces",
      "config.propertyList",
      "config.secretPropertyList",
      "config.enableAsync",
      "config.isCachingEnabled",
      "config.maxCacheSpacePct"
    };
    for (String propName : expected) {
      assertTrue("missing " + propName + " in " + propNames, propNames.contains(propName));
    }
  }

  @Test
  public void testEndpointUriIsRequired() {
    JsonNode element = elementFor("config.restEndpointUri");
    assertNotNull(element);
    assertTrue(
        "restEndpointUri must be validate.isRequired=true",
        element.path("validate").path("isRequired").asBoolean(false));
  }

  @Test
  public void testSecretPropertyListIsSecureAndPropertyListIsNot() {
    JsonNode secret = elementFor("config.secretPropertyList");
    assertNotNull(secret);
    assertTrue("secretPropertyList must be secure", secret.path("secure").asBoolean(false));

    JsonNode plain = elementFor("config.propertyList");
    assertNotNull(plain);
    assertFalse("propertyList must not be secure", plain.path("secure").asBoolean(false));
  }

  @Test
  public void testCheckboxControllersReferenceTaggedFields() {
    List<JsonNode> controllers = new ArrayList<>();
    collect(layout, "checkboxController", controllers);
    Set<String> tagged = taggedFieldNames();
    for (JsonNode controller : controllers) {
      assertTrue(
          "checkboxController " + controller.asText() + " is not a config field",
          tagged.contains(controller.asText()));
    }
  }

  @Test
  public void testNoUnknownPropertyListUiType() {
    // There is no "property_list" uiType in the form DSL; property lists are inferred from the
    // field type.
    List<JsonNode> uiTypes = new ArrayList<>();
    collect(layout, "uiType", uiTypes);
    for (JsonNode uiType : uiTypes) {
      assertFalse("unsupported uiType property_list", "property_list".equals(uiType.asText()));
    }
  }

  @Test
  public void testHelpTextMentionsRequiredPolarisKeys() {
    // Polaris rejects requests without these, so the layout must surface them to users.
    assertTrue("layout should mention 'warehouse'", layoutText.contains("warehouse"));
    assertTrue("layout should mention 'scope'", layoutText.contains("scope"));
    assertTrue(
        "layout should mention 'PRINCIPAL_ROLE:ALL'", layoutText.contains("PRINCIPAL_ROLE:ALL"));
    assertTrue("layout should mention 'credential'", layoutText.contains("credential"));
  }

  @Test
  public void testHelpTextRecommendsS3aRegionForS3CompatibleStorage() {
    // S3A signs with fs.s3a.endpoint.region; dremio.s3.region alone fails against an
    // S3-compatible store outside us-east-1 (storage.md), so the help text must not suggest it as
    // the region setting.
    assertTrue(
        "layout should recommend 'fs.s3a.endpoint.region=<region>'",
        layoutText.contains("fs.s3a.endpoint.region=<region>"));
    assertFalse(
        "layout should not recommend 'dremio.s3.region=<region>'",
        layoutText.contains("dremio.s3.region=<region>"));
  }
}
