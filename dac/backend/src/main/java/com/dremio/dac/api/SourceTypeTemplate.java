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
package com.dremio.dac.api;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.catalog.conf.DoNotDisplay;
import com.dremio.exec.catalog.conf.SourceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.google.common.collect.Iterables;

import io.protostuff.Tag;

/**
 * Source type
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class SourceTypeTemplate {
  private static final Logger logger = LoggerFactory.getLogger(SourceTypeTemplate.class);

  private final String sourceType;
  private final String label;
  private final String icon;
  private final boolean externalQueryAllowed;
  private final List<SourcePropertyTemplate> elements;
  private final String uiConfig;
  private final boolean previewEngineRequired;

  public SourceTypeTemplate(
      String name,
      String label,
      String icon,
      boolean externalQueryAllowed,
      List<SourcePropertyTemplate> elements,
      String uiConfig,
      boolean previewEngineRequired){
    this.sourceType = name;
    this.label = label;
    this.icon = icon;
    this.externalQueryAllowed = externalQueryAllowed;
    this.elements = elements;
    this.uiConfig = uiConfig;
    this.previewEngineRequired = previewEngineRequired;
  }

  @JsonCreator
  public SourceTypeTemplate(
    @JsonProperty("sourceType") String name,
    @JsonProperty("label") String label,
    @JsonProperty("icon") String icon,
    @JsonProperty("arpSource") boolean externalQueryAllowed,
    @JsonProperty("elements") List<SourcePropertyTemplate> elements,
    @JsonProperty("previewEngineRequired") boolean previewEngineRequired) {
    this.sourceType = name;
    this.label = label;
    this.icon = icon;
    this.externalQueryAllowed = externalQueryAllowed;
    this.elements = elements;
    this.previewEngineRequired = previewEngineRequired;
    this.uiConfig = null;
  }

  public String getSourceType() {
    return sourceType;
  }

  public String getIcon() {
    return icon;
  }

  public String getLabel() {
    return label;
  }

  public boolean isExternalQueryAllowed() {
    return externalQueryAllowed;
  }

  public List<SourcePropertyTemplate> getElements() {
    return elements;
  }

  public boolean isPreviewEngineRequired() {
    return previewEngineRequired;
  }

  @JsonRawValue
  @JsonProperty("uiConfig")
  public String getUIConfig() {
    return uiConfig;
  }

  public static SourceTypeTemplate fromSourceClass(Class<?> sourceClass, boolean includeProperties) {
    final SourceType type = sourceClass.getAnnotation(SourceType.class);
    final boolean supportsExternalQuery = type != null && type.externalQuerySupported();
    final boolean previewEngineRequired = type != null && type.previewEngineRequired();


    // source icon has to be SourceTypeTemplate.svg and provided as a resource
    String icon = null;
    final URL resource = sourceClass.getClassLoader().getResource(type.value() + ".svg");

    if (resource != null) {
      try {
        icon = IOUtils.toString(resource);
      } catch (IOException e) {
        logger.warn("Failed to load resource [{}]", resource, e);
      }
    }

    // source uiConfig layout provided as a resource file
    String uiLayoutConfig = null;
    if (!StringUtils.isEmpty(type.uiConfig())) {
      final InputStream inputStream = sourceClass.getClassLoader().getResourceAsStream(type.uiConfig());
      if (inputStream == null) {
        logger.warn("Failed to load ui config file [{}]", type.uiConfig());
      } else {
        try {
          uiLayoutConfig = IOUtils.toString(inputStream);
        } catch (IOException e) {
          logger.warn("Failed to read UI Layout Configuration for [{}]", sourceClass.getName(), e);
        }
      }
    }

    if (!includeProperties) {
      return new SourceTypeTemplate(type.value(),
        type.label(),
        icon,
        supportsExternalQuery,
        null,
        null,previewEngineRequired);
    }

    final Object newClassInstance;
    try {
      newClassInstance = sourceClass.getConstructor().newInstance();
    } catch (Exception e) {
      logger.warn("Failed to create new instance of [{}]", sourceClass.getName(), e);
      return new SourceTypeTemplate(type.value(),
        type.label(),
        icon,
        supportsExternalQuery,
        null,
        null,previewEngineRequired);
    }

    final List<SourcePropertyTemplate> properties = new ArrayList<>();

    for (Field field : getAllFields(sourceClass)) {
      // only properties with the Tag annotation will be included
      if (field.getAnnotation(Tag.class) == null || field.getAnnotation(JsonIgnore.class) != null) {
        continue;
      }

      if (field.getAnnotation(DoNotDisplay.class) != null) {
        // fields which are annotated as DoNotDisplay - should not be shown in UI.
        continue;
      }

      final Class<?> fieldType = field.getType();

      final Object definedValue;
      try {
        definedValue = field.get(newClassInstance);
      } catch (IllegalAccessException e) {
        logger.warn("Failed to access the field [{}] of an instance of class [{}]", field.getName(), sourceClass.getName(), e);
        continue;
      }

      Object defaultValue = null;

      // If we have a primitive numeric type, it will always default to 0 even if no actual value is set.
      // In this case, since we can't distinguish between the two, we opt to skip setting the default as 0.  For other
      // primitives (boolean, etc) we let the default value pass through even if its not explicitly set.
      if (!ClassUtils.isPrimitiveWrapper(fieldType) &&
        Number.class.isAssignableFrom(ClassUtils.primitiveToWrapper(fieldType))) {
        final Number num = (Number) definedValue;
        if (num.intValue() != 0) {
          defaultValue = definedValue;
        }
      } else {
        // skip empty lists
        if (definedValue != null && Collection.class.isAssignableFrom(definedValue.getClass())) {
          final Collection<?> collection = (Collection<?>) definedValue;
          if (collection.size() > 0) {
            defaultValue = definedValue;
          }
        } else {
          defaultValue = definedValue;
        }
      }


      try {
        properties.add(SourcePropertyTemplate.fromField(field, defaultValue));
      } catch (Exception e) {
        logger.warn("Failed to add field [{}]", field.getName(), e);
      }
    }

    return new SourceTypeTemplate(type.value(),
      type.label(),
      icon,
      supportsExternalQuery,
      properties,
      uiLayoutConfig,previewEngineRequired);
  }

  private static Iterable<Field> getAllFields(Class<?> clazz) {
    List<Field> fields = new ArrayList<>();

    fields.addAll(Arrays.asList(clazz.getDeclaredFields()));

    Class<?> superClass = clazz.getSuperclass();
    if (superClass == null || superClass == Object.class) {
      return fields;
    }

    return Iterables.concat(fields, getAllFields(superClass));
  }
}
