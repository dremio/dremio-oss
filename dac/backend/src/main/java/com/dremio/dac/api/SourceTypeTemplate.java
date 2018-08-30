/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.catalog.conf.SourceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.protostuff.Tag;

/**
 * Source type
 */
public class SourceTypeTemplate {
  private static final Logger logger = LoggerFactory.getLogger(SourceTypeTemplate.class);

  private final String sourceType;
  private final String label;
  private final String icon;
  private final List<SourcePropertyTemplate> elements;

  @JsonCreator
  public SourceTypeTemplate(
    @JsonProperty("sourceType") String name,
    @JsonProperty("label") String label,
    @JsonProperty("icon") String icon,
    @JsonProperty("elements") List<SourcePropertyTemplate> elements) {
    this.sourceType = name;
    this.label = label;
    this.icon = icon;
    this.elements = elements;
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

  public List<SourcePropertyTemplate> getElements() {
    return elements;
  }

  public static SourceTypeTemplate fromSourceClass(Class<?> sourceClass, boolean includeProperties) {
    final SourceType type = sourceClass.getAnnotation(SourceType.class);

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

    if (!includeProperties) {
      return new SourceTypeTemplate(type.value(), type.label(), icon, null);
    }

    final Object newClassInstance;
    try {
      newClassInstance = sourceClass.getConstructor().newInstance();
    } catch (Exception e) {
      logger.warn("Failed to create new instance of [{}]", sourceClass.getName(), e);
      return new SourceTypeTemplate(type.value(), type.label(), icon, null);
    }

    final Field[] declaredFields = sourceClass.getDeclaredFields();
    final List<SourcePropertyTemplate> properties = new ArrayList<>();

    for (Field field : declaredFields) {
      // only properties with the Tag annotation will be included
      if (field.getAnnotation(Tag.class) == null || field.getAnnotation(JsonIgnore.class) != null) {
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
          final Collection collection = (Collection) definedValue;
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

    return new SourceTypeTemplate(type.value(), type.label(), icon, properties);
  }
}
