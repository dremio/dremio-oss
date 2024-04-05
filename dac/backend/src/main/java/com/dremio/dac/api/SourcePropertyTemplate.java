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

import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Host;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SecretRef;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents a source conf property template */
public class SourcePropertyTemplate {
  private static final Logger logger = LoggerFactory.getLogger(SourcePropertyTemplate.class);

  /** Enum for all possible property types */
  public enum TemplatePropertyType {
    TEXT("text"),
    NUMBER("number"),
    BOOLEAN("boolean"),
    CREDENTIALS("credentials"),
    HOST_LIST("host_list"),
    PROPERTY_LIST("property_list"),
    VALUE_LIST("value_list"),
    ENUM("enum");

    private final String value;

    TemplatePropertyType(final String value) {
      this.value = value;
    }

    public static TemplatePropertyType fromValue(String value) {
      for (TemplatePropertyType type : TemplatePropertyType.values()) {
        if (type.value.equals(value)) {
          return type;
        }
      }

      return null;
    }
  }

  private final String name;
  private final String label;
  private final TemplatePropertyType type;
  private final Object defaultValue;
  private final Boolean secret;
  private final List<EnumValueTemplate> options;

  /** Represents a source conf enum */
  public static class EnumValueTemplate {
    private final String value;
    private final String label;

    @JsonCreator
    public EnumValueTemplate(
        @JsonProperty("value") String value, @JsonProperty("label") String label) {
      this.value = value;
      this.label = label;
    }

    public String getValue() {
      return value;
    }

    public String getLabel() {
      return label;
    }
  }

  @JsonCreator
  public SourcePropertyTemplate(
      @JsonProperty("propertyName") String name,
      @JsonProperty("label") String label,
      @JsonProperty("type") String type,
      @JsonProperty("secret") Boolean secret,
      @JsonProperty("defaultValue") Object defaultValue,
      @JsonProperty("values") List<EnumValueTemplate> values) {
    this.name = name;
    this.label = label;
    this.type = TemplatePropertyType.fromValue(type);
    this.defaultValue = defaultValue;
    this.secret = secret;
    this.options = values;
  }

  public String getPropertyName() {
    return name;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public String getLabel() {
    return label;
  }

  public String getType() {
    return type.value;
  }

  public Boolean getSecret() {
    return secret;
  }

  public List<EnumValueTemplate> getOptions() {
    return options;
  }

  private static TemplatePropertyType getFieldType(Field field) {
    final Class<?> fieldType = ClassUtils.primitiveToWrapper(field.getType());
    if (String.class.isAssignableFrom(fieldType) || SecretRef.class.isAssignableFrom(fieldType)) {
      return TemplatePropertyType.TEXT;
    }

    if (Number.class.isAssignableFrom(fieldType)) {
      return TemplatePropertyType.NUMBER;
    }

    if (Boolean.class.isAssignableFrom(fieldType)) {
      return TemplatePropertyType.BOOLEAN;
    }

    // AuthenticationType = credentials
    if (AuthenticationType.class.equals(fieldType)) {
      return TemplatePropertyType.CREDENTIALS;
    }

    // handle array types
    if (Collection.class.isAssignableFrom(fieldType)) {
      final ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
      if (parameterizedType.getActualTypeArguments().length == 1) {
        final Type elementType = parameterizedType.getActualTypeArguments()[0];

        if (Host.class.equals(elementType)) {
          return TemplatePropertyType.HOST_LIST;
        }

        if (Property.class.equals(elementType)) {
          return TemplatePropertyType.PROPERTY_LIST;
        }

        if (String.class.equals(elementType)) {
          return TemplatePropertyType.VALUE_LIST;
        }
      }
    }

    // handle enums
    if (fieldType.isEnum()) {
      return TemplatePropertyType.ENUM;
    }

    throw new UnsupportedOperationException(
        String.format(
            "Found source property [%s] with unsupported type [%s]",
            field.getName(), fieldType.getTypeName()));
  }

  public static SourcePropertyTemplate fromField(Field field, Object value) {
    String label = null;
    final TemplatePropertyType type = getFieldType(field);

    // labels are provided by the DisplayMetadata annotation
    DisplayMetadata annotation = field.getAnnotation(DisplayMetadata.class);
    if (annotation != null) {
      label = annotation.label();
    }

    // Check if the field is annotated as Secret
    final boolean isSecret = field.getAnnotation(Secret.class) != null;

    // For enums, get the list of values
    List<EnumValueTemplate> values = null;
    if (field.getType().isEnum()) {
      values = new ArrayList<>();

      for (Object o : field.getType().getEnumConstants()) {
        String enumLabel = null;

        try {
          // enums can have labels defined using the DisplayMetadata annotation.
          DisplayMetadata enumAnnotation =
              o.getClass().getField(o.toString()).getAnnotation(DisplayMetadata.class);
          if (enumAnnotation != null) {
            enumLabel = enumAnnotation.label();
          }
        } catch (NoSuchFieldException e) {
          logger.warn("enum value must exist {}", o, e);
        }

        values.add(new EnumValueTemplate(o.toString(), enumLabel));
      }
    }

    return new SourcePropertyTemplate(field.getName(), label, type.value, isSecret, value, values);
  }
}
