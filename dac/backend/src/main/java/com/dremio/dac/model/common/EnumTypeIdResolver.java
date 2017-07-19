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
package com.dremio.dac.model.common;

import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Custom subtype mapping for types annotated with TypesEnum
 */
public class EnumTypeIdResolver implements TypeIdResolver {

  private final Map<String, JavaType> nameToType = new HashMap<>();
  private final Map<Class<?>, String> typeToName = new HashMap<>();
  private String description;
  private JavaType baseType;

  @Override
  public void init(JavaType baseType) {
    this.baseType = baseType;
    Class<?> baseClass = baseType.getRawClass();
    TypesEnum typesEnum = baseClass.getAnnotation(TypesEnum.class);
    while (baseClass != null && typesEnum == null) {
      baseClass = baseClass.getSuperclass();
      typesEnum = baseClass.getAnnotation(TypesEnum.class);
    }
    if (typesEnum == null) {
      throw new NullPointerException("Missing annotation TypesEnum on " + baseType.getRawClass());
    }
    SubTypeMapping mapping = new SubTypeMapping(typesEnum);
    TypeFactory defaultInstance = TypeFactory.defaultInstance();
    StringBuilder sb = new StringBuilder();
    for (Enum<?> e : mapping.getEnumConstants()) {
      String name = e.name();
      String className = mapping.getClassName(e);
      try {
        Class<?> c = Class.forName(className, false, this.getClass().getClassLoader());
        JavaType type = defaultInstance.uncheckedSimpleType(c);
        this.nameToType.put(name.toLowerCase(), type);
        this.typeToName.put(c, name);
        sb.append(name + " => " + c.getName() + "\n");
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException(String.format(
            "class not found %s for enum value %s for base type %s",
            className, name, baseType
            ) , e1);
      }
    }
    this.description = sb.toString();
  }

  @Override
  public String idFromValue(Object value) {
    return idFromValueAndType(value, value.getClass());
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType) {
    String name = typeToName.get(suggestedType);
    if (name == null) {
      throw new NullPointerException(suggestedType + " " + String.valueOf(value) + "\n" + description);
    }
    return name;
  }

  @Override
  public String idFromBaseType() {
    throw new IllegalArgumentException("base type not serializable: " + baseType);
  }

  @Override
  public JavaType typeFromId(String id) {
    return typeFromId(null, id);
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    JavaType type = nameToType.get(id.toLowerCase());
    if (type == null) {
      throw new NullPointerException(
          format("no subtype of %s found for enum value %s. existing mappings:\n%s",
              baseType, id, description));

    }
    return type;
  }

  @Override
  public String getDescForKnownTypeIds() {
    return description;
  }

  @Override
  public Id getMechanism() {
    return Id.CUSTOM;
  }

  @Override
  public String toString() {
    return "EnumTypeIdResolver{\n" + description + "}";
  }

}
