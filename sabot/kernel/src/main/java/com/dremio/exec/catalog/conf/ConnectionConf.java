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
package com.dremio.exec.catalog.conf;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.inject.Provider;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.AbstractConnectionConf;
import com.dremio.services.credentials.CredentialsException;
import com.dremio.services.credentials.CredentialsService;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Defaults;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import io.protostuff.ByteString;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;

/**
 * Abstract class describing a Source Configuration.
 *
 * Note, we disable getters/setters for Jackson because we should be interacting directly with
 * fields (same as proto encoding). We also avoid using @JsonIgnore annotation as it causes problems
 * when used in tandem with field serialization.
 *
 * We also are claiming that we use JsonTypeName resolution but that isn't actually true. We are
 * using pre-registration using the registerSubTypes() method below to ensure that everything is
 * named correctly. The SourceType(value=<name>) annotation parameter is what is used for
 * serialization/deserialization in JSON.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
@JsonAutoDetect(fieldVisibility=Visibility.PUBLIC_ONLY, getterVisibility=Visibility.NONE, isGetterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE)
public abstract class ConnectionConf<T extends ConnectionConf<T, P>, P extends StoragePlugin> implements AbstractConnectionConf, Externalizable {
  private final transient Schema<T> schema;
  public static final String USE_EXISTING_SECRET_VALUE = "$DREMIO_EXISTING_VALUE$";

  @SuppressWarnings("unchecked")
  protected ConnectionConf() {
    this.schema = (Schema<T>) ConnectionSchema.getSchema(getClass());
  }

  public void clearSecrets() {
    clear(new Predicate<Field>() {
      @Override
      public boolean apply(Field field) {
        return field.isAnnotationPresent(Secret.class);
      }}, true);
  }

  public void clearNotMetadataImpacting() {
    clear(new Predicate<Field>() {
      @Override
      public boolean apply(Field field) {
        return field.isAnnotationPresent(NotMetadataImpacting.class);
      }}, false);
  }

  /**
   * Clears all fields that match a particular predicate. For all primitive types, the value is set
   * to zero. For object types, the value is set to null.
   *
   * @param predicate
   */
  private void clear(Predicate<Field> predicate, boolean isSecret) {
    try {
      for(Field field : FieldUtils.getAllFields(getClass())) {
        if(predicate.apply(field)) {
          //if field is a secret property list, clear all sensitive property values within the list
          if (isSecret && isPropertyList(field)) {
            final List<Property> propertyList = (List<Property>) field.get(this);
            if (CollectionUtils.isNotEmpty(propertyList)) {
              List<Property> secretList = new LinkedList<>();
              for (Property prop : propertyList) {
                secretList.add(new Property(prop.name, USE_EXISTING_SECRET_VALUE));
              }
              field.set(this, secretList);
            } else {
              field.set(this, null);
            }
          } else if (isSecret && (field.getType().equals(String.class))) {
            final String value = (String) field.get(this);

            if (Strings.isNullOrEmpty(value)) {
              field.set(this, null);
            } else {
              field.set(this, USE_EXISTING_SECRET_VALUE);
            }
          } else {
            Object defaultValue = Defaults.defaultValue(field.getType());
            field.set(this, defaultValue);
          }
        }
      }
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies secret values from existingConf to the connectionConf if they are set to {@link USE_EXISTING_SECRET_VALUE}.
   *
   * @param existingConf
   */
  public void applySecretsFrom(ConnectionConf existingConf) {
    for (Field field : FieldUtils.getAllFields(getClass())) {
      if (field.getAnnotation(Secret.class) == null) {
        continue;
      }

      try {

        //apply secrets for field type List<Property>
        if (isPropertyList(field)) {
          List<Property> thisPropertyList = (List<Property>) field.get(this);
          if (thisPropertyList == null) {
            continue;
          }

          //convert existing properties into map for easy access
          List<Property> existingPropertyList = (List<Property>) field.get(existingConf);
          Map<String, Property> existingPropertyMap = new HashMap<>();
          if (existingPropertyList != null) {
            for (Property property : existingPropertyList) {
              existingPropertyMap.put(property.name, property);
            }
          }

          //generate new property list. Apply secrets where applicable
          List<Property> appliedPropertyList = new LinkedList<>();
          for (Property prop : thisPropertyList) {
            if (prop.value.equals(USE_EXISTING_SECRET_VALUE)) {
              appliedPropertyList.add(existingPropertyMap.get(prop.name));
            } else {
              appliedPropertyList.add(new Property(prop.name, prop.value));
            }
          }

          field.set(this, appliedPropertyList);

        //apply secrets for field type String
        } else if (field.getType().equals(String.class) && USE_EXISTING_SECRET_VALUE.equals(field.get(this))) {
          field.set(this, field.get(existingConf));
        }
      } catch (IllegalAccessException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Resolves secrets on the conf via the credentials service. This first clones the conf,
   * then resolves the secrets on the clone, and returns the resolved (cloned) conf. The
   * original conf should remain unchanged. The new resolved conf should be treated with
   * care as it will contain the resolved secret (i.e. should never be persisted).
   */
  public T resolveSecrets(CredentialsService credentialsService) {
    final T resolvedConf = this.clone();
    for (Field field : FieldUtils.getAllFields(resolvedConf.getClass())) {
      if (field.getAnnotation(Secret.class) == null) {
        continue;
      }
      try {

        //resolve secrets for field type List<Property>
        if (isPropertyList(field)) {
          final List<Property> fieldList = (List<Property>) field.get(resolvedConf);
          if (CollectionUtils.isNotEmpty(fieldList)) {
            List<Property> resolvedSecretList = new LinkedList<>();
            for (Property prop : fieldList) {
              String resolvedSecretProp;
              try {
                resolvedSecretProp = credentialsService.lookup(prop.value);
              } catch (IllegalArgumentException e) {
                // If field is not a valid URI, fallback to treating it as a regular password
                resolvedSecretProp = prop.value;
              } catch (CredentialsException e) {
                throw new RuntimeException(e);
              }
              resolvedSecretList.add(new Property(prop.name, resolvedSecretProp));
            }
            field.set(resolvedConf, resolvedSecretList);
          }
        } else if (field.getType().equals(String.class)) { ////resolve Secrets for field type String
          final String fieldString = (String) field.get(resolvedConf);
          if (Strings.isNullOrEmpty(fieldString)) {
            continue;
          }
          String resolvedSecret;
          try {
            resolvedSecret = credentialsService.lookup(fieldString);
          } catch (IllegalArgumentException e) {
            // If field is not a valid URI, fallback to treating it as a regular password
            resolvedSecret = fieldString;
          } catch (CredentialsException e) {
            throw new RuntimeException(e);
          }
          field.set(resolvedConf, resolvedSecret);
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return resolvedConf;
  }

  /**
   * checks if field Type is a List of Property objects
   */
  private boolean isPropertyList(Field field) {
    Type fieldType = field.getGenericType();
    if (fieldType instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) fieldType;
      Type[] typeArguments = parameterizedType.getActualTypeArguments();
      if (typeArguments.length == 1 && typeArguments[0] == Property.class) {
        return List.class.isAssignableFrom((Class<?>) parameterizedType.getRawType());
      }
    }
    return false;
  }

  public static void registerSubTypes(ObjectMapper mapper, ConnectionReader connectionReader) {
    for (Class<?> c : connectionReader.getAllConnectionConfs().values()) {
      NamedType nt = new NamedType(c, c.getAnnotation(SourceType.class).value());
      mapper.registerSubtypes(nt);
    }
  }

  @Override
  public boolean equals(Object other) {
    if(other == null || !(other instanceof ConnectionConf) ) {
      return false;
    }
    final ConnectionConf<?, ?> o = (ConnectionConf<?, ?>) other;

    if(!o.getType().equals(getType())) {
      return false;
    }
    return Arrays.equals(toBytes(),  o.toBytes());
  }

  @Override
  public ByteString toBytesString() {
    return ByteString.copyFrom(toBytes());
  }

  @SuppressWarnings("unchecked")
  public byte[] toBytes() {
    return ProtobufIOUtil.toByteArray( (T) this, schema, LinkedBuffer.allocate() );
  }

  @Override
  public final T clone() {
    byte[] bytes = toBytes();
    T message = schema.newMessage();
    ProtobufIOUtil.mergeFrom(bytes, message, schema);
    return message;
  }

  /**
   * Indicates whether the other conf is equal to this one, ignoring fields that do not impact metadata.
   *
   * @param other connection conf
   * @return true if this connection conf equals other conf, ignoring fields that do not impact metadata
   */
  public final boolean equalsIgnoringNotMetadataImpacting(ConnectionConf<?, ?> other) {
    final ConnectionConf<?, ?> existingConf = clone();
    final ConnectionConf<?, ?> newConf = other.clone();

    // reapply any secrets from existingConf to newConf
    newConf.applySecretsFrom(existingConf);

    existingConf.clearNotMetadataImpacting();
    newConf.clearNotMetadataImpacting();
    return existingConf.equals(newConf);
  }

  @Override
  public final String getType() {
    return this.getClass().getAnnotation(SourceType.class).value();
  }

  @Override
  public final int hashCode() {
    return Arrays.hashCode(toBytes());
  }

  public abstract P newPlugin(final SabotContext context, final String name, Provider<StoragePluginId> pluginIdProvider);

  // Use this if newPlugin logic need to know that if source is created or modified, which is identified using influxSourcePred.
  public P newPlugin(final SabotContext context, final String name, Provider<StoragePluginId> pluginIdProvider, java.util.function.Predicate<String> influxSourcePred) {
    return newPlugin(context, name, pluginIdProvider);
  }

  public boolean isInternal() {
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void writeExternal(ObjectOutput out) throws IOException {
    T obj = (T) this;
    ProtobufIOUtil.writeDelimitedTo(out, obj, schema);
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    T obj = (T) this;
    ProtobufIOUtil.mergeDelimitedFrom(in, obj, schema);
  }

}
