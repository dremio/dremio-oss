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
package com.dremio.dac.util;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import com.dremio.common.logical.FormatPluginConfigBase;
import com.dremio.common.logical.StoragePluginConfigBase;
import com.dremio.common.logical.data.LogicalOperatorBase;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.explore.model.ExpressionBase;
import com.dremio.dac.explore.model.ExtractListRuleBase;
import com.dremio.dac.explore.model.FieldTransformationBase;
import com.dremio.dac.explore.model.FilterBase;
import com.dremio.dac.explore.model.FromBase;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.model.common.Acceptor;
import com.dremio.dac.model.sources.UnknownConfigMixIn;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.TransformConvertCase;
import com.dremio.dac.proto.model.dataset.TransformSorts;
import com.dremio.dac.proto.model.source.UnknownConfig;
import com.dremio.dac.server.socket.SocketMessage;
import com.dremio.datastore.Converter;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.User;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * turn stuff into pretty JSON
 */
public class JSONUtil {

  /**
   * default indent output represents arrays on one line
   * so we want more than just mapper.enable(SerializationFeature.INDENT_OUTPUT);
   */
  private static final class PrettyPrintMappingJsonFactory extends MappingJsonFactory {
    private final DefaultPrettyPrinter pp;
    private static final long serialVersionUID = 1L;

    private PrettyPrintMappingJsonFactory() {
      this.pp = new DefaultPrettyPrinter();
      pp.indentArraysWith(new DefaultIndenter());
    }

    @Override
    public JsonGenerator createGenerator(OutputStream out, JsonEncoding enc) throws IOException {
      JsonGenerator generator = super.createGenerator(out, enc);
      generator.setPrettyPrinter(pp);
      return generator;
    }

    @Override
    public PrettyPrintMappingJsonFactory copy() {
      return new PrettyPrintMappingJsonFactory();
    }
  }

  private static final ObjectMapper prettyMapper = new ObjectMapper(new PrettyPrintMappingJsonFactory());
  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    for (ObjectMapper m : asList(mapper, prettyMapper)) {
      // omit fields when they are null
      m.setSerializationInclusion(Include.NON_NULL);
      m.registerSubtypes(SocketMessage.getImplClasses());
      m.registerModule(new SimpleModule().addAbstractTypeMapping(User.class, SimpleUser.class));
      // Turns out you can annotate generated classes using mixins!
      m.addMixIn(TransformConvertCase.class, TransformConvertCaseMixin.class);
      m.addMixIn(TransformSorts.class, TransformSortsMixin.class);
      m.addMixIn(UnknownConfig.class, UnknownConfigMixIn.class);
      Acceptor<?, ?, ?>[] acceptors = {
          ExpressionBase.acceptor,
          ExtractListRuleBase.acceptor,
          FieldTransformationBase.acceptor,
          FilterBase.acceptor,
          FromBase.acceptor,
          TransformBase.acceptor
      };
      for (Acceptor<?, ?, ?> acceptor : acceptors) {
        m.registerModule(newWrappingModule(acceptor));
      }
    }
  }

  private static <C, W> SimpleModule newWrappingModule(Acceptor<C, ?, W> acceptor) {
    return newWrappingModule(acceptor.getBaseType(), acceptor.getWrapperType(), acceptor.converter());
  }

  private static <T1, T2> SimpleModule newWrappingModule(final Class<T1> wrapped, final Class<T2> wrapper, final Converter<T1, T2> converter) {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(wrapper, new JsonDeserializer<T2>() {
      @Override
      public T2 deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        return converter.convert(ctxt.readValue(p, wrapped));
      }
    });
    module.addSerializer(wrapper, new JsonSerializer<T2>() {
      @Override
      public void serialize(T2 value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
        serializers.defaultSerializeValue(converter.revert(value), gen);
      }
    });
    return module;
  }

  private interface TransformConvertCaseMixin {
    @JsonProperty("case") String getConvertCase();
  }

  private interface TransformSortsMixin {
    @JsonProperty("columns") List<Order> getColumnsList();
  }

  public static ObjectMapper prettyMapper() {
    return prettyMapper.copy();
  }

  public static ObjectMapper mapper() {
    return mapper.copy();
  }

  public static ObjectMapper registerStorageTypes(ObjectMapper mapper, ScanResult scanResult) {
    registerSubtypes(mapper, LogicalOperatorBase.getSubTypes(scanResult));
    registerSubtypes(mapper, StoragePluginConfigBase.getSubTypes(scanResult));
    registerSubtypes(mapper, FormatPluginConfigBase.getSubTypes(scanResult));
    return mapper;
  }

  private static <T> void registerSubtypes(ObjectMapper mapper, Set<Class<? extends T>> types) {
    for (Class<? extends T> type : types) {
      mapper.registerSubtypes(type);
    }
  }

  private static final ObjectWriter writer = prettyMapper.writerWithDefaultPrettyPrinter();

  public static String toString(Object o) {
    try {
      return writer.writeValueAsString(o);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
