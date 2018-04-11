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
package com.dremio.exec.planner;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Set;

import javax.inject.Provider;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.serde.ProtobufByteStringSerDe;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.FragmentRoot;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalOperatorUtil;
import com.dremio.exec.proto.CoordExecRPC.FragmentCodec;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.MajorTypeSerDe;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionList;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.io.CharStreams;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

import io.protostuff.ByteString;

@SuppressWarnings("serial")
public class PhysicalPlanReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalPlanReader.class);

  private final ObjectReader physicalPlanReader;
  private final ObjectMapper mapper;
  private final ObjectReader optionListReader;
  private final ObjectReader operatorReader;
  private final LogicalPlanPersistence lpPersistance;

  public PhysicalPlanReader(
      SabotConfig config,
      ScanResult scanResult,
      LogicalPlanPersistence lpPersistance,
      final NodeEndpoint endpoint,
      final Provider<CatalogService> catalogService,
      SabotContext context) {

    this.lpPersistance = lpPersistance;

    final ObjectMapper lpMapper = lpPersistance.getMapper();

    // automatic serialization of protobuf.
    lpMapper.registerModule(new ProtobufModule());

    final SimpleModule deserModule = new SimpleModule("CustomSerializers")
        .addSerializer(MajorType.class, new MajorTypeSerDe.Se())
        .addSerializer(ByteString.class, new ByteStringSer())
        .addDeserializer(ByteString.class, new ByteStringDeser())
        .addDeserializer(MajorType.class, new MajorTypeSerDe.De());


    ProtoSerializers.registerSchema(deserModule, SourceConfig.getSchema());


    lpMapper.registerModule(deserModule);

    ConnectionConf.registerSubTypes(lpMapper, scanResult);

    Set<Class<? extends PhysicalOperator>> subTypes = PhysicalOperatorUtil.getSubTypes(scanResult);
    for (Class<? extends PhysicalOperator> subType : subTypes) {
      lpMapper.registerSubtypes(subType);
    }
    final InjectableValues injectables = new InjectableValues.Std()
        .addValue(CatalogService.class, catalogService.get())
        .addValue(ConnectionReader.class, new ConnectionReader(scanResult))
        .addValue(SabotContext.class, context)
        .addValue(NodeEndpoint.class, endpoint);

    this.mapper = lpMapper;
    this.physicalPlanReader = mapper.readerFor(PhysicalPlan.class).with(injectables);
    this.optionListReader = mapper.readerFor(OptionList.class).with(injectables);
    this.operatorReader = mapper.readerFor(PhysicalOperator.class).with(injectables);
  }

  public static class ByteStringDeser extends StdDeserializer<ByteString> {

    protected ByteStringDeser() {
      super(ByteString.class);
    }

    @Override
    public ByteString deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      return ByteString.copyFrom(p.getBinaryValue());
    }

  }

  public static class ByteStringSer extends StdSerializer<ByteString> {

    protected ByteStringSer() {
      super(ByteString.class);
    }

    @Override
    public void serialize(ByteString value, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeBinary(value.toByteArray());
    }

  }

  public com.google.protobuf.ByteString writeJsonBytes(OptionList list, FragmentCodec codec) throws JsonProcessingException{
    return writeValueAsByteString(list, codec);
  }

  public com.google.protobuf.ByteString writeJsonBytes(PhysicalOperator op, FragmentCodec codec) throws JsonProcessingException{
    return writeValueAsByteString(op, codec);
  }

  private com.google.protobuf.ByteString writeValueAsByteString(Object value, FragmentCodec codec) throws JsonProcessingException{
    return ProtobufByteStringSerDe.writeValue(mapper, value, toSerDeCodec(codec));
  }

  private static final ProtobufByteStringSerDe.Codec SNAPPY = new ProtobufByteStringSerDe.Codec() {
    @Override
    public OutputStream compress(OutputStream output) {
      return new SnappyOutputStream(output);
    }

    @Override
    public InputStream decompress(InputStream input) throws IOException {
      return new SnappyInputStream(input);
    }
  };

  private static ProtobufByteStringSerDe.Codec toSerDeCodec(FragmentCodec codec) {
    switch (codec) {
    case NONE:
      return ProtobufByteStringSerDe.Codec.NONE;
    case SNAPPY:
      return SNAPPY;
    default:
      throw new UnsupportedOperationException("Do not know how to compress using " + codec + " algorithm.");
    }
  }

  public PhysicalPlan readPhysicalPlan(String json) throws JsonProcessingException, IOException {
    logger.debug("Reading physical plan {}", json);
    return physicalPlanReader.readValue(json);
  }

  public PhysicalPlan readPhysicalPlan(com.google.protobuf.ByteString json, FragmentCodec codec) throws JsonProcessingException, IOException {
    return readValue(physicalPlanReader, json, codec);
  }

  public OptionList readOptionList(com.google.protobuf.ByteString json, FragmentCodec codec) throws JsonProcessingException, IOException {
    return readValue(optionListReader, json, codec);
  }

  public FragmentRoot readFragmentOperator(com.google.protobuf.ByteString json, FragmentCodec codec) throws JsonProcessingException, IOException {
    PhysicalOperator op = readValue(operatorReader, json, codec);
    if(op instanceof FragmentRoot){
      return (FragmentRoot) op;
    }else{
      throw new UnsupportedOperationException(String.format("The provided json fragment doesn't have a FragmentRoot as its root operator.  The operator was %s.", op.getClass().getCanonicalName()));
    }
  }

  private <T> T readValue(ObjectReader reader, com.google.protobuf.ByteString json, FragmentCodec codec)
      throws IOException {
    codec = codec != null ? codec : FragmentCodec.NONE;
    return ProtobufByteStringSerDe.readValue(reader, json, toSerDeCodec(codec), logger);
  }

  // TODO: move to using ProtobufByteStringSerDe#toInputStream
  public static InputStream toInputStream(com.google.protobuf.ByteString json, FragmentCodec codec) throws IOException {
    final FragmentCodec c = codec != null ? codec : FragmentCodec.NONE;

    final InputStream input = json.newInput();
    switch(c) {
    case NONE:
      return input;

    case SNAPPY:
      return new SnappyInputStream(input);

    default:
      throw new UnsupportedOperationException("Do not know how to uncompress using " + c + " algorithm.");
    }
  }

  public static String toString(com.google.protobuf.ByteString json, FragmentCodec codec) throws IOException {
    try(final InputStreamReader reader = new InputStreamReader(toInputStream(json, codec), UTF_8)) {
      return CharStreams.toString(reader);
    }
  }

  public LogicalPlanPersistence getLpPersistance(){
    return lpPersistance;
  }
}
