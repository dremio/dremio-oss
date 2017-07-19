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
package com.dremio.exec.planner;

import java.io.IOException;
import java.util.Set;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.logical.LogicalPlan;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.FragmentRoot;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalOperatorUtil;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.MajorTypeSerDe;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionList;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.service.coordinator.NodeEndpointSerDe;
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

import io.protostuff.ByteString;

public class PhysicalPlanReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalPlanReader.class);

  private final ObjectReader physicalPlanReader;
  private final ObjectMapper mapper;
  private final ObjectReader operatorReader;
  private final ObjectReader logicalPlanReader;
  private final LogicalPlanPersistence lpPersistance;

  public PhysicalPlanReader(SabotConfig config, ScanResult scanResult, LogicalPlanPersistence lpPersistance, final NodeEndpoint endpoint,
                            final StoragePluginRegistry pluginRegistry, SabotContext context) {

    this.lpPersistance = lpPersistance;
    // Endpoint serializer/deserializer.
    final SimpleModule deserModule = new SimpleModule("PhysicalOperatorModule")
        .addSerializer(NodeEndpoint.class, new NodeEndpointSerDe.Se())
        .addDeserializer(NodeEndpoint.class, new NodeEndpointSerDe.De())
        .addSerializer(MajorType.class, new MajorTypeSerDe.Se())
        .addSerializer(ByteString.class, new ByteStringSer())
        .addDeserializer(ByteString.class, new ByteStringDeser())
        .addDeserializer(MajorType.class, new MajorTypeSerDe.De());



    final ObjectMapper lpMapper = lpPersistance.getMapper();
    lpMapper.registerModule(deserModule);
    Set<Class<? extends PhysicalOperator>> subTypes = PhysicalOperatorUtil.getSubTypes(scanResult);
    for (Class<? extends PhysicalOperator> subType : subTypes) {
      lpMapper.registerSubtypes(subType);
    }
    final InjectableValues injectables = new InjectableValues.Std()
        .addValue(StoragePluginRegistry.class, pluginRegistry)
        .addValue(SabotContext.class, context)
        .addValue(NodeEndpoint.class, endpoint);

    this.mapper = lpMapper;
    this.physicalPlanReader = mapper.reader(PhysicalPlan.class).with(injectables);
    this.operatorReader = mapper.reader(PhysicalOperator.class).with(injectables);
    this.logicalPlanReader = mapper.reader(LogicalPlan.class).with(injectables);
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


  public String writeJson(OptionList list) throws JsonProcessingException{
    return mapper.writeValueAsString(list);
  }

  public String writeJson(PhysicalOperator op) throws JsonProcessingException{
    return mapper.writeValueAsString(op);
  }

  public PhysicalOperator readPhysicalOperator(final String json) throws IOException {
    return operatorReader.readValue(json);
  }

  public PhysicalPlan readPhysicalPlan(String json) throws JsonProcessingException, IOException {
    logger.debug("Reading physical plan {}", json);
    return physicalPlanReader.readValue(json);
  }

  public FragmentRoot readFragmentOperator(String json) throws JsonProcessingException, IOException {
    logger.debug("Attempting to read {}", json);
    PhysicalOperator op = operatorReader.readValue(json);
    if(op instanceof FragmentRoot){
      return (FragmentRoot) op;
    }else{
      throw new UnsupportedOperationException(String.format("The provided json fragment doesn't have a FragmentRoot as its root operator.  The operator was %s.", op.getClass().getCanonicalName()));
    }
  }

  public LogicalPlanPersistence getLpPersistance(){
    return lpPersistance;
  }

  public LogicalPlan readLogicalPlan(String json) throws JsonProcessingException, IOException{
    logger.debug("Reading logical plan {}", json);
    return logicalPlanReader.readValue(json);
  }
}
