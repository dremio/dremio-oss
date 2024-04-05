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
package com.dremio.exec.store.sys.udf;

import com.dremio.common.expression.CompleteType;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.service.namespace.function.proto.FunctionArg;
import com.dremio.service.namespace.function.proto.FunctionBody;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.function.proto.FunctionDefinition;
import com.dremio.service.namespace.function.proto.ReturnType;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import org.junit.Test;

public class UserDefinedFunctionSerdeTest {

  @Test
  public void testToProto() {
    GoldenFileTestBuilder.create(UserDefinedFunctionSerde::toProto)
        .add(
            "No args",
            new UserDefinedFunction(
                "foo",
                "SELECT 1",
                CompleteType.INT,
                ImmutableList.of(),
                ImmutableList.of("dir", "space"),
                null,
                null,
                null))
        .add(
            "1 arg",
            new UserDefinedFunction(
                "foo",
                "SELECT 1",
                CompleteType.INT,
                ImmutableList.of(
                    new UserDefinedFunction.FunctionArg("bar", CompleteType.VARCHAR, null)),
                null,
                null,
                null,
                null))
        .runTests();
  }

  @Test
  public void testFromProto() {
    GoldenFileTestBuilder.create(UserDefinedFunctionSerde::fromProto)
        .add(
            "No Args",
            new FunctionConfig()
                .setName("foo")
                .setReturnType(
                    new ReturnType()
                        .setRawDataType(ByteString.copyFrom(CompleteType.INT.serialize())))
                .setFunctionDefinitionsList(
                    ImmutableList.of(
                        new FunctionDefinition()
                            .setFunctionArgList(ImmutableList.of())
                            .setFunctionBody(new FunctionBody().setRawBody("SELECT 1"))))
                .setFullPathList(ImmutableList.of("dir", "space")))
        .add(
            "1 arg",
            new FunctionConfig()
                .setFunctionDefinitionsList(
                    ImmutableList.of(
                        new FunctionDefinition()
                            .setFunctionArgList(
                                ImmutableList.of(
                                    new FunctionArg()
                                        .setName("bar")
                                        .setRawDataType(
                                            ByteString.copyFrom(CompleteType.VARCHAR.serialize()))))
                            .setFunctionBody(new FunctionBody().setRawBody("SELECT 1"))))
                .setFullPathList(null)
                .setName("foo")
                .setReturnType(
                    new ReturnType()
                        .setRawDataType(ByteString.copyFrom(CompleteType.INT.serialize()))))
        .runTests();
  }

  @Test
  public void testRoundTrip() {
    ProtostuffSerializer<FunctionConfig> protostuffSerializer =
        new ProtostuffSerializer<>(FunctionConfig.getSchema());
    GoldenFileTestBuilder.create(
            (UserDefinedFunction fc) -> {
              FunctionConfig proto1 = UserDefinedFunctionSerde.toProto(fc);
              byte[] bytes = protostuffSerializer.convert(proto1);
              FunctionConfig proto2 = protostuffSerializer.revert(bytes);
              return UserDefinedFunctionSerde.fromProto(proto2);
            })
        .add(
            "No Args",
            new UserDefinedFunction(
                "foo",
                "SELECT 1",
                CompleteType.INT,
                ImmutableList.of(),
                ImmutableList.of("dir", "space"),
                null,
                null,
                null))
        .add(
            "1 arg",
            new UserDefinedFunction(
                "foo",
                "SELECT 1",
                CompleteType.INT,
                ImmutableList.of(
                    new UserDefinedFunction.FunctionArg("bar", CompleteType.VARCHAR, null)),
                null,
                null,
                null,
                null))
        .runTests();
  }
}
