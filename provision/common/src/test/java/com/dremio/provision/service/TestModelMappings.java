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
package com.dremio.provision.service;

import org.junit.Test;
import org.modelmapper.ModelMapper;

import com.dremio.provision.AwsProps;
import com.dremio.provision.AwsPropsApi;
import com.dremio.provision.ImmutableAwsPropsApi;
import com.dremio.test.DremioTest;

/**
 * Test any model mappings used in provisioning.
 */
public class TestModelMappings extends DremioTest {

  /**
   * check to make sure both protobuf entities have same number of fields.
   */
  @Test
  public void ensure1to1onAwsProps() {
    ModelMapper mapper = new ModelMapper();
    mapper.createTypeMap(AwsProps.class, AwsPropsApi.class);
    mapper.createTypeMap(AwsPropsApi.class, AwsProps.class);
    mapper.validate();

    AwsPropsApi api = AwsPropsApi.builder().build();
    mapper.map(mapper.map(api, AwsProps.class), ImmutableAwsPropsApi.Builder.class).build();

  }
}
