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
package com.dremio.exec.planner.acceleration;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.work.user.SubstitutionSettings;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.collect.ImmutableList;

public class TestMaterializationList {

  @Mock
  private SqlConverter converter;

  @Mock
  private UserSession session;

  @Mock
  private MaterializationDescriptorProvider provider;

  @Mock
  private DremioMaterialization relOptMat1;

  @Mock
  private DremioMaterialization relOptMat2;

  @Mock
  private MaterializationDescriptor excluded;

  @Mock
  private MaterializationDescriptor included;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testListDiscardsGivenExclusions() {
    when(excluded.getMaterializationFor(converter)).thenReturn(relOptMat1);
    when(excluded.getLayoutId()).thenReturn("rid-1");
    when(included.getMaterializationFor(converter)).thenReturn(relOptMat2);
    when(included.getLayoutId()).thenReturn("rid-2");

    SubstitutionSettings materializationSettings = new SubstitutionSettings(ImmutableList.of("rid-1"));

    when(session.getSubstitutionSettings()).thenReturn(materializationSettings);
    when(provider.get()).thenReturn(ImmutableList.of(excluded, included));

    final MaterializationList materializations = new MaterializationList(converter, session, provider);
    // we do not need to check the result here as making sure that convert method is hit once is enough
    // otherwise we need to do excessive amount of rel mocking
    materializations.build(provider);

    verify(excluded, never()).getMaterializationFor(any(SqlConverter.class));
    verify(included, atLeastOnce()).getMaterializationFor(converter);
  }



}
