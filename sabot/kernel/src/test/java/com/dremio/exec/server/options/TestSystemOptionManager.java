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
package com.dremio.exec.server.options;

import static com.dremio.exec.server.options.SystemOptionManager.OPTIONS_KEY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.modelmapper.internal.util.Lists;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValueProtoList;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests for {@link SystemOptionManager}
 */
public class TestSystemOptionManager extends DremioTest {
  private SystemOptionManager som;
  private LegacyKVStore<String, OptionValueProtoList> kvStore;
  private OptionValidatorListing optionValidatorListing;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() throws Exception {
    optionValidatorListing = spy(new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT));
    LogicalPlanPersistence lpp = mock(LogicalPlanPersistence.class);
    when(lpp.getMapper()).thenReturn(mock(ObjectMapper.class));

    kvStore = mock(LegacyKVStore.class);
    LegacyKVStoreProvider storeProvider = mock(LegacyKVStoreProvider.class);
    when(storeProvider.getStore(SystemOptionManager.OptionStoreCreator.class))
      .thenReturn(kvStore);

    LegacyKVStore mockedEmptyKVStore = mock(LegacyKVStore.class);

    when(mockedEmptyKVStore.find()).thenReturn(Collections.emptyList());
    when(storeProvider.getStore(SystemOptionManager.LegacyProtoOptionStoreCreator.class))
      .thenReturn(mockedEmptyKVStore);
    when(storeProvider.getStore(SystemOptionManager.LegacyJacksonOptionStoreCreator.class))
      .thenReturn(mockedEmptyKVStore);

    som = spy(new SystemOptionManager(optionValidatorListing, lpp, () -> storeProvider, false));
    som.start();
    reset(kvStore); //clearInvocations
  }

  private OptionValue registerTestOption(OptionValue.Kind kind, String name, String defaultValue) {
    OptionValue defaultOptionValue = OptionValue.createOption(kind, OptionValue.OptionType.SYSTEM, name, defaultValue);
    OptionValidator optionValidator = mock(OptionValidator.class);
    when(optionValidator.getDefault()).thenReturn(defaultOptionValue);
    doReturn(optionValidator).when(optionValidatorListing).getValidator(name);
    doReturn(true).when(optionValidatorListing).isValid(name);
    return defaultOptionValue;
  }

  @Test
  public void testGet() {
    registerTestOption(OptionValue.Kind.LONG, "test-option", "0");

    OptionValue optionValue = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test-option", 123);
    OptionValueProtoList optionList = OptionValueProtoList.newBuilder()
      .addAllOptions(Collections.singletonList(OptionValueProtoUtils.toOptionValueProto(optionValue)))
      .build();
    when(kvStore.get(OPTIONS_KEY)).thenReturn(optionList);

    assertEquals(optionValue, som.getOption(optionValue.getName()));
    verify(kvStore, times(1)).get(eq(OPTIONS_KEY));

    assertNull(som.getOption("not-a-real-option"));
  }

  @Test
  public void testSet() {
    registerTestOption(OptionValue.Kind.LONG, "already-added-option",  "0");
    OptionValue toAddOptionDefault = registerTestOption(OptionValue.Kind.STRING, "to-add-option",  "default-value");

    OptionValue alreadyAddedOption = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "already-added-option", 123);
    OptionValue toAddOption = OptionValue.createString(OptionValue.OptionType.SYSTEM, "to-add-option", "some-value");

    OptionValueProtoList optionList = OptionValueProtoList.newBuilder()
      .addAllOptions(Collections.singletonList(OptionValueProtoUtils.toOptionValueProto(alreadyAddedOption)))
      .build();
    when(kvStore.get(OPTIONS_KEY)).thenReturn(optionList);

    // Re-adding same option should exit early
    som.setOption(alreadyAddedOption);
    verify(kvStore, times(0)).put(any(), any());

    // Adding a option with default value should exit early
    som.setOption(toAddOptionDefault);
    verify(kvStore, times(0)).put(any(), any());

    // regular add option
    som.setOption(toAddOption);
    ArgumentCaptor<OptionValueProtoList> argument = ArgumentCaptor.forClass(OptionValueProtoList.class);
    verify(kvStore, times(1)).put(eq(OPTIONS_KEY), argument.capture());
    assertThat(argument.getValue().getOptionsList(),
      containsInAnyOrder(OptionValueProtoUtils.toOptionValueProto(toAddOption),
        OptionValueProtoUtils.toOptionValueProto(alreadyAddedOption))
    );

    // Overriding an option
    OptionValue overridingOption = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "already-added-option", 999);
    som.setOption(overridingOption);
    verify(kvStore, times(1)).put(OPTIONS_KEY,
      OptionValueProtoList.newBuilder()
        .addAllOptions(Collections.singletonList(OptionValueProtoUtils.toOptionValueProto(overridingOption)))
        .build()
    );
  }

  @Test
  public void testDelete() {
    registerTestOption(OptionValue.Kind.LONG, "added-option-0",  "0");
    registerTestOption(OptionValue.Kind.LONG, "added-option-1",  "1");
    registerTestOption(OptionValue.Kind.STRING, "not-added-option",  "default-value");

    OptionValue optionValue0 = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "added-option-0", 100);
    OptionValue optionValue1 = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "added-option-1", 111);
    OptionValueProtoList optionList = OptionValueProtoList.newBuilder()
      .addAllOptions(Arrays.asList(
        OptionValueProtoUtils.toOptionValueProto(optionValue0),
        OptionValueProtoUtils.toOptionValueProto(optionValue1)))
      .build();
    when(kvStore.get(OPTIONS_KEY)).thenReturn(optionList);

    // Attempt to option that is not in som should exit early
    som.deleteOption("not-added-option", OptionValue.OptionType.SYSTEM);
    verify(kvStore, times(0)).put(any(), any());

    // regular delete option
    som.deleteOption("added-option-0", OptionValue.OptionType.SYSTEM);
    verify(kvStore, times(1)).put(OPTIONS_KEY,
      OptionValueProtoList.newBuilder()
      .addAllOptions(Collections.singletonList(OptionValueProtoUtils.toOptionValueProto(optionValue1)))
      .build()
    );
  }

  @Test
  public void testDeleteAll() {
    registerTestOption(OptionValue.Kind.LONG, "test-option-0", "0");
    registerTestOption(OptionValue.Kind.LONG, "test-option-1", "1");

    OptionValue optionValue0 = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test-option-0", 100);
    OptionValue optionValue1 = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test-option-1", 111);
    OptionValueProtoList optionList = OptionValueProtoList.newBuilder()
      .addAllOptions(Arrays.asList(
        OptionValueProtoUtils.toOptionValueProto(optionValue0),
        OptionValueProtoUtils.toOptionValueProto(optionValue1)))
      .build();
    when(kvStore.get(OPTIONS_KEY)).thenReturn(optionList);

    som.deleteAllOptions(OptionValue.OptionType.SYSTEM);
    verify(kvStore, times(1)).put(OPTIONS_KEY,
      OptionValueProtoList.newBuilder()
        .addAllOptions(Collections.emptyList())
        .build()
    );
  }

  @Test
  public void testGetNonDefaultOptions() {
    registerTestOption(OptionValue.Kind.LONG, "test-option-0", "0");
    registerTestOption(OptionValue.Kind.LONG, "test-option-1", "1");

    OptionValue optionValue0 = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test-option-0", 100);
    OptionValue optionValue1 = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test-option-1", 111);

    OptionValueProtoList optionList = OptionValueProtoList.newBuilder()
      .addAllOptions(Arrays.asList(
        OptionValueProtoUtils.toOptionValueProto(optionValue0),
        OptionValueProtoUtils.toOptionValueProto(optionValue1)))
      .build();
    when(kvStore.get(OPTIONS_KEY)).thenReturn(optionList);

    assertThat(som.getNonDefaultOptions(), containsInAnyOrder(optionValue0, optionValue1));
  }

  @Test
  public void testIterator() {
    registerTestOption(OptionValue.Kind.LONG, "test-option-0", "0");
    registerTestOption(OptionValue.Kind.LONG, "test-option-1", "1");

    OptionValue optionValue0 = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test-option-0", 100);
    OptionValue optionValue1 = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test-option-1", 111);

    OptionValueProtoList optionList = OptionValueProtoList.newBuilder()
      .addAllOptions(Arrays.asList(
        OptionValueProtoUtils.toOptionValueProto(optionValue0),
        OptionValueProtoUtils.toOptionValueProto(optionValue1)))
      .build();
    when(kvStore.get(OPTIONS_KEY)).thenReturn(optionList);

    assertThat(Lists.from(som.iterator()), containsInAnyOrder(optionValue0, optionValue1));
  }

  @Test
  public void testIsSet() {
    registerTestOption(OptionValue.Kind.LONG, "set-option", "0");
    registerTestOption(OptionValue.Kind.LONG, "not-set-option", "1");

    OptionValue optionValue = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "set-option", 123);
    OptionValueProtoList optionList = OptionValueProtoList.newBuilder()
      .addAllOptions(Collections.singletonList(OptionValueProtoUtils.toOptionValueProto(optionValue)))
      .build();
    when(kvStore.get(OPTIONS_KEY)).thenReturn(optionList);

    assertTrue(som.isSet("set-option"));
    assertFalse(som.isSet("not-set-option"));
  }

  @Test
  public void testIsValid() {
    registerTestOption(OptionValue.Kind.LONG, "valid-option", "0");
    // invalid options should not be registered

    assertTrue(som.isValid("valid-option"));
    assertFalse(som.isValid("invalid-option"));
  }
}
