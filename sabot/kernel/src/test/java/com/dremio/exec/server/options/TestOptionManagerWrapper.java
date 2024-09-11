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

import static com.dremio.exec.ExecConstants.ENABLE_VERBOSE_ERRORS_KEY;
import static com.dremio.exec.ExecConstants.SLICE_TARGET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.test.DremioTest;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOptionManagerWrapper extends DremioTest {
  private static LogicalPlanPersistence lpp;
  private static OptionValidatorListing optionValidatorListing;

  private LegacyKVStoreProvider storeProvider;
  private DefaultOptionManager defaultOptionManager;
  private SystemOptionManager systemOptionManager;
  private SessionOptionManager sessionOptionManager;
  private QueryOptionManager queryOptionManager;

  @BeforeClass
  public static void setupClass() throws Exception {
    optionValidatorListing = new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
    lpp = new LogicalPlanPersistence(CLASSPATH_SCAN_RESULT);
  }

  @Before
  public void setup() throws Exception {
    storeProvider = LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    defaultOptionManager = new DefaultOptionManager(optionValidatorListing);
    systemOptionManager =
        new SystemOptionManagerImpl(optionValidatorListing, lpp, () -> storeProvider, true);
    sessionOptionManager = new SessionOptionManagerImpl(optionValidatorListing);
    queryOptionManager = new QueryOptionManager(optionValidatorListing);

    storeProvider.start();
    systemOptionManager.start();
  }

  @After
  public void cleanup() throws Exception {
    AutoCloseables.close(systemOptionManager, storeProvider);
  }

  @Test
  public void testGetAndSetOption() throws Exception {
    String testOptionName = SLICE_TARGET;
    OptionManager optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionValidatorProvider(optionValidatorListing)
            .withOptionManager(defaultOptionManager)
            .withOptionManager(systemOptionManager)
            .build();
    OptionValue optionValue = optionManager.getOption(testOptionName);

    // Sanity check default value
    assertEquals(
        optionValidatorListing.getValidator(testOptionName).getDefault().getNumVal(),
        optionValue.getNumVal());

    long newValue = 10;
    optionManager.setOption(
        OptionValue.createLong(OptionValue.OptionType.SYSTEM, testOptionName, newValue));
    optionValue = optionManager.getOption(testOptionName);
    assertEquals((Long) newValue, optionValue.getNumVal());
  }

  @Test
  public void testSetFallback() throws Exception {
    String testOptionName = SLICE_TARGET;

    OptionManager optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionValidatorProvider(optionValidatorListing)
            .withOptionManager(defaultOptionManager)
            .withOptionManager(systemOptionManager)
            .withOptionManager(sessionOptionManager)
            .withOptionManager(queryOptionManager)
            .build();

    // Set value and check specific OM's for value
    long newValue1 = 10;
    OptionValue optionValue1 =
        OptionValue.createLong(OptionValue.OptionType.SESSION, testOptionName, newValue1);
    optionManager.setOption(optionValue1);
    assertEquals(sessionOptionManager.getOption(testOptionName), optionValue1);
    // Check other OM's and make sure option was not set
    assertEquals(null, systemOptionManager.getOption(testOptionName));
    assertEquals(null, queryOptionManager.getOption(testOptionName));

    // Set another option
    long newValue2 = 20;
    OptionValue optionValue2 =
        OptionValue.createLong(OptionValue.OptionType.SYSTEM, testOptionName, newValue2);
    optionManager.setOption(optionValue2);
    assertEquals(optionValue2, systemOptionManager.getOption(testOptionName));
    // Check previously set option is still present
    assertEquals(optionValue1, sessionOptionManager.getOption(testOptionName));
    // Check other OM's and make sure option was not set
    assertEquals(null, queryOptionManager.getOption(testOptionName));
  }

  @Test
  public void testGetFallback() throws Exception {
    String testOptionName = SLICE_TARGET;

    OptionManager optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionValidatorProvider(optionValidatorListing)
            .withOptionManager(defaultOptionManager)
            .withOptionManager(systemOptionManager)
            .withOptionManager(sessionOptionManager)
            .withOptionManager(queryOptionManager)
            .build();

    // Set value directly to specific OM and check specific OM's for value
    long newValue1 = 10;
    OptionValue optionValue1 =
        OptionValue.createLong(OptionValue.OptionType.SYSTEM, testOptionName, newValue1);
    systemOptionManager.setOption(optionValue1);
    assertEquals(optionValue1, optionManager.getOption(testOptionName));
    // Check other OM's and make sure option was not set
    assertEquals(null, sessionOptionManager.getOption(testOptionName));
    assertEquals(null, queryOptionManager.getOption(testOptionName));

    // Set another option
    long newValue2 = 20;
    OptionValue optionValue2 =
        OptionValue.createLong(OptionValue.OptionType.SESSION, testOptionName, newValue2);
    sessionOptionManager.setOption(optionValue2);
    assertEquals(optionValue2, optionManager.getOption(testOptionName));
    // Check previously set option is still present
    assertEquals(optionValue1, systemOptionManager.getOption(testOptionName));
    // Check other OM's and make sure option was not set
    assertEquals(null, queryOptionManager.getOption(testOptionName));
  }

  @Test
  public void testGetNonDefaultOptions() throws Exception {
    OptionManager optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionValidatorProvider(optionValidatorListing)
            .withOptionManager(defaultOptionManager)
            .withOptionManager(systemOptionManager)
            .withOptionManager(sessionOptionManager)
            .withOptionManager(queryOptionManager)
            .build();

    int initialOptionsCount =
        defaultOptionManager.getNonDefaultOptions().size()
            + systemOptionManager.getNonDefaultOptions().size()
            + sessionOptionManager.getNonDefaultOptions().size()
            + queryOptionManager.getNonDefaultOptions().size();

    List<OptionValue> optionValues =
        Arrays.asList(
            OptionValue.createLong(OptionValue.OptionType.SYSTEM, SLICE_TARGET, 10),
            OptionValue.createLong(OptionValue.OptionType.SESSION, SLICE_TARGET, 15),
            OptionValue.createLong(OptionValue.OptionType.QUERY, SLICE_TARGET, 20),
            OptionValue.createBoolean(
                OptionValue.OptionType.SESSION, ENABLE_VERBOSE_ERRORS_KEY, true),
            OptionValue.createBoolean(
                OptionValue.OptionType.QUERY, ENABLE_VERBOSE_ERRORS_KEY, true));
    optionValues.forEach(optionManager::setOption);
    OptionList nonDefaultOptions = optionManager.getNonDefaultOptions();

    // Check all set optionValues are returned
    assertEquals(initialOptionsCount + optionValues.size(), nonDefaultOptions.size());
    for (OptionValue optionValue : optionValues) {
      assertTrue(nonDefaultOptions.contains(optionValue));
    }

    // Check no default options are returned
    for (OptionValue nonDefaultOption : nonDefaultOptions) {
      assertNotEquals(nonDefaultOption, defaultOptionManager.getOption(nonDefaultOption.getName()));
    }
  }

  @Test
  public void testGetDefaultOptions() throws Exception {
    OptionManager optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionValidatorProvider(optionValidatorListing)
            .withOptionManager(defaultOptionManager)
            .withOptionManager(systemOptionManager)
            .withOptionManager(sessionOptionManager)
            .withOptionManager(queryOptionManager)
            .build();
    OptionList defaultOptions = optionManager.getDefaultOptions();

    // Check only default options are returned
    assertEquals(defaultOptionManager.getDefaultOptions().size(), defaultOptions.size());
    for (OptionValue defaultOption : defaultOptions) {
      assertEquals(
          defaultOption, optionValidatorListing.getValidator(defaultOption.getName()).getDefault());
    }
  }

  @Test
  public void testIterator() throws Exception {
    OptionManager optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionValidatorProvider(optionValidatorListing)
            .withOptionManager(defaultOptionManager)
            .withOptionManager(systemOptionManager)
            .withOptionManager(sessionOptionManager)
            .withOptionManager(queryOptionManager)
            .build();

    int initialSystemOptionCount = systemOptionManager.getNonDefaultOptions().size();
    int initialOptionsCount =
        defaultOptionManager.getNonDefaultOptions().size()
            + initialSystemOptionCount
            + sessionOptionManager.getNonDefaultOptions().size()
            + queryOptionManager.getNonDefaultOptions().size();

    int defaultOptionsCount = optionValidatorListing.getValidatorList().size();

    List<OptionValue> optionValues =
        Arrays.asList(
            OptionValue.createLong(OptionValue.OptionType.SYSTEM, SLICE_TARGET, 10),
            OptionValue.createLong(OptionValue.OptionType.SESSION, SLICE_TARGET, 15),
            OptionValue.createLong(OptionValue.OptionType.QUERY, SLICE_TARGET, 20),
            OptionValue.createBoolean(
                OptionValue.OptionType.SESSION, ENABLE_VERBOSE_ERRORS_KEY, true),
            OptionValue.createBoolean(
                OptionValue.OptionType.QUERY, ENABLE_VERBOSE_ERRORS_KEY, true));
    AtomicInteger systemOptionsCount = new AtomicInteger(initialSystemOptionCount);
    optionValues.forEach(
        optionValue -> {
          optionManager.setOption(optionValue);
          if (optionValue.getType().equals(OptionValue.OptionType.SYSTEM)) {
            systemOptionsCount.addAndGet(1);
          }
        });

    OptionList iteratorResult = new OptionList();
    optionManager.iterator().forEachRemaining(iteratorResult::add);

    // Check size, minus systemOptionsCount because set System Option should override default option
    assertEquals(
        initialOptionsCount + defaultOptionsCount + optionValues.size() - systemOptionsCount.get(),
        iteratorResult.size());
  }

  @Test
  public void testNormalOrdering() {
    String testOptionName = SLICE_TARGET;

    // "Normal" ordering
    // SystemOM will take priority
    OptionManager optionManager1 =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionValidatorProvider(optionValidatorListing)
            .withOptionManager(defaultOptionManager)
            .withOptionManager(systemOptionManager)
            .build();
    long newValue1 = 10;
    OptionValue optionValue1 =
        OptionValue.createLong(OptionValue.OptionType.SYSTEM, testOptionName, newValue1);
    optionManager1.setOption(optionValue1);
    assertEquals(optionValue1, optionManager1.getOption(testOptionName));

    OptionList iteratorResult1 = new OptionList();
    optionManager1.iterator().forEachRemaining(iteratorResult1::add);
    assertTrue(iteratorResult1.contains(optionValue1));
  }

  @Test
  public void testFlippedOrdering() {
    String testOptionName = SLICE_TARGET;
    OptionValue testOptionDefault =
        optionValidatorListing.getValidator(testOptionName).getDefault();

    // "Flipped" ordering
    // DefaultOM will take priority
    OptionManager optionManager2 =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionValidatorProvider(optionValidatorListing)
            .withOptionManager(systemOptionManager)
            .withOptionManager(defaultOptionManager)
            .build();
    long newValue2 = 20;
    OptionValue optionValue2 =
        OptionValue.createLong(OptionValue.OptionType.SYSTEM, testOptionName, newValue2);

    try {
      optionManager2.setOption(optionValue2);
      Assert.fail();
    } catch (UnsupportedOperationException ignored) {
      // Expect to hit default option manager, which should fail since set is not supported
    }
    // Sanity check default value is returned
    assertEquals(testOptionDefault, optionManager2.getOption(testOptionName));

    // Sanity check iterator
    OptionList iteratorResult2 = new OptionList();
    optionManager2.iterator().forEachRemaining(iteratorResult2::add);

    assertTrue(iteratorResult2.contains(testOptionDefault));
  }
}
