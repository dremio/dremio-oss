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
package com.dremio.datastore.api.options;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.indexed.ImmutableIndexPutOption;
import com.dremio.datastore.indexed.IndexPutOption;
import java.util.Optional;
import org.junit.Test;

/** Tests OptionUtility methods. */
public class KVStoreOptionUtilityTest {
  private static final VersionOption versionOption =
      new ImmutableVersionOption.Builder().setTag("1").build();
  private static final IndexPutOption indexPutOption =
      new ImmutableIndexPutOption.Builder().build();
  private static final KVStore.PutOption createOption = KVStore.PutOption.CREATE;

  @Test
  public void testValidateOptionsNull() {
    KVStoreOptionUtility.validateOptions((KVStore.PutOption[]) null);
  }

  @Test
  public void testValidateOptionsEmpty() {
    KVStoreOptionUtility.validateOptions(new KVStore.PutOption[] {});
  }

  @Test
  public void testValidateOptionsSingleCreate() {
    KVStoreOptionUtility.validateOptions(new KVStore.PutOption[] {createOption});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateOptionsMultipleCreate() {
    KVStoreOptionUtility.validateOptions(new KVStore.PutOption[] {createOption, createOption});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateOptionsMultipleCreateWithIndex() {
    KVStoreOptionUtility.validateOptions(
        new KVStore.PutOption[] {createOption, indexPutOption, createOption});
  }

  @Test
  public void testValidateOptionsCreateAndIndex() {
    KVStoreOptionUtility.validateOptions(new KVStore.PutOption[] {createOption, indexPutOption});
  }

  @Test
  public void testValidateOptionsIndexAndCreate() {
    KVStoreOptionUtility.validateOptions(new KVStore.PutOption[] {indexPutOption, createOption});
  }

  @Test
  public void testValidateOptionsSingleVersion() {
    KVStoreOptionUtility.validateOptions(new KVStore.PutOption[] {versionOption});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateOptionsMultipleVersion() {
    KVStoreOptionUtility.validateOptions(new KVStore.PutOption[] {versionOption, versionOption});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateOptionsMultipleVersionWithIndex() {
    KVStoreOptionUtility.validateOptions(
        new KVStore.PutOption[] {versionOption, indexPutOption, versionOption});
  }

  @Test
  public void testValidateOptionsVersionAndIndex() {
    KVStoreOptionUtility.validateOptions(new KVStore.PutOption[] {versionOption, indexPutOption});
  }

  @Test
  public void testValidateOptionsIndexAndVersion() {
    KVStoreOptionUtility.validateOptions(new KVStore.PutOption[] {indexPutOption, versionOption});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateOptionsConflict() {
    KVStoreOptionUtility.validateOptions(new KVStore.PutOption[] {createOption, versionOption});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateOptionsConflictWithIndex() {
    KVStoreOptionUtility.validateOptions(
        new KVStore.PutOption[] {indexPutOption, createOption, versionOption});
  }

  @Test
  public void testIndexPutOptionNotSupportedNull() {
    KVStoreOptionUtility.checkIndexPutOptionIsNotUsed();
  }

  @Test
  public void testIndexPutOptionNotSupportedEmpty() {
    KVStoreOptionUtility.checkIndexPutOptionIsNotUsed(new KVStore.PutOption[] {});
  }

  @Test
  public void testIndexPutOptionNotSupportedSingleCreate() {
    KVStoreOptionUtility.checkIndexPutOptionIsNotUsed(new KVStore.PutOption[] {createOption});
  }

  @Test
  public void testIndexPutOptionNotSupportedSingleVersion() {
    KVStoreOptionUtility.checkIndexPutOptionIsNotUsed(new KVStore.PutOption[] {versionOption});
  }

  @Test
  public void testIndexPutOptionNotSupportedCreateAndVersion() {
    KVStoreOptionUtility.checkIndexPutOptionIsNotUsed(
        new KVStore.PutOption[] {createOption, versionOption});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIndexPutOptionNotSupportedIndexPutOptionFound() {
    KVStoreOptionUtility.checkIndexPutOptionIsNotUsed(new KVStore.PutOption[] {indexPutOption});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIndexPutOptionNotSupportedIndexPutOptionFoundAmongMany() {
    KVStoreOptionUtility.checkIndexPutOptionIsNotUsed(
        new KVStore.PutOption[] {createOption, indexPutOption, versionOption});
  }

  @Test
  public void testGetCreateOrVersionOptionNull() {
    // Act
    final Optional<KVStore.PutOption> ret = KVStoreOptionUtility.getCreateOrVersionOption();

    // Assert
    assertFalse(ret.isPresent());
  }

  @Test
  public void testGetCreateOrVersionOptionEmpty() {
    // Act
    final Optional<KVStore.PutOption> ret =
        KVStoreOptionUtility.getCreateOrVersionOption(new KVStore.PutOption[] {});

    // Assert
    assertFalse(ret.isPresent());
  }

  @Test
  public void testGetCreateOrVersionOptionSingleCreate() {
    // Act
    final Optional<KVStore.PutOption> ret =
        KVStoreOptionUtility.getCreateOrVersionOption(new KVStore.PutOption[] {createOption});

    // Assert
    assertEquals(createOption, ret.get());
  }

  @Test
  public void testGetCreateOrVersionOptionSingleVersion() {
    // Act
    final Optional<KVStore.PutOption> ret =
        KVStoreOptionUtility.getCreateOrVersionOption(new KVStore.PutOption[] {versionOption});

    // Assert
    assertEquals(versionOption, ret.get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCreateOrVersionOptionCreateAndVersion() {
    // Act
    KVStoreOptionUtility.getCreateOrVersionOption(
        new KVStore.PutOption[] {createOption, versionOption});
  }

  @Test
  public void testGetCreateOrVersionOptionIndexPutOptionFound() {
    // Act
    final Optional<KVStore.PutOption> ret =
        KVStoreOptionUtility.getCreateOrVersionOption(new KVStore.PutOption[] {indexPutOption});

    // Assert
    assertFalse(ret.isPresent());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCreateOrVersionOptionPutOptionFoundAmongMany() {
    // Act
    KVStoreOptionUtility.getCreateOrVersionOption(
        new KVStore.PutOption[] {createOption, indexPutOption, versionOption});
  }

  public void testRemoveIndexPutOptions(
      KVStore.PutOption[] testPutOptions, KVStore.PutOption[] expectedPutOptions) {
    // Act
    final KVStore.PutOption[] actualPutOptions =
        KVStoreOptionUtility.removeIndexPutOption(testPutOptions);

    // Assert
    assertArrayEquals(expectedPutOptions, actualPutOptions);
  }

  @Test
  public void testRemoveIndexPutOptionNull() {
    testRemoveIndexPutOptions(null, null);
  }

  @Test
  public void testRemoveIndexPutOptionEmptyArray() {
    testRemoveIndexPutOptions(new KVStore.PutOption[] {}, new KVStore.PutOption[] {});
  }

  @Test
  public void testRemoveIndexPutOptionWithVersionOption() {
    testRemoveIndexPutOptions(
        new KVStore.PutOption[] {indexPutOption, versionOption},
        new KVStore.PutOption[] {versionOption});
  }

  @Test
  public void testRemoveIndexPutOption() {
    testRemoveIndexPutOptions(new KVStore.PutOption[] {indexPutOption}, new KVStore.PutOption[] {});
  }

  @Test
  public void testRemoveIndexPutOptionWithCreateOption() {
    testRemoveIndexPutOptions(
        new KVStore.PutOption[] {indexPutOption, createOption},
        new KVStore.PutOption[] {createOption});
  }

  @Test
  public void testRemoveIndexPutOptionWithCreateAndVersionOptions() {
    testRemoveIndexPutOptions(
        new KVStore.PutOption[] {versionOption, indexPutOption, createOption},
        new KVStore.PutOption[] {versionOption, createOption});
  }
}
