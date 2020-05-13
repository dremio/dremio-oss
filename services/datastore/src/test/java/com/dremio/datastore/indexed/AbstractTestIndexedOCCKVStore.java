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
package com.dremio.datastore.indexed;

import org.junit.Test;

import com.dremio.datastore.OCCKVStoreTests;
import com.dremio.datastore.generator.supplier.UniqueSupplierOptions;
import com.dremio.datastore.indexed.doughnut.DoughnutStoreGenerator;

/**
 * Tests index and OCC operations against an OCCIndexedKVStore
 */
public abstract class AbstractTestIndexedOCCKVStore extends AbstractTestIndexedStore {

  private DoughnutStoreGenerator gen = new DoughnutStoreGenerator(UniqueSupplierOptions.FIXED_LENGTH);

  @Test(expected = NullPointerException.class)
  public void testShouldNotPutNulls() {
    OCCKVStoreTests.testShouldNotPutNulls(getKvStore(), gen);
  }

  @Test
  public void testPutWithoutCreateOptionIsValid() {
    OCCKVStoreTests.testPutWithoutCreateOptionIsValid(getKvStore(), gen);
  }

  @Test
  public void testPutWithCreateOption() {
    OCCKVStoreTests.testPutWithCreateOption(getKvStore(), gen);
  }

  @Test
  public void testPutWithCreateOptionFailsToOverwrite() {
    OCCKVStoreTests.testPutWithCreateOptionFailsToOverwrite(getKvStore(), gen);
  }

  @Test
  public void testModificationWithoutOptionsAlwaysOverwrites() {
    OCCKVStoreTests.testModificationWithoutOptionsAlwaysOverwrites(getKvStore(), gen);
  }

  @Test
  public void testPutAfterDeleteWithoutOptions() {
    OCCKVStoreTests.testPutAfterDeleteWithoutOptions(getKvStore(), gen);
  }

  @Test
  public void testPutAlwaysUsingOptionsShouldUpdate() {
    OCCKVStoreTests.testPutAlwaysUsingOptionsShouldUpdate(getKvStore(), gen);
  }

  @Test
  public void testPutLateOptionAdoptionShouldStillUpdate() {
    OCCKVStoreTests.testPutLateOptionAdoptionShouldStillUpdate(getKvStore(), gen);
  }

  @Test
  public void testPutLateOptionAdoptionShouldStillProtectFromCM() {
    OCCKVStoreTests.testPutLateOptionAdoptionShouldStillProtectFromCM(getKvStore(), gen);
  }

  @Test
  public void testCreateWithManyOptionsFails() {
    OCCKVStoreTests.testCreateWithManyOptionsFails(getKvStore(), gen);
  }

  @Test
  public void testPutWithMultipleVersionsFails() {
    OCCKVStoreTests.testPutWithMultipleVersionsFails(getKvStore(), gen);
  }

  @Test
  public void testPutWithOutdatedVersionFails() {
    OCCKVStoreTests.testPutWithOutdatedVersionFails(getKvStore(), gen);
  }

  @Test
  public void testDeleteWithoutTagAlwaysDeletes() {
    OCCKVStoreTests.testDeleteWithoutTagAlwaysDeletes(getKvStore(), gen);
  }

  @Test
  public void testDeleteWithOutdatedVersionFails() {
    OCCKVStoreTests.testDeleteWithOutdatedVersionFails(getKvStore(), gen);
  }

  @Test
  public void testDeleteWithLateOptionShouldStillProtectFromCM() {
    OCCKVStoreTests.testDeleteWithLateOptionShouldStillProtectFromCM(getKvStore(), gen);
  }

  @Test
  public void testDeleteWithLatestVersionShouldDelete() {
    OCCKVStoreTests.testDeleteWithLatestVersionShouldDelete(getKvStore(), gen);
  }

  @Test
  public void testDeleteWithLateVersionAdoptionShouldDelete() {
    OCCKVStoreTests.testDeleteWithLateVersionAdoptionShouldDelete(getKvStore(), gen);
  }

  @Test
  public void testDeleteWithManyVersionsFails() {
    OCCKVStoreTests.testDeleteWithManyVersionsFails(getKvStore(), gen);
  }
}
