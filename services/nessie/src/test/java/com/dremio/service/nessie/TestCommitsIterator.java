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
package com.dremio.service.nessie;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Test;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.WithHash;

import com.google.common.collect.ImmutableMap;

/**
 * Test cases for {@code CommitsIterator}.
 *
 * Copied over from the module org.projectnessie:nessie-versioned-memory. The original Java
 * package was org.projectnessie.versioned.memory.
 */
public class TestCommitsIterator {

  private static final Hash HASH_OF_1 = Hash.of("01");
  private static final Hash HASH_OF_2 = Hash.of("02");
  private static final Hash HASH_OF_3 = Hash.of("03");
  private static final Hash HASH_OF_4 = Hash.of("04");

  private static final Commit<String, String> FIRST_COMMIT =
    new Commit<String, String>(HASH_OF_1, Commit.NO_ANCESTOR, "initial commit", Collections.emptyList());
  private static final Commit<String, String> SECOND_COMMIT =
    new Commit<String, String>(HASH_OF_2, HASH_OF_1, "2nd commit", Collections.emptyList());
  private static final Commit<String, String> THIRD_COMMIT =
    new Commit<String, String>(HASH_OF_3, HASH_OF_2, "3rd commit", Collections.emptyList());
  private static final Commit<String, String> FOURTH_COMMIT =
    new Commit<String, String>(HASH_OF_4, HASH_OF_3, "4th commit", Collections.emptyList());

  private static final ImmutableMap<Hash, Commit<String, String>> COMMITS = ImmutableMap.<Hash, Commit<String, String>>builder()
    .put(HASH_OF_1, FIRST_COMMIT)
    .put(HASH_OF_2, SECOND_COMMIT)
    .put(HASH_OF_3, THIRD_COMMIT)
    .put(HASH_OF_4, FOURTH_COMMIT)
    .build();

  @Test(expected = NoSuchElementException.class)
  public void testIterator() {
    final Iterator<WithHash<Commit<String, String>>> iterator = new CommitsIterator<>(COMMITS::get, HASH_OF_4);
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_4, FOURTH_COMMIT)));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_3, THIRD_COMMIT)));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_2, SECOND_COMMIT)));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_1, FIRST_COMMIT)));
    assertThat(iterator.hasNext(), is(false));
    iterator.next();
  }

  @Test(expected = NoSuchElementException.class)
  public void testNoHasNextCheck() {
    final Iterator<WithHash<Commit<String, String>>> iterator = new CommitsIterator<>(COMMITS::get, HASH_OF_4);
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_4, FOURTH_COMMIT)));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_3, THIRD_COMMIT)));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_2, SECOND_COMMIT)));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_1, FIRST_COMMIT)));
    iterator.next();
  }

  @Test(expected = NoSuchElementException.class)
  public void testMultipleHasNextChecks() {
    final Iterator<WithHash<Commit<String, String>>> iterator = new CommitsIterator<>(COMMITS::get, HASH_OF_4);
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_4, FOURTH_COMMIT)));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_3, THIRD_COMMIT)));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_2, SECOND_COMMIT)));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_1, FIRST_COMMIT)));
    assertThat(iterator.hasNext(), is(false));
    assertThat(iterator.hasNext(), is(false));
    iterator.next();
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidState() {
    final Map<Hash, Commit<String, String>> commits = ImmutableMap.of(HASH_OF_4, FOURTH_COMMIT);
    final Iterator<WithHash<Commit<String, String>>> iterator = new CommitsIterator<>(commits::get, HASH_OF_4);
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(WithHash.of(HASH_OF_4, FOURTH_COMMIT)));
    try {
      iterator.hasNext();
      fail();
    } catch (IllegalStateException e) {
      iterator.next();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidStateEmptyCommitMap() {
    final Map<Hash, Commit<String, String>> commits = ImmutableMap.of();
    final Iterator<WithHash<Commit<String, String>>> iterator = new CommitsIterator<>(commits::get, HASH_OF_4);
    try {
      iterator.hasNext();
      fail();
    } catch (IllegalStateException e) {
      iterator.next();
    }
  }

  @Test(expected = NoSuchElementException.class)
  public void testEmpty() {
    final Iterator<WithHash<Commit<String, String>>> iterator = new CommitsIterator<>(COMMITS::get, Commit.NO_ANCESTOR);
    assertThat(iterator.hasNext(), is(false));
    iterator.next();
  }
}
