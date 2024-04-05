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
package com.dremio.common.logical.data;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.logical.data.Order.Ordering;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.junit.Test;

public class OrderTest {

  //////////
  // Order.Ordering tests:

  // "Round trip" tests that strings from output work as input:

  @Test
  public void test_Ordering_roundTripAscAndNullsFirst() {
    Ordering src = new Ordering(Direction.ASCENDING, null, NullDirection.FIRST);
    Ordering reconstituted = new Ordering(src.getDirection(), null, src.getNullDirection());
    assertThat(reconstituted.getDirection()).isEqualTo(RelFieldCollation.Direction.ASCENDING);
    assertThat(reconstituted.getNullDirection()).isEqualTo(NullDirection.FIRST);
  }

  @Test
  public void test_Ordering_roundTripDescAndNullsLast() {
    Ordering src = new Ordering(Direction.DESCENDING, null, NullDirection.LAST);
    Ordering reconstituted = new Ordering(src.getDirection(), null, src.getNullDirection());
    assertThat(reconstituted.getDirection()).isEqualTo(RelFieldCollation.Direction.DESCENDING);
    assertThat(reconstituted.getNullDirection()).isEqualTo(NullDirection.LAST);
  }

  @Test
  public void test_Ordering_roundTripDescAndNullsUnspecified() {
    Ordering src = new Ordering(Direction.DESCENDING, null, NullDirection.UNSPECIFIED);
    Ordering reconstituted = new Ordering(src.getDirection(), null, src.getNullDirection());
    assertThat(reconstituted.getDirection()).isEqualTo(RelFieldCollation.Direction.DESCENDING);
    assertThat(reconstituted.getNullDirection()).isEqualTo(NullDirection.UNSPECIFIED);
  }

  // Basic input validation:
  @Test(expected = IllegalArgumentException.class) // (Currently.)
  public void test_Ordering_garbageOrderRejected() {
    new Ordering("AS CE ND IN G", null, null);
  }

  @Test(expected = IllegalArgumentException.class) // (Currently.)
  public void test_Ordering_garbageNullOrderingRejected() {
    new Ordering(null, null, "HIGH");
  }

  // Defaults-value/null-strings test:

  @Test
  public void testOrdering_nullStrings() {
    Ordering ordering = new Ordering((String) null, null, null);
    assertThat(ordering.getDirection()).isEqualTo(RelFieldCollation.Direction.ASCENDING);
    assertThat(ordering.getNullDirection()).isEqualTo(RelFieldCollation.NullDirection.UNSPECIFIED);
    assertThat(ordering.getOrder()).isEqualTo("ASC");
  }
}
