/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
/*
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
package io.airlift.tpch;

import static java.util.Locale.ENGLISH;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import io.airlift.tpch.GenerationDefinition.TpchTable;

import java.time.LocalDate;

class CustomerGenerator extends TpchGenerator {
  public static final int SCALE_BASE = 150_000;
  private static final int ACCOUNT_BALANCE_MIN = -99999;
  private static final int ACCOUNT_BALANCE_MAX = 999999;
  private static final int ADDRESS_AVERAGE_LENGTH = 25;
  private static final int COMMENT_AVERAGE_LENGTH = 73;
  private static final int YEAR_MIN = 1900;
  private static final int YEAR_MAX = 2050;

  private final RandomAlphaNumeric addressRandom = randomAlphaNumeric(881155353, ADDRESS_AVERAGE_LENGTH);
  private final RandomBoundedInt nationKeyRandom =  randomBoundedInt(1489529863, 0, DISTRIBUTIONS.getNations().size() - 1);;
  private final RandomPhoneNumber phoneRandom = randomPhoneNumber(1521138112);
  private final RandomBoundedInt accountBalanceRandom = randomBoundedInt(298370230, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX);
  private final RandomString marketSegmentRandom = randomString(1140279430, DISTRIBUTIONS.getMarketSegments());
  private final RandomText commentRandom = randomText(1335826707, TEXT_POOL, COMMENT_AVERAGE_LENGTH);
  private final RandomBoundedInt yearRandom = randomBoundedInt(298370321, YEAR_MIN,
    YEAR_MAX);

  private final BigIntVector customerKey;
  private final BigIntVector nationKey;
  private final BigIntVector accountBalance;

  private final VarCharVector customerName;
  private final VarCharVector address;
  private final VarCharVector phone;
  private final VarCharVector marketSegment;
  private final VarCharVector comment;
  private final VarCharVector date;

  public CustomerGenerator(BufferAllocator allocator, GenerationDefinition def, int partitionIndex, String...includedColumns) {
    super(TpchTable.CUSTOMER, allocator, def, partitionIndex, includedColumns);

    // setup fields.
    this.customerKey = int8("c_custkey");
    this.customerName = varChar("c_name");
    this.address = varChar("c_address");
    this.nationKey = int8("c_nationkey");
    this.phone = varChar("c_phone");
    this.accountBalance = int8("c_acctbal");
    this.marketSegment = varChar("c_mktsegment");
    this.comment = varChar("c_comment");
    this.date = varChar("c_date");

    finalizeSetup();
  }

  protected void generateRecord(long globalRecordIndex, int outputIndex){
    final long customerKey = globalRecordIndex;
    final long nationKey = nationKeyRandom.nextValue();

    this.customerKey.setSafe(outputIndex, customerKey);
    this.nationKey.setSafe(outputIndex, nationKey);
    this.accountBalance.setSafe(outputIndex, accountBalanceRandom.nextValue());

    set(outputIndex, customerName, String.format(ENGLISH, "Customer#%09d", customerKey));
    set(outputIndex, address, addressRandom.nextValue());
    set(outputIndex, phone, phoneRandom.nextValue(nationKey));
    set(outputIndex, marketSegment, marketSegmentRandom.nextValue());
    set(outputIndex, comment, commentRandom.nextValue());
    set(outputIndex, date, LocalDate.of(yearRandom.nextValue(),1,1).toString());
  }

}
