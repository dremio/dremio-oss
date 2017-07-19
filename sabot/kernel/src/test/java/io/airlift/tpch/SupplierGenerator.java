/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.record.VectorContainer;

import io.airlift.tpch.GenerationDefinition.TpchTable;

public class SupplierGenerator extends TpchGenerator {

  public static final int SCALE_BASE = TpchTable.SUPPLIER.scaleBase;

  private static final int ACCOUNT_BALANCE_MIN = -99999;
  private static final int ACCOUNT_BALANCE_MAX = 999999;
  private static final int ADDRESS_AVERAGE_LENGTH = 25;
  private static final int COMMENT_AVERAGE_LENGTH = 63;

  public static final String BBB_BASE_TEXT = "Customer ";
  public static final String BBB_COMPLAINT_TEXT = "Complaints";
  public static final String BBB_RECOMMEND_TEXT = "Recommends";
  public static final int BBB_COMMENT_LENGTH = BBB_BASE_TEXT.length() + BBB_COMPLAINT_TEXT.length();
  public static final int BBB_COMMENTS_PER_SCALE_BASE = 10;
  public static final int BBB_COMPLAINT_PERCENT = 50;

  private final RandomAlphaNumeric addressRandom = randomAlphaNumeric(706178559, ADDRESS_AVERAGE_LENGTH);
  private final RandomBoundedInt nationKeyRandom = randomBoundedInt(110356601, 0, DISTRIBUTIONS.getNations().size() - 1);
  private final RandomPhoneNumber phoneRandom = randomPhoneNumber(884434366);
  private final RandomBoundedInt accountBalanceRandom = randomBoundedInt(962338209, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX);
  private final RandomText commentRandom = randomText(1341315363, TEXT_POOL, COMMENT_AVERAGE_LENGTH);

  private final RandomBoundedInt bbbCommentRandom = randomBoundedInt(202794285, 1, SCALE_BASE);
  private final RandomInt bbbJunkRandom = randomInt(263032577, 1);
  private final RandomInt bbbOffsetRandom = randomInt(715851524, 1);
  private final RandomBoundedInt bbbTypeRandom = randomBoundedInt(753643799, 0, 100);

  public SupplierGenerator(BufferAllocator allocator, GenerationDefinition def, int partIndex, String...includedColumns) {
    super(TpchTable.SUPPLIER, allocator, def, partIndex, includedColumns);

    // setup fields


    finalizeSetup();
  }

  @Override
  protected void generateRecord(long supplierKey, int outputIndex) {
    // TODO Auto-generated method stub

    String comment = commentRandom.nextValue();

    // Add supplier complaints or commendation to the comment
    int bbbCommentRandomValue = bbbCommentRandom.nextValue();
    if (bbbCommentRandomValue <= BBB_COMMENTS_PER_SCALE_BASE) {
      StringBuilder buffer = new StringBuilder(comment);

      // select random place for BBB comment
      int noise = bbbJunkRandom.nextInt(0, (comment.length() - BBB_COMMENT_LENGTH));
      int offset = bbbOffsetRandom.nextInt(0, (comment.length() - (BBB_COMMENT_LENGTH + noise)));

      // select complaint or recommendation
      String type;
      if (bbbTypeRandom.nextValue() < BBB_COMPLAINT_PERCENT) {
        type = BBB_COMPLAINT_TEXT;
      } else {
        type = BBB_RECOMMEND_TEXT;
      }

      // write base text (e.g., "Customer ")
      buffer.replace(offset, offset + BBB_BASE_TEXT.length(), BBB_BASE_TEXT);

      // write complaint or commendation text (e.g., "Complaints" or
      // "Recommends")
      buffer.replace(BBB_BASE_TEXT.length() + offset + noise, BBB_BASE_TEXT.length() + offset + noise + type.length(),
          type);

      comment = buffer.toString();
    }

    long nationKey = nationKeyRandom.nextValue();


//    supplierKey,
//    supplierKey,
//    String.format("Supplier#%09d", supplierKey),
//    addressRandom.nextValue(),
//    nationKey,
//    phoneRandom.nextValue(nationKey),
//    accountBalanceRandom.nextValue(),
//    comment

  }


}
