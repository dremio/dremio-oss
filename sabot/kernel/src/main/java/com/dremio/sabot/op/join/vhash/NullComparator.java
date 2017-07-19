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
package com.dremio.sabot.op.join.vhash;

import java.util.BitSet;

import io.netty.util.internal.PlatformDependent;

/**
 * Determines whether a particular pivot set of bits allows comparison.
 */
public class NullComparator {

  public static enum Mode {NONE, FOUR, EIGHT, BIG};

  private final long[] bigs;
  private final Mode mode;
  private final int four;
  private final long eight;

  public NullComparator(BitSet mask, int count) {
    super();
    this.bigs = mask.toLongArray();
    if(mask.cardinality() == 0){
      mode = Mode.NONE;
      four = 0;
      eight = 0;
    }else{
      if(count > 64){
        mode = Mode.BIG;
        four = 0;
        eight = 0;
      }else if(count > 32){
        mode = Mode.EIGHT;
        four = 0;
        eight = bigs[0];
      } else {
        mode = Mode.FOUR;
        four = (int) bigs[0];
        eight = 0;
      }
    }

  }

  public boolean isComparableBigBits(long pos){
    assert mode == Mode.BIG;
    for(int i =0; i < bigs.length; i++, pos += 8){
      long nullMask = bigs[i];
      if((PlatformDependent.getLong(pos) & nullMask) != nullMask){
        return false;
      }
    }
    return true;
  }

  public Mode getMode() {
    return mode;
  }

  public int getFour() {
    return four;
  }

  public long getEight() {
    return eight;
  }


}
