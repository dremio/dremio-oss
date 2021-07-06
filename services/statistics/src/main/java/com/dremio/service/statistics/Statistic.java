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
package com.dremio.service.statistics;

import com.dremio.service.statistics.proto.StatisticMessage;

import io.protostuff.ByteString;


/**
 * Statistic
 */
public class Statistic {

  /**
   * Type of statistics
   */
  public enum StatisticType {
    RCOUNT,
    COLRCOUNT,
    NDV,
    TDIGEST,
    CMS;
  }

  private final StatisticMessage statisticMessage;

  public Statistic() {
    this.statisticMessage = new StatisticMessage();
  }

  public Statistic(StatisticMessage statisticMessage) {
    this.statisticMessage = statisticMessage;
  }

  public StatisticMessage getStatisticMessage() {
    return statisticMessage;
  }

  public String getTag() {
    return statisticMessage.getTag();
  }

  public Long getNdv() {
    return statisticMessage.getNdv();
  }

  public Long getRowCount() {
    return statisticMessage.getRowCount();
  }

  public Long getColumnRowCount() {
    return statisticMessage.getColumnRowCount();
  }

  public Long getCreatedAt() {
    return statisticMessage.getCreatedAt();
  }

  public void setCreatedAt(long currentTimeMillis) {
    statisticMessage.setCreatedAt(currentTimeMillis);
  }

  public HistogramImpl getHistogram() {
    if (statisticMessage.getSerializedTdigest() == null) {
      return null;
    }
    return new HistogramImpl(statisticMessage.getSerializedTdigest().asReadOnlyByteBuffer());
  }

  /**
   * Statistics Builder
   */
  public static class StatisticBuilder {
    private final Statistic statistic;

    public StatisticBuilder() {
      this.statistic = new Statistic();
    }

    public StatisticBuilder(Statistic statistic) {
      this.statistic = statistic;

    }

    public void update(StatisticType type, Object value) {
      if (value == null) {
        return;
      }
      switch (type) {
        case RCOUNT: {
          long rCount = ((Number) value).longValue();
          statistic.statisticMessage.setRowCount(rCount);
        }
        break;
        case COLRCOUNT: {
          long colRowCount = ((Number) value).longValue();
          statistic.statisticMessage.setColumnRowCount(colRowCount);
        }
        break;
        case NDV: {
          long ndv = ((Number) value).longValue();
          statistic.statisticMessage.setNdv(ndv);
        }
        break;
        case TDIGEST: {
          byte[] byteArray = (byte[]) value;
          statistic.statisticMessage.setSerializedTdigest(ByteString.copyFrom(byteArray));
        }
        break;
        case CMS:
        default:
          throw new UnsupportedOperationException("Statistics type, " + type.toString() + ", is not supported");
      }
    }

    public Statistic build() {
      return statistic;
    }
  }

}




