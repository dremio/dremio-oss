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
package com.dremio.plugins.elastic;

import java.util.List;

import javax.annotation.Nullable;

import org.joda.time.LocalDateTime;
import org.joda.time.LocalDateTimes;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


/**
 * Utilities to match elasticsearch's various date formats.
 */
public final class DateFormats {

  private static final Logger logger = LoggerFactory.getLogger(DateFormats.class);

  public static final String EPOCH_MILLIS = "epoch_millis";
  public static final List<String> DEFAULT_FORMAT = ImmutableList.of("date_optional_time", EPOCH_MILLIS);
  public static final FormatterAndType[] DEFAULT_FORMATTERS = {getFormatterAndType("dateOptionalTime"), getFormatterAndType(EPOCH_MILLIS)};

  public static class FormatterAndType {

    private final DateTimeFormatter formatter;
    private final DateTimeFormatter printer;
    private final ElasticMappingSet.Type type;
    private final boolean isEpochMillis;

    public FormatterAndType(final DateTimeFormatter formatter, final ElasticMappingSet.Type type) {
      this(formatter, formatter, type, false);
    }

    public FormatterAndType(final DateTimeFormatter formatter, final DateTimeFormatter printer, final ElasticMappingSet.Type type, final boolean isEpochMillis) {
      Preconditions.checkArgument((formatter == null && printer == null) || (formatter.isParser() && printer.isPrinter()));
      this.formatter = formatter == null ? null : formatter.withZoneUTC();
      this.printer = printer == null ? null : printer.withZoneUTC();
      this.type = type;
      this.isEpochMillis = isEpochMillis;
    }

    public long parseToLong(String value){
      if (formatter == null) {
        if (isEpochMillis) {
          return Long.parseLong(value);
        } else {
          // isEpochSeconds
          return Long.parseLong(value) * 1000;
        }
      }
      return LocalDateTimes.getLocalMillis(formatter.parseLocalDateTime(value));
    }

    public LocalDateTime parse(String value) {
      if (formatter == null) {
        if (isEpochMillis) {
          return new LocalDateTime(Long.parseLong(value), ISOChronology.getInstanceUTC());
        } else {
          // isEpochSeconds
          return new LocalDateTime(Long.parseLong(value) * 1000, ISOChronology.getInstanceUTC());
        }
      }
      return formatter.parseLocalDateTime(value);
    }

    public String print(LocalDateTime value) {
      if (printer == null) {
        if (isEpochMillis) {
          return Long.toString(com.dremio.common.util.DateTimes.toMillis(value));
        } else {
          return Long.toString(com.dremio.common.util.DateTimes.toMillis(value) / 1000);
        }
      }
      return printer.print(com.dremio.common.util.DateTimes.toDateTime(value));
    }

    public ElasticMappingSet.Type type() {
      return type;
    }
  }

  public static ElasticMappingSet.Type getType(final List<String> formats) {
    Preconditions.checkArgument(formats != null && !formats.isEmpty());
    ElasticMappingSet.Type knownType = null;
    for (String format : formats) {
      final FormatterAndType formatterAndType = getFormatterAndType(format);
      final ElasticMappingSet.Type formatType = formatterAndType.type();
      if (formatType == ElasticMappingSet.Type.TIMESTAMP) {
        return ElasticMappingSet.Type.TIMESTAMP;
      }

      if (knownType == null) {
        knownType = formatType;
      } else {
        if (knownType == formatType) {
          continue;
        }
        if ((knownType == ElasticMappingSet.Type.TIME && formatType == ElasticMappingSet.Type.DATE) || (knownType == ElasticMappingSet.Type.DATE && formatType == ElasticMappingSet.Type.TIME)) {
          return ElasticMappingSet.Type.TIMESTAMP;
        }
      }
    }
    return knownType;
  }


  public static List<String> getFormatList(String format) {
    if(format == null || format.isEmpty()){
      return ImmutableList.of();
    }
    return Lists.transform(ImmutableList.copyOf(format.split("\\|\\|")), new Function<String, String>() {
      @Nullable
      @Override
      public String apply(@Nullable String input) {
        if (input != null && !input.isEmpty() && input.startsWith(ElasticsearchConstants.STRICT)) {
          return input.substring(ElasticsearchConstants.STRICT.length());
        }
        return input;
      }
    });
  }

  public static FormatterAndType[] getFormatterAndType(List<String> formats){
    if(formats == null || formats.isEmpty()){
      return DEFAULT_FORMATTERS;
    }
    return FluentIterable.from(formats).transform(new Function<String, FormatterAndType>(){
      @Override
      public FormatterAndType apply(String input) {
        return getFormatterAndType(input);
      }}).toArray(FormatterAndType.class);
  }

  public static FormatterAndType getFormatterAndType(final String format) {

    switch (format) {
      case "epoch_millis":
      case "epochMillis":
        return new FormatterAndType(null, null, ElasticMappingSet.Type.TIMESTAMP, true);

      case "epoch_second":
      case "epochSecond":
        return new FormatterAndType(null, null, ElasticMappingSet.Type.TIMESTAMP, false);

      case "basicTime":                           // HHmmss.SSSZ
      case "basic_time":
        return new FormatterAndType(ISODateTimeFormat.basicTime(), ElasticMappingSet.Type.TIME);

      case "basicTimeNoMillis":                   // HHmmssZ
      case "basic_time_no_millis":
        return new FormatterAndType(ISODateTimeFormat.basicTimeNoMillis(), ElasticMappingSet.Type.TIME);

      case "basicTTime":                          // 'T’HHmmss.SSSZ
      case "basic_t_Time":
      case "basic_t_time":
        return new FormatterAndType(ISODateTimeFormat.basicTTime(), ElasticMappingSet.Type.TIME);

      case "basicTTimeNoMillis":
      case "basic_t_time_no_millis":
        return new FormatterAndType(ISODateTimeFormat.basicTTimeNoMillis(), ElasticMappingSet.Type.TIME);

      case "time":
        return new FormatterAndType(ISODateTimeFormat.time(), ElasticMappingSet.Type.TIME);

      case "time_no_millis":
      case "timeNoMillis":
        return new FormatterAndType(ISODateTimeFormat.timeNoMillis(), ElasticMappingSet.Type.TIME);

      case "tTime":
      case "t_time":
        return new FormatterAndType(ISODateTimeFormat.tTime(), ElasticMappingSet.Type.TIME);

      case "tTimeNoMillis":
      case "t_time_no_millis":
        return new FormatterAndType(ISODateTimeFormat.tTimeNoMillis(), ElasticMappingSet.Type.TIME);

      case "basicDate":                           // yyyyMMdd
      case "basic_date":
        return new FormatterAndType(ISODateTimeFormat.basicDate(), ElasticMappingSet.Type.DATE);

      case "basicDateTime":                       // yyyyMMdd’T'HHmmss.SSSZ
      case "basic_date_time":
        return new FormatterAndType(ISODateTimeFormat.basicDateTime(), ElasticMappingSet.Type.TIMESTAMP);

      case "basicDateTimeNoMillis":               // yyyyMMdd’T'HHmmssZ
      case "basic_date_time_no_millis":
        return new FormatterAndType(ISODateTimeFormat.basicDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

      case "basicOrdinalDate":                    // yyyyDDD
      case "basic_ordinal_date":
        return new FormatterAndType(ISODateTimeFormat.basicOrdinalDate(), ElasticMappingSet.Type.DATE);

      case "basicOrdinalDateTime":                // yyyyDDD’T'HHmmss.SSSZ
      case "basic_ordinal_date_time":
        return new FormatterAndType(ISODateTimeFormat.basicOrdinalDateTime(), ElasticMappingSet.Type.TIMESTAMP);

      case "basicOrdinalDateTimeNoMillis":        // yyyyDDD’T'HHmmssZ
      case "basic_ordinal_date_time_no_millis":
        return new FormatterAndType(ISODateTimeFormat.basicOrdinalDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

      case "basicWeekDate":                       // xxxx’W'wwe
      case "basic_week_date":
        return new FormatterAndType(ISODateTimeFormat.basicWeekDate(), ElasticMappingSet.Type.DATE);

      case "basicWeekDateTime":                   // xxxx’W'wwe’T'HHmmss.SSSZ
      case "basic_week_date_time":
        return new FormatterAndType(ISODateTimeFormat.basicWeekDateTime(), ElasticMappingSet.Type.TIMESTAMP);

      case "basicWeekDateTimeNoMillis":           // xxxx’W'wwe’T'HHmmssZ
      case "basic_week_date_time_no_millis":
        return new FormatterAndType(ISODateTimeFormat.basicWeekDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

      case "date":                                // yyyy-MM-dd
        return new FormatterAndType(ISODateTimeFormat.date(), ElasticMappingSet.Type.DATE);

      case "dateHour":
      case "date_hour":
        return new FormatterAndType(ISODateTimeFormat.dateHour(), ElasticMappingSet.Type.TIMESTAMP);

      case "dateHourMinute":                      // yyyy-MM-dd'T'HH:mm
      case "date_hour_minute":
        return new FormatterAndType(ISODateTimeFormat.dateHourMinute(), ElasticMappingSet.Type.TIMESTAMP);

      case "dateHourMinuteSecond":                // yyyy-MM-dd'T'HH:mm:ss
      case "date_hour_minute_second":
        return new FormatterAndType(ISODateTimeFormat.dateHourMinuteSecond(), ElasticMappingSet.Type.TIMESTAMP);

      case "dateHourMinuteSecondFraction":        // yyyy-MM-dd'T'HH:mm:ss.SSS
      case "date_hour_minute_second_fraction":
        return new FormatterAndType(ISODateTimeFormat.dateHourMinuteSecondFraction(), ElasticMappingSet.Type.TIMESTAMP);

      case "dateHourMinuteSecondMillis":          // yyyy-MM-dd'T'HH:mm:ss.SSS
      case "date_hour_minute_second_millis":
        return new FormatterAndType(ISODateTimeFormat.dateHourMinuteSecondMillis(), ElasticMappingSet.Type.TIMESTAMP);

      case "dateOptionalTime":
      case "date_optional_time":
        return new FormatterAndType(ISODateTimeFormat.dateOptionalTimeParser(), ISODateTimeFormat.dateTime(), ElasticMappingSet.Type.TIMESTAMP, false);

      case "dateTime":
      case "date_time":
        return new FormatterAndType(ISODateTimeFormat.dateTime(), ElasticMappingSet.Type.TIMESTAMP);

      case "dateTimeNoMillis":
      case "date_time_no_millis":
        return new FormatterAndType(ISODateTimeFormat.dateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

      case "hour":
        return new FormatterAndType(ISODateTimeFormat.hour(), ElasticMappingSet.Type.TIME);

      case "hourMinute":
      case "hour_minute":
        return new FormatterAndType(ISODateTimeFormat.hourMinute(), ElasticMappingSet.Type.TIME);

      case "hourMinuteSecond":
      case "hour_minute_second":
        return new FormatterAndType(ISODateTimeFormat.hourMinuteSecond(), ElasticMappingSet.Type.TIME);

      case "hourMinuteSecondFraction":
      case "hour_minute_second_fraction":
        return new FormatterAndType(ISODateTimeFormat.hourMinuteSecondFraction(), ElasticMappingSet.Type.TIME);

      case "hourMinuteSecondMillis":
      case "hour_minute_second_millis":
        return new FormatterAndType(ISODateTimeFormat.hourMinuteSecondMillis(), ElasticMappingSet.Type.TIME);

      case "ordinalDate":
      case "ordinal_date":
        return new FormatterAndType(ISODateTimeFormat.ordinalDate(), ElasticMappingSet.Type.DATE);

      case "ordinalDateTime":
      case "ordinal_date_time":
        return new FormatterAndType(ISODateTimeFormat.ordinalDateTime(), ElasticMappingSet.Type.TIMESTAMP);

      case "ordinalDateTimeNoMillis":
      case "ordinal_date_time_no_millis":
        return new FormatterAndType(ISODateTimeFormat.ordinalDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

      case "weekDate":
      case "week_date":
        return new FormatterAndType(ISODateTimeFormat.weekDate(), ElasticMappingSet.Type.DATE);

      case "weekDateTime":
      case "week_date_time":
        return new FormatterAndType(ISODateTimeFormat.weekDateTime(), ElasticMappingSet.Type.TIMESTAMP);

      case "week_date_time_no_millis":
      case "weekDateTimeNoMillis":
        return new FormatterAndType(ISODateTimeFormat.weekDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

      case "weekyear":
      case "week_year":
        return new FormatterAndType(ISODateTimeFormat.weekyear(), ElasticMappingSet.Type.DATE);

      case "weekyear_week":
      case "weekyearWeek":
        return new FormatterAndType(ISODateTimeFormat.weekyearWeek(), ElasticMappingSet.Type.DATE);

      case "weekyear_week_day":
      case "weekyearWeekDay":
        return new FormatterAndType(ISODateTimeFormat.weekyearWeekDay(), ElasticMappingSet.Type.DATE);

      case "year":
        return new FormatterAndType(ISODateTimeFormat.year(), ElasticMappingSet.Type.DATE);

      case "yearMonth":
      case "year_month":
        return new FormatterAndType(ISODateTimeFormat.yearMonth(), ElasticMappingSet.Type.DATE);

      case "yearMonthDay":
      case "year_month_day":
        return new FormatterAndType(ISODateTimeFormat.yearMonthDay(), ElasticMappingSet.Type.DATE);

      default:
        try {
          return new FormatterAndType(DateTimeFormat.forPattern(format), ElasticMappingSet.Type.TIMESTAMP);
        } catch (IllegalArgumentException e) {
          throw UserException.unsupportedError().message("Found invalid custom date format, " + format).build(logger);
        }
    }
  }
}
