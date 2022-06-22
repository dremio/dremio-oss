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
package com.dremio.plugins.elastic;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
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

  public static class FormatterAndType extends AbstractFormatterAndType{

    private final DateTimeFormatter formatter;
    private final DateTimeFormatter printer;
    private final ElasticMappingSet.Type type;
    private final boolean isEpochMillis;
    public static final FormatterAndType[] DEFAULT_FORMATTERS = {getFormatterAndType("dateOptionalTime"), getFormatterAndType(EPOCH_MILLIS)};

    public FormatterAndType(final DateTimeFormatter formatter, final ElasticMappingSet.Type type) {
      this(formatter, formatter, type, false);
    }

    public FormatterAndType(final DateTimeFormatter formatter, final DateTimeFormatter printer, final ElasticMappingSet.Type type, final boolean isEpochMillis) {
      super(formatter, printer, type, isEpochMillis);
      Preconditions.checkArgument((formatter == null && printer == null) || (formatter.isParser() && printer.isPrinter()));
      this.formatter = formatter == null ? null : formatter.withZoneUTC();
      this.printer = printer == null ? null : printer.withZoneUTC();
      this.type = type;
      this.isEpochMillis = isEpochMillis;
    }

    public long parseToLong(String value){
      //TODO-Following logic will be refactored using seperate subclasses for each flag. DX-33250:Polymorphic behaviour required for seperate behaviors
      if (formatter == null) {
        if (isEpochMillis) {
          return Long.parseLong(value);
        } else {
          // isEpochSeconds
          return Long.parseLong(value) * ElasticsearchConstants.MILLIS_PER_SECOND;
        }
      }
      return LocalDateTimes.getLocalMillis(formatter.parseLocalDateTime(value));
    }

    public static long getMillisGenericFormatter(String value) {
      return DEFAULT_FORMATTERS[0].parseToLong(value);
    }

    public LocalDateTime parse(String value) {
      //TODO-Following logic will be refactored using seperate subclasses for each flag. DX-33250:Polymorphic behaviour required for seperate behaviors
      if (formatter == null) {
        if (isEpochMillis) {
          return new LocalDateTime(Long.parseLong(value), ISOChronology.getInstanceUTC());
        } else {
          // isEpochSeconds
          return new LocalDateTime(Long.parseLong(value) * ElasticsearchConstants.MILLIS_PER_SECOND, ISOChronology.getInstanceUTC());
        }
      }
      return formatter.parseLocalDateTime(value);
    }

    public String dateFormatString(LocalDateTime value) {
      //TODO-Following logic will be refactored using seperate subclasses for each flag. DX-33250:Polymorphic behaviour required for seperate behaviors.
      if (printer == null) {
        if (isEpochMillis) {
          return Long.toString(com.dremio.common.util.DateTimes.toMillis(value));
        } else {
          return Long.toString(com.dremio.common.util.DateTimes.toMillis(value) / ElasticsearchConstants.MILLIS_PER_SECOND);
        }
      }
      return printer.print(com.dremio.common.util.DateTimes.toDateTime(value));
    }

    public ElasticMappingSet.Type type() {
      return type;
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

  public static class FormatterAndTypeJavaTime extends AbstractFormatterAndType{

    private final java.time.format.DateTimeFormatter formatter;
    private final java.time.format.DateTimeFormatter printer;
    private final ElasticMappingSet.Type type;
    private final boolean isEpochMillis;
    public static final FormatterAndTypeJavaTime[] DEFAULT_FORMATTERS_JAVA_TIME = {getFormatterAndType("dateOptionalTime"), getFormatterAndType(EPOCH_MILLIS)};

    public FormatterAndTypeJavaTime(final java.time.format.DateTimeFormatter formatter, final ElasticMappingSet.Type type) {
      this(formatter, formatter, type, false);
    }

    public FormatterAndTypeJavaTime(final java.time.format.DateTimeFormatter formatter, final java.time.format.DateTimeFormatter printer, final ElasticMappingSet.Type type, final boolean isEpochMillis) {
      super(formatter, printer, type, isEpochMillis);
      this.formatter = formatter;
      this.printer = printer;
      this.type = type;
      this.isEpochMillis = isEpochMillis;
    }

    public java.time.LocalDateTime parse(String value, java.time.format.DateTimeFormatter formatter) {
      return parseLocalDateTime(value, this.formatter);
    }

    /*
    Parse date string value to corresponding long value
     */
    public long parseToLong(String value) {
      //TODO-Following logic will be refactored using seperate subclasses for each flag. DX-33250:Polymorphic behaviour required for seperate behaviors
      if (formatter == null) {
        if (isEpochMillis) {
          return Long.parseLong(value);
        } else {
          // isEpochSeconds
          return Long.parseLong(value) * ElasticsearchConstants.MILLIS_PER_SECOND;
        }
      }
      try {
        return parseLongMillis(value, formatter);
      }
      catch (DateTimeParseException e) {
        throw new IllegalArgumentException();
      }
    }

    public static long getMillisGenericFormatter(String value) {
      return AbstractFormatterAndType.getMillisGenericFormatter(value);
    }

    public ElasticMappingSet.Type type() {
      return type;
    }

    public static FormatterAndTypeJavaTime[] getFormatterAndType(List<String> formats){
      if(formats == null || formats.isEmpty()){
        return DEFAULT_FORMATTERS_JAVA_TIME;
      }
      return FluentIterable.from(formats).transform(new Function<String, FormatterAndTypeJavaTime>(){
        @Override
        public FormatterAndTypeJavaTime apply(String input) {
          return getFormatterAndType(input);
        }}).toArray(FormatterAndTypeJavaTime.class);
    }

    public static FormatterAndTypeJavaTime getFormatterAndType(final String format) {
      String pattern;
      switch (format) {
        case "epoch_millis":
        case "epochMillis":
          return new FormatterAndTypeJavaTime(null, null, ElasticMappingSet.Type.TIMESTAMP, true);

        case "epoch_second":
        case "epochSecond":
          return new FormatterAndTypeJavaTime(null, null, ElasticMappingSet.Type.TIMESTAMP, false);

        case "basicTime":                           // HHmmss.SSSZ
        case "basic_time":
          pattern = "HHmmss.SSSz";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "basicTimeNoMillis":                   // HHmmssZ
        case "basic_time_no_millis":
          pattern = "HHmmssz";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "basicTTime":                          // 'T’HHmmss.SSSZ
        case "basic_t_Time":
        case "basic_t_time":
          pattern = "'T'HHmmss.SSSz";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "basicTTimeNoMillis":
        case "basic_t_time_no_millis":
          pattern = "'T'HHmmssz";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);


        case "time":
          pattern = "HH:mm:ss.SSSzz";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "time_no_millis":
        case "timeNoMillis":
          pattern = "HH:mm:sszz";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "tTime":
        case "t_time":
          pattern = "'T'HH:mm:ss.SSSzz";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "tTimeNoMillis":
        case "t_time_no_millis":
          pattern = "'T'HH:mm:sszz";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "basicDate":                           // yyyyMMdd
        case "basic_date":
          pattern = "yyyyMMdd";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.DATE);


        case "basicDateTime":                       // yyyyMMdd’T'HHmmss.SSSZ
        case "basic_date_time":
          pattern = "yyyyMMdd'T'HHmmss.SSSZ";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);

        case "basicDateTimeNoMillis":               // yyyyMMdd’T'HHmmssZ
        case "basic_date_time_no_millis":
          pattern = "yyyyMMdd'T'HHmmssZ";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);

        case "basicOrdinalDate":                    // yyyyDDD
        case "basic_ordinal_date":
          pattern = "yyyyDDD";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);

        case "basicOrdinalDateTime":                // yyyyDDD’T'HHmmss.SSSZ
        case "basic_ordinal_date_time":
          final java.time.format.DateTimeFormatter basicordinalDatetimeFormatter = new java.time.format.DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("[yyyyDDD]")
            .appendPattern(ElasticsearchConstants.ES_TIME_FORMAT)
            .toFormatter();
          return new FormatterAndTypeJavaTime(basicordinalDatetimeFormatter, ElasticMappingSet.Type.TIMESTAMP);

        case "basicOrdinalDateTimeNoMillis":        // yyyyDDD’T'HHmmssZ
        case "basic_ordinal_date_time_no_millis":
          final java.time.format.DateTimeFormatter basicordinalDatetimeFormatterNoMillis = new java.time.format.DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("[yyyyDDD]")
            .appendPattern(ElasticsearchConstants.ES_TIME_FORMAT)
            .toFormatter();
          return new FormatterAndTypeJavaTime(basicordinalDatetimeFormatterNoMillis, ElasticMappingSet.Type.TIMESTAMP);

        case "basicWeekDate":                       // xxxx’W'wwe
        case "basic_week_date":
          pattern = "[[YYYY]'W'wwe]";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.DATE);

        case "basicWeekDateTime":                   // xxxx’W'wwe’T'HHmmss.SSSZ
        case "basic_week_date_time":
          final java.time.format.DateTimeFormatter basicweekDateTimeFormatter = new java.time.format.DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("[[YYYY]'W'wwe]")
            .appendPattern(ElasticsearchConstants.ES_TIME_FORMAT)
            .toFormatter();
          return new FormatterAndTypeJavaTime(basicweekDateTimeFormatter, ElasticMappingSet.Type.TIMESTAMP);

        case "basicWeekDateTimeNoMillis":           // xxxx’W'wwe’T'HHmmssZ
        case "basic_week_date_time_no_millis":
          final java.time.format.DateTimeFormatter basicweekDateTimeFormatterNoMillis = new java.time.format.DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("[[YYYY]'W'wwe]")
            .appendPattern(ElasticsearchConstants.ES_TIME_FORMAT)
            .toFormatter();
          return new FormatterAndTypeJavaTime(basicweekDateTimeFormatterNoMillis, ElasticMappingSet.Type.TIMESTAMP);

        case "date":                                // yyyy-MM-dd
          pattern = "yyyy-MM-dd";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.DATE);

        case "dateHour":
        case "date_hour":
          pattern = "yyyy-MM-dd'T'HH";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinute":                      // yyyy-MM-dd'T'HH:mm
        case "date_hour_minute":
          pattern = "yyyy-MM-dd'T'HH:mm";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinuteSecond":                // yyyy-MM-dd'T'HH:mm:ss
        case "date_hour_minute_second":
          pattern = "yyyy-MM-dd'T'HH:mm:ss";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinuteSecondFraction":        // yyyy-MM-dd'T'HH:mm:ss.SSS
        case "date_hour_minute_second_fraction":
          pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinuteSecondMillis":          // yyyy-MM-dd'T'HH:mm:ss.SSS
        case "date_hour_minute_second_millis":
          pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);

        case "dateOptionalTime":
        case "date_optional_time":
          pattern = "yyyy-MM-dd";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.DATE);

        case "dateTime":
        case "date_time":
          pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);

        case "dateTimeNoMillis":
        case "date_time_no_millis":
          pattern = "yyyy-MM-dd'T'HH:mm:ssZZ";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);

        case "hour":
          pattern = "HH";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "hourMinute":
        case "hour_minute":
          pattern = "HH:mm";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "hourMinuteSecond":
        case "hour_minute_second":
          pattern = "HH:mm:ss";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "hourMinuteSecondFraction":
        case "hour_minute_second_fraction":
          pattern = "HH:mm:ss.SSS";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "hourMinuteSecondMillis":
        case "hour_minute_second_millis":
          pattern = "HH:mm:ss.SSS";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIME);

        case "ordinalDate":
        case "ordinal_date":
          pattern = "yyyy-DDD";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.DATE);

        case "ordinalDateTime":
        case "ordinal_date_time":
          final java.time.format.DateTimeFormatter ordinalDatetimeFormatter = new java.time.format.DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(java.time.format.DateTimeFormatter.ISO_ORDINAL_DATE)
            .appendPattern(ElasticsearchConstants.ES_TIME_FORMAT)
            .toFormatter();
          return new FormatterAndTypeJavaTime(ordinalDatetimeFormatter, ElasticMappingSet.Type.TIMESTAMP);
        case "ordinalDateTimeNoMillis":
        case "ordinal_date_time_no_millis":
          final java.time.format.DateTimeFormatter ordinalDatetimeFormatterNomillis = new java.time.format.DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(java.time.format.DateTimeFormatter.ISO_ORDINAL_DATE)
            .appendPattern(ElasticsearchConstants.ES_TIME_FORMAT)
            .toFormatter();
          return new FormatterAndTypeJavaTime(ordinalDatetimeFormatterNomillis, ElasticMappingSet.Type.TIMESTAMP);
        case "weekDate":
        case "week_date":
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ISO_WEEK_DATE, ElasticMappingSet.Type.DATE);

        case "weekDateTime":
        case "week_date_time":
          final java.time.format.DateTimeFormatter weekFormatter = new java.time.format.DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(java.time.format.DateTimeFormatter.ISO_WEEK_DATE)
            .appendPattern(ElasticsearchConstants.ES_TIME_FORMAT)
            .toFormatter();
          return new FormatterAndTypeJavaTime(weekFormatter, ElasticMappingSet.Type.TIMESTAMP);

        case "week_date_time_no_millis":
        case "weekDateTimeNoMillis":
          final java.time.format.DateTimeFormatter weekFormatterNomillis = new java.time.format.DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(java.time.format.DateTimeFormatter.ISO_WEEK_DATE)
            .appendPattern(ElasticsearchConstants.ES_TIME_FORMAT)
            .toFormatter();
          return new FormatterAndTypeJavaTime(weekFormatterNomillis, ElasticMappingSet.Type.TIMESTAMP);

        case "weekyear":
        case "week_year":
          pattern = "YYYY";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.DATE);

        case "weekyear_week":
        case "weekyearWeek":
          pattern = "YYYY-'W'ww";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.DATE);

        case "weekyear_week_day":
        case "weekyearWeekDay":
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ISO_WEEK_DATE, ElasticMappingSet.Type.DATE);

        case "year":
          pattern = "yyyy";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.DATE);

        case "yearMonth":
        case "year_month":
          pattern = "yyyy-mm";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.DATE);

        case "yearMonthDay":
        case "year_month_day":
          pattern = "yyyy-MM-dd";
          return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.DATE);

        default:
          try {
            // To handle formats prefixed with 8. For ES 7 format should be considered as Javatime regardless whether prefixed with 8 or not.
            if (format.startsWith("8")) {
              final String modifiedPattern = format.substring(1);
              return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(modifiedPattern), ElasticMappingSet.Type.TIMESTAMP);
            }
            return new FormatterAndTypeJavaTime(java.time.format.DateTimeFormatter.ofPattern(format), ElasticMappingSet.Type.TIMESTAMP);
          } catch (IllegalArgumentException e) {
            throw UserException.unsupportedError().message("Found invalid custom date format, " + format).build(logger);
          }
      }
    }
  }

  public static class FormatterAndTypeMix extends AbstractFormatterAndType{

    private java.time.format.DateTimeFormatter formatterJT;
    private java.time.format.DateTimeFormatter printerJT;
    private DateTimeFormatter formatterJD;
    private DateTimeFormatter printerJD;
    private final ElasticMappingSet.Type type;
    private final boolean isEpochMillis;
    public static final FormatterAndTypeMix[] DEFAULT_FORMATTERS_MIX = {getFormatterAndType("dateOptionalTime"), getFormatterAndType(EPOCH_MILLIS)};

    public FormatterAndTypeMix(final DateTimeFormatter formatterJD, final ElasticMappingSet.Type type) {
      this(formatterJD, formatterJD, type, false);
    }

    public FormatterAndTypeMix(final DateTimeFormatter formatterJD, final DateTimeFormatter printerJD, final ElasticMappingSet.Type type, final boolean isEpochMillis) {
      super(formatterJD, printerJD, type, isEpochMillis);
      this.formatterJD = formatterJD;
      this.printerJD = printerJD;
      this.type = type;
      this.isEpochMillis = isEpochMillis;
    }

    public FormatterAndTypeMix(final java.time.format.DateTimeFormatter formatterJT, final ElasticMappingSet.Type type) {
      this(formatterJT, formatterJT, type, false);
    }

    public FormatterAndTypeMix(final java.time.format.DateTimeFormatter formatterJT, final java.time.format.DateTimeFormatter printerJT, final ElasticMappingSet.Type type, final boolean isEpochMillis) {
      super(formatterJT, printerJT, type, isEpochMillis);
      this.formatterJT = formatterJT;
      this.printerJT = printerJT;
      this.type = type;
      this.isEpochMillis = isEpochMillis;
    }
    /*
     Parse date string value to corresponding long value
    */
    public long parseToLong(String value)  {
      //TODO-Following logic will be refactored using seperate subclasses for each flag. DX-33250:Polymorphic behaviour required for seperate behaviors
      if (formatterJT == null && formatterJD == null) {
        if (isEpochMillis) {
          return Long.parseLong(value);
        } else {
          // isEpochSeconds
          return Long.parseLong(value) * ElasticsearchConstants.MILLIS_PER_SECOND;
        }
      }
      if (formatterJT != null && formatterJD == null) {
        try {
          return parseLongMillis(value, formatterJT);
        }
        catch (DateTimeParseException e) {
          throw new IllegalArgumentException();
        }
      } else {
        return LocalDateTimes.getLocalMillis(formatterJD.parseLocalDateTime(value));
      }
    }

    public static long getMillisGenericFormatter(String value) {
      return DEFAULT_FORMATTERS_MIX[0].parseToLong(value);
    }

    public java.time.LocalDateTime parse(String value, java.time.format.DateTimeFormatter formatterJT) {
      return parseLocalDateTime(value, this.formatterJT);
    }

    public LocalDateTime parse(String value, DateTimeFormatter formatterJD) {
      return this.formatterJD.parseLocalDateTime(value);
    }

    public ElasticMappingSet.Type type() {
      return type;
    }

    public static FormatterAndTypeMix[] getFormatterAndType(List<String> formats){
      if(formats == null || formats.isEmpty()){
        return DEFAULT_FORMATTERS_MIX;
      }
      return FluentIterable.from(formats).transform(new Function<String, FormatterAndTypeMix>(){
        @Override
        public FormatterAndTypeMix apply(String input) {
          return getFormatterAndType(input);
        }}).toArray(FormatterAndTypeMix.class);
    }

    public static FormatterAndTypeMix getFormatterAndType(final String format) {
      switch (format) {
        case "epoch_millis":
        case "epochMillis":
          return new FormatterAndTypeMix (null, java.time.format.DateTimeFormatter.BASIC_ISO_DATE, ElasticMappingSet.Type.TIMESTAMP, true);

        case "epoch_second":
        case "epochSecond":
          return new FormatterAndTypeMix (null, java.time.format.DateTimeFormatter.BASIC_ISO_DATE, ElasticMappingSet.Type.TIMESTAMP, false);

        case "basicTime":                           // HHmmss.SSSZ
        case "basic_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicTime(), ElasticMappingSet.Type.TIME);

        case "basicTimeNoMillis":                   // HHmmssZ
        case "basic_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicTimeNoMillis(), ElasticMappingSet.Type.TIME);

        case "basicTTime":                          // 'T’HHmmss.SSSZ
        case "basic_t_Time":
        case "basic_t_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicTTime(), ElasticMappingSet.Type.TIME);

        case "basicTTimeNoMillis":
        case "basic_t_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicTTimeNoMillis(), ElasticMappingSet.Type.TIME);

        case "time":
          return new FormatterAndTypeMix(ISODateTimeFormat.time(), ElasticMappingSet.Type.TIME);

        case "time_no_millis":
        case "timeNoMillis":
          return new FormatterAndTypeMix(ISODateTimeFormat.timeNoMillis(), ElasticMappingSet.Type.TIME);

        case "tTime":
        case "t_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.tTime(), ElasticMappingSet.Type.TIME);

        case "tTimeNoMillis":
        case "t_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.tTimeNoMillis(), ElasticMappingSet.Type.TIME);

        case "basicDate":                           // yyyyMMdd
        case "basic_date":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicDate(), ElasticMappingSet.Type.DATE);

        case "basicDateTime":                       // yyyyMMdd’T'HHmmss.SSSZ
        case "basic_date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicDateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "basicDateTimeNoMillis":               // yyyyMMdd’T'HHmmssZ
        case "basic_date_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "basicOrdinalDate":                    // yyyyDDD
        case "basic_ordinal_date":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicOrdinalDate(), ElasticMappingSet.Type.DATE);

        case "basicOrdinalDateTime":                // yyyyDDD’T'HHmmss.SSSZ
        case "basic_ordinal_date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicOrdinalDateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "basicOrdinalDateTimeNoMillis":        // yyyyDDD’T'HHmmssZ
        case "basic_ordinal_date_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicOrdinalDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "basicWeekDate":                       // xxxx’W'wwe
        case "basic_week_date":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicWeekDate(), ElasticMappingSet.Type.DATE);

        case "basicWeekDateTime":                   // xxxx’W'wwe’T'HHmmss.SSSZ
        case "basic_week_date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicWeekDateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "basicWeekDateTimeNoMillis":           // xxxx’W'wwe’T'HHmmssZ
        case "basic_week_date_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicWeekDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "date":                                // yyyy-MM-dd
          return new FormatterAndTypeMix(ISODateTimeFormat.date(), ElasticMappingSet.Type.DATE);

        case "dateHour":
        case "date_hour":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateHour(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinute":                      // yyyy-MM-dd'T'HH:mm
        case "date_hour_minute":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateHourMinute(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinuteSecond":                // yyyy-MM-dd'T'HH:mm:ss
        case "date_hour_minute_second":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateHourMinuteSecond(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinuteSecondFraction":        // yyyy-MM-dd'T'HH:mm:ss.SSS
        case "date_hour_minute_second_fraction":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateHourMinuteSecondFraction(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinuteSecondMillis":          // yyyy-MM-dd'T'HH:mm:ss.SSS
        case "date_hour_minute_second_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateHourMinuteSecondMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateOptionalTime":
        case "date_optional_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateOptionalTimeParser(), ISODateTimeFormat.dateTime(), ElasticMappingSet.Type.TIMESTAMP, false);

        case "dateTime":
        case "date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateTimeNoMillis":
        case "date_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "hour":
          return new FormatterAndTypeMix(ISODateTimeFormat.hour(), ElasticMappingSet.Type.TIME);

        case "hourMinute":
        case "hour_minute":
          return new FormatterAndTypeMix(ISODateTimeFormat.hourMinute(), ElasticMappingSet.Type.TIME);

        case "hourMinuteSecond":
        case "hour_minute_second":
          return new FormatterAndTypeMix(ISODateTimeFormat.hourMinuteSecond(), ElasticMappingSet.Type.TIME);

        case "hourMinuteSecondFraction":
        case "hour_minute_second_fraction":
          return new FormatterAndTypeMix(ISODateTimeFormat.hourMinuteSecondFraction(), ElasticMappingSet.Type.TIME);

        case "hourMinuteSecondMillis":
        case "hour_minute_second_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.hourMinuteSecondMillis(), ElasticMappingSet.Type.TIME);

        case "ordinalDate":
        case "ordinal_date":
          return new FormatterAndTypeMix(ISODateTimeFormat.ordinalDate(), ElasticMappingSet.Type.DATE);

        case "ordinalDateTime":
        case "ordinal_date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.ordinalDateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "ordinalDateTimeNoMillis":
        case "ordinal_date_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.ordinalDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "weekDate":
        case "week_date":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekDate(), ElasticMappingSet.Type.DATE);

        case "weekDateTime":
        case "week_date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekDateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "week_date_time_no_millis":
        case "weekDateTimeNoMillis":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "weekyear":
        case "week_year":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekyear(), ElasticMappingSet.Type.DATE);

        case "weekyear_week":
        case "weekyearWeek":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekyearWeek(), ElasticMappingSet.Type.DATE);

        case "weekyear_week_day":
        case "weekyearWeekDay":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekyearWeekDay(), ElasticMappingSet.Type.DATE);

        case "year":
          return new FormatterAndTypeMix(ISODateTimeFormat.year(), ElasticMappingSet.Type.DATE);

        case "yearMonth":
        case "year_month":
          return new FormatterAndTypeMix(ISODateTimeFormat.yearMonth(), ElasticMappingSet.Type.DATE);

        case "yearMonthDay":
        case "year_month_day":
          return new FormatterAndTypeMix(ISODateTimeFormat.yearMonthDay(), ElasticMappingSet.Type.DATE);

        default:
          try {
            // To handle formats prefixed with 8.
            // For ES 6.8 if format prefixed with 8 then it should be considered as JavaTime otherwise should be considered as JodaTime.
            // For ES 7 format should be considered as Javatime regardless whether prefixed with 8 or not.
            if (format.startsWith("8")) {
              final String pattern = format.substring(1);
              return new FormatterAndTypeMix(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);
            }
            return new FormatterAndTypeMix(DateTimeFormat.forPattern(format), ElasticMappingSet.Type.TIMESTAMP);
          } catch (IllegalArgumentException e) {
            throw UserException.unsupportedError().message("Found invalid custom date format, " + format).build(logger);
          }
      }
    }
  }

  public abstract static class AbstractFormatterAndType {

    private java.time.format.DateTimeFormatter formatterJT;
    private java.time.format.DateTimeFormatter printerJT;
    private DateTimeFormatter formatterJD;
    private DateTimeFormatter printerJD;
    private final ElasticMappingSet.Type type;
    private final boolean isEpochMillis;
    public static final AbstractFormatterAndType[] DEFAULT_FORMATTERS = {getFormatterAndType("dateOptionalTime"), getFormatterAndType(EPOCH_MILLIS)};

    public AbstractFormatterAndType(final java.time.format.DateTimeFormatter formatterJT, final ElasticMappingSet.Type type) {
      this(formatterJT, formatterJT, type, false);
    }

    public AbstractFormatterAndType(final java.time.format.DateTimeFormatter formatter, final java.time.format.DateTimeFormatter printer, final ElasticMappingSet.Type type, final boolean isEpochMillis) {
      this.formatterJT = formatter;
      this.printerJT = printer;
      this.type = type;
      this.isEpochMillis = isEpochMillis;
    }

    public AbstractFormatterAndType(final DateTimeFormatter formatterJD, final ElasticMappingSet.Type type) {
      this(formatterJD, formatterJD, type, false);
    }

    public AbstractFormatterAndType(final DateTimeFormatter formatter, final DateTimeFormatter printer, final ElasticMappingSet.Type type, final boolean isEpochMillis) {
      this.formatterJD = formatter;
      this.printerJD = printer;
      this.type = type;
      this.isEpochMillis = isEpochMillis;
    }

    public abstract long parseToLong(String value);

    public java.time.LocalDateTime parse(String value, java.time.format.DateTimeFormatter formatterJT) {
      return java.time.LocalDateTime.parse(value, formatterJT);
    }

    public static long getMillisGenericFormatter(String value) {
    /*
    Create generic formatter to support more specific formats.
     */
      final java.time.format.DateTimeFormatter dateTimeFormatter = new java.time.format.DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendPattern(ElasticsearchConstants.ES_GENERIC_FORMAT1)
        .optionalStart()
        .appendFraction( ChronoField.MICRO_OF_SECOND , 1 , 6 , true )
        .optionalEnd()
        .appendPattern( ElasticsearchConstants.ES_GENERIC_FORMAT2 )
        .appendPattern( ElasticsearchConstants.ES_GENERIC_FORMAT3 )
        .optionalStart()
        .appendFraction( ChronoField.MICRO_OF_SECOND , 1 , 6 , true )
        .optionalEnd()
        .appendPattern( ElasticsearchConstants.ES_GENERIC_FORMAT4 )
        .appendPattern( ElasticsearchConstants.ES_GENERIC_FORMAT5 )
        .appendPattern(ElasticsearchConstants.ES_GENERIC_FORMAT6)
        .appendPattern(ElasticsearchConstants.ES_GENERIC_FORMAT7)
        .appendPattern(ElasticsearchConstants.ES_GENERIC_FORMAT8)
        .appendPattern(ElasticsearchConstants.ES_GENERIC_FORMAT9)
        .toFormatter();

      return DateFormats.parseLongMillis(value, dateTimeFormatter);
    }

    public LocalDateTime parse(String value) {
      return formatterJD.parseLocalDateTime(value);
    }

    public ElasticMappingSet.Type type() {
      return type;
    }

    public static AbstractFormatterAndType getFormatterAndType(final String format) {
      switch (format) {
        case "epoch_millis":
        case "epochMillis":
          return new FormatterAndTypeMix (null, java.time.format.DateTimeFormatter.BASIC_ISO_DATE, ElasticMappingSet.Type.TIMESTAMP, true);

        case "epoch_second":
        case "epochSecond":
          return new FormatterAndTypeMix (null, java.time.format.DateTimeFormatter.BASIC_ISO_DATE, ElasticMappingSet.Type.TIMESTAMP, false);

        case "basicTime":                           // HHmmss.SSSZ
        case "basic_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicTime(), ElasticMappingSet.Type.TIME);

        case "basicTimeNoMillis":                   // HHmmssZ
        case "basic_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicTimeNoMillis(), ElasticMappingSet.Type.TIME);

        case "basicTTime":                          // 'T’HHmmss.SSSZ
        case "basic_t_Time":
        case "basic_t_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicTTime(), ElasticMappingSet.Type.TIME);

        case "basicTTimeNoMillis":
        case "basic_t_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicTTimeNoMillis(), ElasticMappingSet.Type.TIME);

        case "time":
          return new FormatterAndTypeMix(ISODateTimeFormat.time(), ElasticMappingSet.Type.TIME);

        case "time_no_millis":
        case "timeNoMillis":
          return new FormatterAndTypeMix(ISODateTimeFormat.timeNoMillis(), ElasticMappingSet.Type.TIME);

        case "tTime":
        case "t_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.tTime(), ElasticMappingSet.Type.TIME);

        case "tTimeNoMillis":
        case "t_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.tTimeNoMillis(), ElasticMappingSet.Type.TIME);

        case "basicDate":                           // yyyyMMdd
        case "basic_date":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicDate(), ElasticMappingSet.Type.DATE);

        case "basicDateTime":                       // yyyyMMdd’T'HHmmss.SSSZ
        case "basic_date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicDateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "basicDateTimeNoMillis":               // yyyyMMdd’T'HHmmssZ
        case "basic_date_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "basicOrdinalDate":                    // yyyyDDD
        case "basic_ordinal_date":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicOrdinalDate(), ElasticMappingSet.Type.DATE);

        case "basicOrdinalDateTime":                // yyyyDDD’T'HHmmss.SSSZ
        case "basic_ordinal_date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicOrdinalDateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "basicOrdinalDateTimeNoMillis":        // yyyyDDD’T'HHmmssZ
        case "basic_ordinal_date_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicOrdinalDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "basicWeekDate":                       // xxxx’W'wwe
        case "basic_week_date":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicWeekDate(), ElasticMappingSet.Type.DATE);

        case "basicWeekDateTime":                   // xxxx’W'wwe’T'HHmmss.SSSZ
        case "basic_week_date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicWeekDateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "basicWeekDateTimeNoMillis":           // xxxx’W'wwe’T'HHmmssZ
        case "basic_week_date_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.basicWeekDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "date":                                // yyyy-MM-dd
          return new FormatterAndTypeMix(ISODateTimeFormat.date(), ElasticMappingSet.Type.DATE);

        case "dateHour":
        case "date_hour":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateHour(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinute":                      // yyyy-MM-dd'T'HH:mm
        case "date_hour_minute":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateHourMinute(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinuteSecond":                // yyyy-MM-dd'T'HH:mm:ss
        case "date_hour_minute_second":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateHourMinuteSecond(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinuteSecondFraction":        // yyyy-MM-dd'T'HH:mm:ss.SSS
        case "date_hour_minute_second_fraction":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateHourMinuteSecondFraction(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateHourMinuteSecondMillis":          // yyyy-MM-dd'T'HH:mm:ss.SSS
        case "date_hour_minute_second_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateHourMinuteSecondMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateOptionalTime":
        case "date_optional_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateOptionalTimeParser(), ISODateTimeFormat.dateTime(), ElasticMappingSet.Type.TIMESTAMP, false);

        case "dateTime":
        case "date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "dateTimeNoMillis":
        case "date_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.dateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "hour":
          return new FormatterAndTypeMix(ISODateTimeFormat.hour(), ElasticMappingSet.Type.TIME);

        case "hourMinute":
        case "hour_minute":
          return new FormatterAndTypeMix(ISODateTimeFormat.hourMinute(), ElasticMappingSet.Type.TIME);

        case "hourMinuteSecond":
        case "hour_minute_second":
          return new FormatterAndTypeMix(ISODateTimeFormat.hourMinuteSecond(), ElasticMappingSet.Type.TIME);

        case "hourMinuteSecondFraction":
        case "hour_minute_second_fraction":
          return new FormatterAndTypeMix(ISODateTimeFormat.hourMinuteSecondFraction(), ElasticMappingSet.Type.TIME);

        case "hourMinuteSecondMillis":
        case "hour_minute_second_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.hourMinuteSecondMillis(), ElasticMappingSet.Type.TIME);

        case "ordinalDate":
        case "ordinal_date":
          return new FormatterAndTypeMix(ISODateTimeFormat.ordinalDate(), ElasticMappingSet.Type.DATE);

        case "ordinalDateTime":
        case "ordinal_date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.ordinalDateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "ordinalDateTimeNoMillis":
        case "ordinal_date_time_no_millis":
          return new FormatterAndTypeMix(ISODateTimeFormat.ordinalDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "weekDate":
        case "week_date":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekDate(), ElasticMappingSet.Type.DATE);

        case "weekDateTime":
        case "week_date_time":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekDateTime(), ElasticMappingSet.Type.TIMESTAMP);

        case "week_date_time_no_millis":
        case "weekDateTimeNoMillis":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekDateTimeNoMillis(), ElasticMappingSet.Type.TIMESTAMP);

        case "weekyear":
        case "week_year":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekyear(), ElasticMappingSet.Type.DATE);

        case "weekyear_week":
        case "weekyearWeek":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekyearWeek(), ElasticMappingSet.Type.DATE);

        case "weekyear_week_day":
        case "weekyearWeekDay":
          return new FormatterAndTypeMix(ISODateTimeFormat.weekyearWeekDay(), ElasticMappingSet.Type.DATE);

        case "year":
          return new FormatterAndTypeMix(ISODateTimeFormat.year(), ElasticMappingSet.Type.DATE);

        case "yearMonth":
        case "year_month":
          return new FormatterAndTypeMix(ISODateTimeFormat.yearMonth(), ElasticMappingSet.Type.DATE);

        case "yearMonthDay":
        case "year_month_day":
          return new FormatterAndTypeMix(ISODateTimeFormat.yearMonthDay(), ElasticMappingSet.Type.DATE);

        default:
          try {
            // To handle formats prefixed with 8. For ES 7 format should be considered as Javatime regardless whether prefixed with 8 or not.
            if (format.startsWith("8")) {
              final String pattern = format.substring(1);
              return new FormatterAndTypeMix(java.time.format.DateTimeFormatter.ofPattern(pattern), ElasticMappingSet.Type.TIMESTAMP);
            }
            return new FormatterAndTypeMix(DateTimeFormat.forPattern(format), ElasticMappingSet.Type.TIMESTAMP);
          } catch (IllegalArgumentException e) {
            throw UserException.unsupportedError().message("Found invalid custom date format, " + format).build(logger);
          }
      }
    }
  }

  public static long parseLongMillis(String value, java.time.format.DateTimeFormatter dateTimeFormatter) {
    long parsedValue = 0L;
    // There are some temporal implementations but not handled in parseBest as such formats could not be used as date/datetime format in elasticsearch. e.g. Period, Era, Month, DayOfWeek.
    // Some implementations are not mentioned in parseBest as those could be handled through other implementations.Please refer comments for specific cases.
    final TemporalAccessor temporalAccessor = dateTimeFormatter.parseBest(value, ZonedDateTime::from, java.time.LocalDateTime::from, LocalDate::from, OffsetTime::from, LocalTime::from, YearMonth::from, Year::from, MonthDay::from);
    final String temporalAccessorClass = temporalAccessor.getClass().getSimpleName();
    switch (temporalAccessorClass) {
      // ZonedDateTime case will handle temporal datetime formats like OffsetDateTime, Instant, ChronoZonedDateTime. So no need to handle its seperately in parseBest.
      case "ZonedDateTime": {
        final ZonedDateTime zonedDateTime = ZonedDateTime.parse(value, dateTimeFormatter);
        final ZoneOffset zoneOffset = zonedDateTime.getOffset();
        parsedValue = zonedDateTime.toEpochSecond() * ElasticsearchConstants.MILLIS_PER_SECOND + zonedDateTime.getNano() / ElasticsearchConstants.NANOS_PER_MILLISECOND_LONG + zoneOffset.getTotalSeconds() * ElasticsearchConstants.MILLIS_PER_SECOND;
        break;
      }
      // LocalDateTime case will handle temporal datetime formats like ChronoLocalDateTime. So no need to handle its separately in parseBest.
      case "LocalDateTime": {
        final java.time.LocalDateTime localDateTime = java.time.LocalDateTime.parse(value, dateTimeFormatter);
        parsedValue = localDateTime.toEpochSecond(ZoneOffset.UTC) * ElasticsearchConstants.MILLIS_PER_SECOND + localDateTime.getNano() / ElasticsearchConstants.NANOS_PER_MILLISECOND_LONG;
        break;
      }
      // LocalDate case will handle temporal datetime formats like ChronoLocalDate. So no need to handle its separately in parseBest.
      // Other temporal formats like JapneseDate/MinguoDate will be handled through this case.
      case "LocalDate": {
        final LocalDate localDate = LocalDate.parse(value, dateTimeFormatter);
        parsedValue = localDate.toEpochDay() * ElasticsearchConstants.MILLIS_PER_DAY;
        break;
      }
      case "LocalTime": {
        final LocalTime localTime = LocalTime.parse(value, dateTimeFormatter);
        final java.time.LocalDateTime localDateTime = LocalDate.of(1970, 01, 01).atTime((localTime)).withNano(localTime.getNano());
        parsedValue = localDateTime.toEpochSecond(ZoneOffset.UTC) * ElasticsearchConstants.MILLIS_PER_SECOND + localDateTime.getNano() / ElasticsearchConstants.NANOS_PER_MILLISECOND_LONG;
        break;
      }
      case "YearMonth": {
        final YearMonth yearMonth = YearMonth.parse(value);
        final int year = yearMonth.getYear();
        final int month = yearMonth.getMonthValue();
        final LocalDate localDate = LocalDate.of(year, month, 01);
        parsedValue = localDate.toEpochDay() * ElasticsearchConstants.MILLIS_PER_DAY;
        break;
      }
      case "Year": {
        final int year = Year.parse(value).getValue();
        final LocalDate localDate = LocalDate.of(year, 01, 01);
        parsedValue = localDate.toEpochDay() * ElasticsearchConstants.MILLIS_PER_DAY;
        break;
      }
      case "OffsetTime": {
        final OffsetTime offsetTime = OffsetTime.parse(value, dateTimeFormatter);
        final ZoneOffset zoneOffset = offsetTime.getOffset();
        final OffsetDateTime offsetDateTime = LocalDate.of(1970, 01, 01).atTime(offsetTime).withNano(offsetTime.getNano());
        final long parsedvalueLocalTime = LocalDate.of(1970, 01, 01).atTime(offsetTime).toEpochSecond() * ElasticsearchConstants.MILLIS_PER_SECOND;
        final long parsedValueWithNano = offsetDateTime.getNano() /  ElasticsearchConstants.NANOS_PER_MILLISECOND_LONG ;
        final long parsedValueOffset = zoneOffset.getTotalSeconds() * ElasticsearchConstants.MILLIS_PER_SECOND;
        parsedValue = parsedvalueLocalTime + parsedValueWithNano + parsedValueOffset;
        break;
      }
      // MonthDay case handled to support format without year like "--MM-dd" which may represent birthday/Anniversary.
      // To align with behaviour of elasticsearch 6, using 2000 as year while selecting data.
      case "MonthDay": {
        final MonthDay monthDay = MonthDay.parse(value, dateTimeFormatter);
        final LocalDate localDate = LocalDate.of(2000, monthDay.getMonth(), monthDay.getDayOfMonth());
        parsedValue = localDate.toEpochDay() * ElasticsearchConstants.MILLIS_PER_DAY;
        break;
      }
    }
    return parsedValue;
  }

  public static java.time.LocalDateTime parseLocalDateTime(String value, java.time.format.DateTimeFormatter dateTimeFormatter) {
    final TemporalAccessor temporalAccessor = dateTimeFormatter.parseBest(value, ZonedDateTime::from, java.time.LocalDateTime::from, LocalDate::from,OffsetTime::from, LocalTime::from, MonthDay::from);
    final String temporalAccessorClass = temporalAccessor.getClass().getSimpleName();
    switch(temporalAccessorClass){
      case "ZonedDateTime":
        final ZonedDateTime zonedDateTime = ZonedDateTime.parse(value, dateTimeFormatter);
        return zonedDateTime.toLocalDateTime().withNano(zonedDateTime.getNano());
      case "LocalDateTime":
        return java.time.LocalDateTime.parse(value, dateTimeFormatter);
      case "LocalDate":
        final LocalDate localDate = LocalDate.parse(value, dateTimeFormatter);
        return localDate.atTime(0,0,0,1).minusNanos(1);
      case "LocalTime":
        final LocalTime localTime = LocalTime.parse(value, dateTimeFormatter);
        return LocalDate.of(1970, 01, 01).atTime((localTime));
      case "OffsetTime":
        final OffsetTime offsetTime = OffsetTime.parse(value, dateTimeFormatter);
        return LocalDate.of(1970, 01, 01).atTime(offsetTime).toLocalDateTime();
      case "MonthDay":
        final MonthDay monthDay = MonthDay.parse(value, dateTimeFormatter);
        return LocalDate.of(2000, monthDay.getMonth(), monthDay.getDayOfMonth()).atTime(0,0,0);
    }
    return java.time.LocalDateTime.parse(value, dateTimeFormatter);
  }

  public static ElasticMappingSet.Type getType(final List<String> formats) {
    Preconditions.checkArgument(formats != null && !formats.isEmpty());
    ElasticMappingSet.Type knownType = null;
    for (String format : formats) {
      final FormatterAndType formatterAndType = DateFormats.FormatterAndType.getFormatterAndType(format);
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
}
