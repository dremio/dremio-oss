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
import moment from 'moment';

const INVALID_DATE_MSG = 'Invalid date';

class TimeUtils {

  covertHoursFrom12To24(hours, part) {
    return part.toLowerCase() === 'pm'
      ? +hours + 12
      : hours;
  }
  zeroesPadding(value, length) {
    let sValue = String(value);
    if (sValue.length < length) {
      const zeroes = new Array(length - sValue.length + 1);
      sValue = zeroes.join('0') + sValue;
    }
    return sValue;
  }

  durationWithZero(duration, isNumberFormat = false) { // todo: loc
    const seconds = this.zeroesPadding(duration.seconds(), 2);
    const minutes = this.zeroesPadding(duration.minutes(), 2);
    const hours = this.zeroesPadding(Math.floor(duration.asHours()), 2);
    if (Math.floor(duration.asHours()) <= 0 && duration.minutes() <= 0 && duration.seconds() < 1) {
      return '<1s';
    } else if (isNumberFormat) {
      return `${hours}:${minutes}:${seconds}`;
    }
    // todo: loc
    return `${hours}h:${minutes}m:${seconds}s`.replace(/^(00[a-z]:)+/, '').replace(/^0+/, '');
  }

  getTimeRange(step, max, pad) {
    const arr = [];
    for (let start = 0; start <= max; start += step) {
      const label = start.toString();
      arr.push({label: label.length === 1 && pad ? '0' + label : label, option: start});
    }
    return arr;
  }

  getDayOfWeek() {
    const arr = [];
    for (let i = 1; i <= 7; i++) {
      const day = moment().isoWeekday(i).format('dddd');
      arr.push({label: day, option: day.toUpperCase()});
    }
    return arr;
  }

  fromNow(timestamp) {
    if (!timestamp) {
      return 'unknown';
    }
    return moment(timestamp).fromNow();
  }

  formatDateToMonthDayYearTimeStamp(time, invalidDateString = INVALID_DATE_MSG) {
    const t = moment(time);
    return t.isValid() ? t.format('MMM DD, YYYY h:mm:ss A') : invalidDateString;
  }

  formatTime(time, invalidDateString = la(INVALID_DATE_MSG), locale = window.navigator.language, format = 'x') {
    moment.locale(locale);
    const t = moment(time, format);
    return t.isValid() ? t.format('L HH:mm:ss') : invalidDateString;
  }

  formatTimeWithTZ(time, invalidDateString = la(INVALID_DATE_MSG), locale = window.navigator.language) {
    moment.locale(locale);
    const t = moment(time, 'x');
    return t.isValid() ? t.format('L HH:mm:ss Z') : invalidDateString;
  }

  formatTimeDiff(timeDiffMs, format = 'H:mm:ss') {
    return moment.utc(timeDiffMs).format(format);
  }

  isMoreThanYearsFromNow(time, years) {
    const last = moment().add(years, 'y');
    return moment(time).isAfter(last);
  }

  getUnixTime(stringTime, invalidDateString = la(INVALID_DATE_MSG)) {
    const t = moment(stringTime);
    return t.isValid() ? t.valueOf() : invalidDateString;
  }

  isInvalidDateTime(stringTime) {
    const t = moment(stringTime);
    return t.isValid() && t.valueOf() === 0;
  }

  toNow(timestamp) {
    if (!timestamp) {
      return 'unknown';
    }
    return moment(timestamp).toNow(true);
  }

  durationWithMS(duration, isNumberFormat = false) { // todo: loc
    const seconds = this.zeroesPadding(duration.seconds(), 2);
    const minutes = this.zeroesPadding(duration.minutes(), 2);
    const hours = this.zeroesPadding(Math.floor(duration.asHours()), 2);
    if (Math.floor(duration.asHours()) <= 0 && duration.minutes() <= 0 && duration.seconds() < 1) {
      return '<1s';
    } else if (Math.floor(duration.asHours()) <= 0 && duration.minutes() < 1 && duration.seconds() >= 1) {
      return moment.utc(duration.as('milliseconds')).format('ss.SS[s]', {
        minValue: 1
      });
    } else if (isNumberFormat) {
      return `${hours}:${minutes}:${seconds}`;
    }
    // todo: loc
    return hours > 0 ? moment.utc(duration.as('milliseconds')).format('HH[h]:mm[m]:ss[s]') :
      moment.utc(duration.as('milliseconds')).format('mm[m]:ss[s]');
  }

  nanoSecondsUpToHours = (nanos) => {
    if (nanos === undefined || Number.isNaN(nanos)) {
      return '--';
    }
    if (nanos > 1000 && nanos <= 1000000) {
      const micros = nanos / 1000;
      return `${micros.toFixed(2)}\u03BCs`;
    }
    if (nanos > 1000000 && nanos <= 1000000000) {
      const millis = nanos / 1000000;
      return `${millis.toFixed(2)}ms`;
    }
    if (nanos > 1000000000 && nanos <= 60000000000) {
      const seconds = nanos / 1000000000;
      return `${seconds.toFixed(2)}s`;
    }
    if (nanos > 60000000000 && nanos <= 3600000000000) {
      const minutes = nanos / 60000000000;
      return `${minutes.toFixed(2)}m`;
    }
    if (nanos > 3600000000000) {
      const hours = nanos / 3600000000000;
      return `${hours.toFixed(2)}hr`;
    }
    return `${nanos}ns`;
  }
}

TimeUtils.prototype.INVALID_DATE_MSG = INVALID_DATE_MSG;

TimeUtils.prototype.formats = {
  UNIX_TIMESTAMP: 'x',
  ISO: moment.ISO_8601
};

const timeUtils = new TimeUtils();

export default timeUtils;
