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
import moment from 'moment';

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

  durationWithZero(duration) { // todo: loc
    const seconds = this.zeroesPadding(duration.seconds(), 2);
    const minutes = this.zeroesPadding(duration.minutes(), 2);
    const hours = this.zeroesPadding(Math.floor(duration.asHours()), 2);
    if (Math.floor(duration.asHours()) <= 0 && duration.minutes() <= 0 && duration.seconds() < 1) {
      return '<1s';
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

  formatTime(time, invalidDateString = la('Invalid date'), locale = window.navigator.language) {
    moment.locale(locale);
    const t = moment(time, 'x');
    return t.isValid() ? t.format('L HH:mm:ss') : invalidDateString;
  }

  formatTimeDiff(timeDiffMs, format = 'H:mm:ss') {
    return moment.utc(timeDiffMs).format(format);
  }
}

const timeUtils = new TimeUtils();

export default timeUtils;
