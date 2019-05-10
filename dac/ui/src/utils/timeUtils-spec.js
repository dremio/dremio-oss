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
import TimeUtils from './timeUtils';

describe('Tests for time utils', () => {
  describe('test zeroes padding function', () => {
    it('padding value smaller than', () => {
      expect(TimeUtils.zeroesPadding(20, 1)).to.equal('20');
      expect(TimeUtils.zeroesPadding('test', 2)).to.equal('test');
      expect(TimeUtils.zeroesPadding(true, 2)).to.equal('true');
    });
    it('padding value bigger than', () => {
      expect(TimeUtils.zeroesPadding(20, 3)).to.equal('020');
      expect(TimeUtils.zeroesPadding('test', 5)).to.equal('0test');
      expect(TimeUtils.zeroesPadding('test', 10)).to.equal('000000test');
      expect(TimeUtils.zeroesPadding(true, 6)).to.equal('00true');
    });
    it('padding value is zero', () => {
      expect(TimeUtils.zeroesPadding(20, 0)).to.equal('20');
      expect(TimeUtils.zeroesPadding('test', 0)).to.equal('test');
      expect(TimeUtils.zeroesPadding(true, 2)).to.equal('true');
    });
    it('padding value is negative', () => {
      expect(TimeUtils.zeroesPadding(20, -1)).to.equal('20');
      expect(TimeUtils.zeroesPadding('test', -1)).to.equal('test');
      expect(TimeUtils.zeroesPadding(true, -10)).to.equal('true');
    });
    it('padding value is equal', () => {
      expect(TimeUtils.zeroesPadding(20, 2)).to.equal('20');
      expect(TimeUtils.zeroesPadding('test', 4)).to.equal('test');
      expect(TimeUtils.zeroesPadding(true, 4)).to.equal('true');
    });
  });
  describe('test duration convert function', () => {
    it('several seconds duration', () => {
      let duration = moment.duration(2, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('2s');
      duration = moment.duration(5, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('5s');
      duration = moment.duration(0, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('<1s');
      duration = moment.duration(10, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('10s');
      duration = moment.duration(20, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('20s');
      duration = moment.duration(30, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('30s');
      duration = moment.duration(43, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('43s');
    });
    it('several minutes duration', () => {
      let duration = moment.duration(2, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('2m:00s');
      duration = moment.duration(5, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('5m:00s');
      duration = moment.duration(0, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('<1s');
      duration = moment.duration(10, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('10m:00s');
      duration = moment.duration(20, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('20m:00s');
      duration = moment.duration(30, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('30m:00s');
      duration = moment.duration(43, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('43m:00s');
    });
    it('several hours duration', () => {
      let duration = moment.duration(2, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('2h:00m:00s');
      duration = moment.duration(5, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('5h:00m:00s');
      duration = moment.duration(0, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('<1s');
      duration = moment.duration(10, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('10h:00m:00s');
      duration = moment.duration(20, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('20h:00m:00s');
      duration = moment.duration(30, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('30h:00m:00s');
      duration = moment.duration(43, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('43h:00m:00s');
    });
    it('various duration', () => {
      let duration = moment.duration({
        seconds: 2,
        minutes: 20
      });
      expect(TimeUtils.durationWithZero(duration)).to.equal('20m:02s');
      duration = moment.duration({
        seconds: 2,
        minutes: 0,
        hours: 2
      });
      expect(TimeUtils.durationWithZero(duration)).to.equal('2h:00m:02s');
      duration = moment.duration({
        seconds: 0,
        minutes: 5,
        hours: 22
      });
      expect(TimeUtils.durationWithZero(duration)).to.equal('22h:05m:00s');
      duration = moment.duration({
        seconds: 2,
        minutes: 0,
        hours: 32
      });
      expect(TimeUtils.durationWithZero(duration)).to.equal('32h:00m:02s');
      duration = moment.duration({
        seconds: 2,
        minutes: 20,
        hours: 2
      });
      expect(TimeUtils.durationWithZero(duration)).to.equal('2h:20m:02s');
    });
  });

  describe('formatTime', () => {
    const unixTimestamp = +new Date(2019, 1, 15, 20, 0, 1, 0);

    it('should return invalid message', () => {
      expect(TimeUtils.formatTime('')).to.equal('Invalid date');
    });
    it('should use MM/DD/YYYY HH:mm:ss for american english locale', () => {
      expect(TimeUtils.formatTime(unixTimestamp, 'Invalid', 'en-US')).to.equal('02/15/2019 20:00:01');
      // assuming default locale is en-US in our build/test environment
      expect(TimeUtils.formatTime(unixTimestamp)).to.equal('02/15/2019 20:00:01');
    });
    it('should use localized format for non en-US', () => {
      expect(TimeUtils.formatTime(unixTimestamp, 'Invalid', 'en-GB')).to.equal('15/02/2019 20:00:01');
    });
  });

  describe('formatTimeDiff', () => {
    it('should format min and sec', () => {
      expect(TimeUtils.formatTimeDiff(1000, 'mm:ss')).to.equal('00:01');
      expect(TimeUtils.formatTimeDiff(100000, 'mm:ss')).to.equal('01:40');
      expect(TimeUtils.formatTimeDiff( 61 * 60000, 'mm:ss')).to.equal('01:00');
    });
    it('should format hrs min and sec', () => {
      expect(TimeUtils.formatTimeDiff(1000)).to.equal('0:00:01');
      expect(TimeUtils.formatTimeDiff(100000)).to.equal('0:01:40');
      expect(TimeUtils.formatTimeDiff(61 * 60000)).to.equal('1:01:00');
    });

  });

});

