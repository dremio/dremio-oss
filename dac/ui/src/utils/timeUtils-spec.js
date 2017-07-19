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
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:00:02');
      duration = moment.duration(5, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:00:05');
      duration = moment.duration(0, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('<1s');
      duration = moment.duration(10, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:00:10');
      duration = moment.duration(20, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:00:20');
      duration = moment.duration(30, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:00:30');
      duration = moment.duration(43, 'seconds');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:00:43');
    });
    it('several minutes duration', () => {
      let duration = moment.duration(2, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:02:00');
      duration = moment.duration(5, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:05:00');
      duration = moment.duration(0, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('<1s');
      duration = moment.duration(10, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:10:00');
      duration = moment.duration(20, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:20:00');
      duration = moment.duration(30, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:30:00');
      duration = moment.duration(43, 'minutes');
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:43:00');
    });
    it('several hours duration', () => {
      let duration = moment.duration(2, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('02:00:00');
      duration = moment.duration(5, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('05:00:00');
      duration = moment.duration(0, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('<1s');
      duration = moment.duration(10, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('10:00:00');
      duration = moment.duration(20, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('20:00:00');
      duration = moment.duration(30, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('30:00:00');
      duration = moment.duration(43, 'hours');
      expect(TimeUtils.durationWithZero(duration)).to.equal('43:00:00');
    });
    it('various duration', () => {
      let duration = moment.duration({
        seconds: 2,
        minutes: 20
      });
      expect(TimeUtils.durationWithZero(duration)).to.equal('00:20:02');
      duration = moment.duration({
        seconds: 2,
        minutes: 0,
        hours: 2
      });
      expect(TimeUtils.durationWithZero(duration)).to.equal('02:00:02');
      duration = moment.duration({
        seconds: 0,
        minutes: 5,
        hours: 22
      });
      expect(TimeUtils.durationWithZero(duration)).to.equal('22:05:00');
      duration = moment.duration({
        seconds: 2,
        minutes: 0,
        hours: 32
      });
      expect(TimeUtils.durationWithZero(duration)).to.equal('32:00:02');
      duration = moment.duration({
        seconds: 2,
        minutes: 20,
        hours: 2
      });
      expect(TimeUtils.durationWithZero(duration)).to.equal('02:20:02');
    });
  });
});

