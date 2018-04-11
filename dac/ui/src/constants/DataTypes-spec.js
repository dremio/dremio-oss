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
import {
  getDefaultValue,
  convertToUnix,
  parseTextToDataType,
  BOOLEAN,
  TEXT,
  FLOAT,
  INTEGER,
  TIME,
  DATE,
  DATETIME
} from './DataTypes';

describe('DataTypes', () => {
  const timeTest = '1481901812000';
  const dateTest = '1429543412000';
  const dateTimeTest = '1429543412000';

  describe('getDefaultValue check', () => {
    it('should return 0 as default for FLOAT and INTEGER', () => {
      expect(getDefaultValue(FLOAT)).to.be.eql(0);
      expect(getDefaultValue(INTEGER)).to.be.eql(0);
    });
    it('should return default value for date type', () => {
      expect(getDefaultValue(TIME)).to.be.eql('00:00:00');
      expect(getDefaultValue(DATETIME)).to.be.eql('1970-01-01 00:00:00');
      expect(getDefaultValue(DATE)).to.be.eql('1970-01-01');
    });
    it('should format correctly if date type', () => {
      expect(getDefaultValue(TIME, timeTest)).to.be.eql('15:23:32');
      expect(getDefaultValue(DATE, dateTest)).to.be.eql('2015-04-20');
      expect(getDefaultValue(DATETIME, dateTimeTest)).to.be.eql('2015-04-20 15:23:32');
    });
  });
  describe('convertToUnix check', () => {
    it('should return null if value is not present', () => {
      expect(convertToUnix()).to.be.eql(null);
    });
    it('should correctly convert value unix timestamp according to column type', () => {
      const timeResult = 55412000;
      const dateResult = 1429488000000;
      const dateTimeResult = 1429543412000;
      expect(convertToUnix('15:23:32', TIME)).to.be.eql(timeResult);
      expect(convertToUnix('2015-04-20', DATE)).to.be.eql(dateResult);
      expect(convertToUnix('2015-04-20 15:23:32', DATETIME)).to.be.eql(dateTimeResult);
    });
  });

  describe('parseTextToDataType', () => {


    describe('when dataType == BOOLEAN', () => {
      it('should return true for "true"', () => {
        expect(parseTextToDataType('true', BOOLEAN)).to.be.true;
      });
      it('should return false for "false"', () => {
        expect(parseTextToDataType('false', BOOLEAN)).to.be.false;
      });
    });
    describe('when dataType == FLOAT', () => {
      it('should return 0.02 for "0.02"', () => {
        expect(parseTextToDataType('0.02', FLOAT)).to.equal(0.02);
      });
    });

    describe('when dataType == TIME', () => {
      it('should convertToUnix', () => {
        expect(parseTextToDataType('15:23:32', TIME)).to.equal(convertToUnix('15:23:32', TIME));
      });
    });

    it('should text for other types', () => {
      expect(parseTextToDataType('foo', TEXT)).to.equal('foo');
    });
  });
});
