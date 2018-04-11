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
import Immutable from 'immutable';

import { MAP, LIST, BOOLEAN, TEXT } from 'constants/DataTypes';
import { UNMATCHED_CELL_VALUE, EMPTY_NULL_VALUE, EMPTY_STRING_VALUE } from './dataFormatUtils';
import DataFormatUtils from './dataFormatUtils';


describe('#DataFormatUtils', () => {
  describe('#formatValue', () => {
    let row;

    beforeEach(() => {
      row = Immutable.Map({
        isDeleted: false
      });
    });

    it('should return EMPTY_NULL_VALUE when value is undefined', () => {
      expect(DataFormatUtils.formatValue(undefined, TEXT, row)).to.be.equal(EMPTY_NULL_VALUE);
    });

    it('should return EMPTY_STRING_VALUE when value is empty string', () => {
      expect(DataFormatUtils.formatValue('', TEXT, row)).to.be.equal(EMPTY_STRING_VALUE);
    });

    it('should return EMPTY_NULL_VALUE when value is null', () => {
      expect(DataFormatUtils.formatValue(null, TEXT, row)).to.be.equal(EMPTY_NULL_VALUE);
    });

    it('should return UNMATCHED_CELL_VALUE when row deleted', () => {
      row = row.set('isDeleted', true);
      expect(DataFormatUtils.formatValue(null, TEXT, row)).to.be.equal(UNMATCHED_CELL_VALUE);
    });

    it('should return string when value is BOOLEAN', () => {
      expect(DataFormatUtils.formatValue(true, BOOLEAN, row)).to.be.equal('true');
    });

    it('should return string when value is LIST', () => {
      expect(DataFormatUtils.formatValue([], LIST, row)).to.be.equal('[]');
    });

    it('should return string when value is MAP', () => {
      expect(DataFormatUtils.formatValue({}, MAP, row)).to.be.equal('{}');
    });
  });
});
