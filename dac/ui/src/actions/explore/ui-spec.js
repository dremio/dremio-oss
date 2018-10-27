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
import { expect } from 'chai';

import * as Actions from './ui.js';

describe('ui actions', () => {
  describe('test constants', () => {
    it('verify existence of SET_SQL_EDITOR_SIZE', () => {
      expect(Actions.SET_SQL_EDITOR_SIZE).to.not.be.undefined;
    });
    it('verify existence of TOGGLE_EXPLORE_SQL', () => {
      expect(Actions.TOGGLE_EXPLORE_SQL).to.not.be.undefined;
    });
  });

  describe('test action creators', () => {
    it('test results of calling of function updateSqlPartSize', () => {
      const size = 20;
      const expectedResult = { type: Actions.SET_SQL_EDITOR_SIZE, size };
      expect(Actions.updateSqlPartSize(size)).to.eql(expectedResult);
    });
    it('test results of calling of function toggleExploreSql', () => {
      const expectedResult = { type: Actions.TOGGLE_EXPLORE_SQL};
      expect(Actions.toggleExploreSql()).to.eql(expectedResult);
    });
  });
});
