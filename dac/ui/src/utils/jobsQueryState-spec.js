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
  parseQueryState,
  renderQueryState,
  renderQueryStateForServer
} from './jobsQueryState';

describe('jobsQueryState', () => {

  describe('parseQueryState', () => {
    it('parses query state from query', () => {
      const result = parseQueryState({
        filters: '{"qt":["UI","EXTERNAL"]}',
        sort: 'qt',
        order: 'ASCENDING'
      });
      expect(result.toJS()).to.eql({
        filters: {qt: ['UI', 'EXTERNAL']},
        sort: 'qt',
        order: 'ASCENDING'
      });
    });

    it('defaults sort and order', () => {
      const result = parseQueryState({filters: '{"qt":["UI","EXTERNAL"]}'});
      expect(result.toJS()).to.eql({
        filters: {qt: ['UI', 'EXTERNAL']},
        sort: 'st',
        order: 'DESCENDING'
      });
    });
  });

  describe('renderQueryState', () => {
    it('should render filter as json', () => {
      const result = renderQueryState(Immutable.fromJS({
        filters: {qt: ['UI', 'EXTERNAL']},
        sort: 'qt',
        order: 'ASCENDING'
      }));
      expect(result).to.eql({
        sort: 'qt',
        order: 'ASCENDING',
        filters: '{"qt":["UI","EXTERNAL"]}'
      });
    });
  });

  describe('renderQueryStateForServer', () => {
    it('should render normal filters', () => {
      let result = renderQueryStateForServer(Immutable.fromJS({
        filters: {qt: ['UI', 'EXTERNAL']}
      }));
      expect(result).to.eql(`filter=${encodeURIComponent('(qt=="UI",qt=="EXTERNAL")')}`);
      result = renderQueryStateForServer(Immutable.fromJS({
        filters: {asd: ['"@dremio"."sys+v\\ersion"']}
      }));
      expect(result).to.eql(`filter=${encodeURIComponent('(asd=="\\"@dremio\\".\\"sys+v\\\\ersion\\"")')}`);
    });

    it('should render date filter', () => {
      const result = renderQueryStateForServer(Immutable.fromJS({
        filters: {st: [12345, 12345]}
      }));
      expect(result).to.eql('filter=' + encodeURIComponent('(st=gt=12345;st=lt=12345)'));
    });

    it('should render contains filter', () => {
      const result = renderQueryStateForServer(Immutable.fromJS({
        filters: {contains: ['"@dremio"."a+\\folder"']}
      }));
      expect(result).to.eql('filter=' + encodeURIComponent('*=contains="\\"@dremio\\".\\"a+\\\\folder\\""'));
    });
  });
});
