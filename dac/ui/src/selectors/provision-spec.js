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
import { getAllProvisions, getProvision } from './provision';

const state = {
  resources: {
    entities: Immutable.fromJS({
      provision: {
        '5': {
          id: {
            id: '5'
          },
          name: 'foo',
          clusterType: 'YARN'
        },
        '3': {
          id: {
            id: '3'
          },
          name: 'foo',
          clusterType: 'YARN'
        },
        '4': {
          id: {
            id: '4'
          },
          name: 'baz',
          clusterType: 'YARN'
        },
        '1': {
          id: {
            id: '1'
          },
          name: 'bar',
          clusterType: 'MESOS'
        },
        '2': {
          id: {
            id: '2'
          },
          name: 'abc',
          clusterType: 'AMAZON'
        }
      }
    })
  }
};

describe('provision selectors', () => {
  describe('getProvision', () => {
    it('should get provision by id', () => {
      const provision = getProvision(state, '2').toJS();
      expect(provision).to.eql({
        id: {
          id: '2'
        },
        name: 'abc',
        clusterType: 'AMAZON'
      });
    });
  });
  describe('getAllProvisions', () => {
    it('should be sorted by name and then by clusterType then by id', () => {
      const provisions = getAllProvisions(state);
      expect(provisions.get(0).toJS()).to.eql({
        id: {
          id: '2'
        },
        name: 'abc',
        clusterType: 'AMAZON'
      });
      expect(provisions.get(1).toJS()).to.eql({
        id: {
          id: '1'
        },
        name: 'bar',
        clusterType: 'MESOS'
      });
      expect(provisions.get(2).toJS()).to.eql({
        id: {
          id: '4'
        },
        name: 'baz',
        clusterType: 'YARN'
      });
      expect(provisions.get(3).toJS()).to.eql({
        id: {
          id: '3'
        },
        name: 'foo',
        clusterType: 'YARN'
      });
      expect(provisions.get(4).toJS()).to.eql({
        id: {
          id: '5'
        },
        name: 'foo',
        clusterType: 'YARN'
      });
    });
  });
});
