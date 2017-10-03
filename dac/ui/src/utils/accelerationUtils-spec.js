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
import {summarizeState, summarizeByteSize, syntheticLayoutState} from './accelerationUtils';

describe('accelerationUtils', () => {
  describe('summarizeState (in reverse order of precedence...)', () => {
    // this test intentionally builds up a (shared) object in order to test precedence
    let acceleration;
    before(() => {
      acceleration = {
        rawLayouts: {
          enabled: true,
          layoutList: [{latestMaterializationState: 'DONE', hasValidMaterialization: true}]
        },
        aggregationLayouts: {
          enabled: true,
          layoutList: [{latestMaterializationState: 'DONE', hasValidMaterialization: true}]
        }
      };
    });

    it('all (enabled) layouts DONE', () => {
      expect(summarizeState(acceleration)).to.equal('DONE');

      acceleration.aggregationLayouts.enabled = false;
      expect(summarizeState(acceleration)).to.equal('DONE');
    });

    it('any layouts NEW', () => {
      acceleration.rawLayouts.layoutList[0].latestMaterializationState = 'NEW';
      expect(summarizeState(acceleration)).to.equal('NEW');
    });

    it('any layouts RUNNING', () => {
      acceleration.rawLayouts.layoutList.push({latestMaterializationState: 'RUNNING', hasValidMaterialization: true});
      expect(summarizeState(acceleration)).to.equal('RUNNING');
    });

    it('any layouts EXPIRED', () => {
      acceleration.rawLayouts.layoutList.push({latestMaterializationState: 'DONE', hasValidMaterialization: false});
      expect(summarizeState(acceleration)).to.equal('EXPIRED');
    });

    it('any layouts FAILED_NONFINAL', () => {
      acceleration.rawLayouts.layoutList.push({latestMaterializationState: 'FAILED', state: 'ACTIVE'});
      expect(summarizeState(acceleration)).to.equal('FAILED_NONFINAL');
    });

    it('any layouts FAILED_FINAL', () => {
      acceleration.rawLayouts.layoutList.push({latestMaterializationState: 'FAILED', state: 'FAILED'});
      expect(summarizeState(acceleration)).to.equal('FAILED_FINAL');
    });

    it('all layout types disabled', () => {
      acceleration.rawLayouts.enabled = false;
      expect(summarizeState(acceleration)).to.equal('DISABLED');
    });

    it('when generating suggestions', () => {
      acceleration.state = 'NEW';
      expect(summarizeState(acceleration)).to.equal('NEW');
    });

    it('when there are top-level Acceleration errors', () => {
      acceleration.errorList = [{}];
      expect(summarizeState(acceleration)).to.equal('FAILED');
    });
  });

  describe('syntheticLayoutState', () => {
    it('default pass thru of latestMaterializationState', () => {
      expect(syntheticLayoutState({latestMaterializationState: 'CANARY'})).to.equal('CANARY');
    });
    it('FAILED_FINAL', () => {
      expect(syntheticLayoutState({latestMaterializationState: 'FAILED', state: 'FAILED'})).to.equal('FAILED_FINAL');
    });
    it('FAILED_NONFINAL', () => {
      expect(syntheticLayoutState({latestMaterializationState: 'FAILED', state: 'ACTIVE'})).to.equal('FAILED_NONFINAL');
    });
    it('EXPIRED', () => {
      expect(syntheticLayoutState({latestMaterializationState: 'DONE', hasValidMaterialization: false})).to.equal('EXPIRED');
    });
  });

  describe('summarizeByteSize', () => {
    it('generates sums', () => {
      const acceleration = {
        rawLayouts: {
          layoutList: [{totalByteSize: 100, currentByteSize: 10}, {totalByteSize: 110, currentByteSize: 11}]
        },
        aggregationLayouts: {
          layoutList: [{totalByteSize: 111}] // currentByteSize is undefined if this is no current materialization
        }
      };

      expect(summarizeByteSize(acceleration)).to.eql({totalByteSize: 321, currentByteSize: 21});
    });
  });
});
