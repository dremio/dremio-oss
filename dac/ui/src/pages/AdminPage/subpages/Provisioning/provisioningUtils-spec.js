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
import Immutable from 'immutable';
import { getEngineSizeLabel, getNodeCount, getIsInReadOnlyState } from './provisioningUtils';

describe('getIsInReadOnlyState', () => {
  it('should default to false', () => {
    expect(getIsInReadOnlyState()).to.equal(false);
    expect(getIsInReadOnlyState({})).to.equal(false);
    expect(getIsInReadOnlyState(Immutable.fromJS({currentState: 'RUNNING'}))).to.equal(false);
  });
  it('should return true for STARTING/STOPPING', () => {
    expect(getIsInReadOnlyState(Immutable.fromJS({currentState: 'STARTING'}))).to.equal(true);
    expect(getIsInReadOnlyState(Immutable.fromJS({currentState: 'STOPPING'}))).to.equal(true);
  });
  it('should return true for DELETING', () => {
    expect(getIsInReadOnlyState(Immutable.fromJS({desiredState: 'DELETED'}))).to.equal(true);
  });
});

describe('getEngineSizeLabel', () => {
  it('should find standard size label', () => {
    expect(getEngineSizeLabel(2)).to.equal('Small - 2');
    expect(getEngineSizeLabel(4)).to.equal('Medium - 4');
    expect(getEngineSizeLabel(64)).to.equal('3X Large - 64');
  });

  it('should use custom size for non-standard', () => {
    expect(getEngineSizeLabel(6)).to.equal('Custom - 6');
    expect(getEngineSizeLabel(100)).to.equal('Custom - 100');
  });

});

describe('getNodeCount', () => {
  it('should default to zero', () => {
    expect(getNodeCount( Immutable.fromJS({}))).to.equal(0);
    expect(getNodeCount( Immutable.fromJS({ workersSummary: {}}))).to.equal(0);
  });

  it('should get total from workersSummary', () => {
    expect(getNodeCount( Immutable.fromJS({ workersSummary: { total: 2 }}))).to.equal(2);
  });
});
