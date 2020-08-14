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

// import {AWS_INSTANCE_TYPE_OPTIONS} from '@app/constants/provisioningPage/ec2FormConstants';
// import { isEditMode, isRestartRequired, addPropsForSave, getInstanceTypeValue } from './provisioningFormUtil';
import {
  isRestartRequired,
  addPropsForSave,
  getInstanceTypeValue,
  preparePropertyListFieldForSave,
  getInitPropListValue
} from './provisioningFormUtil';


describe('#isRestartRequired', () => {
  const provision = {id: 'foo', currentState: 'RUNNING'};

  it('return true if any children dirty', () => {
    expect(isRestartRequired(Immutable.fromJS(provision), true)).to.be.true;
  });
  it('return false if not edit mode', () => {
    expect(isRestartRequired(Immutable.fromJS({}), true)).to.be.false;
  });
  it('return false if currentState is not RUNNING', () => {
    expect(isRestartRequired(Immutable.fromJS({...provision, currentState: 'STOPPED'}), true)).to.be.false;
  });
  it('return false if there is nothing dirty', () => {
    expect(isRestartRequired(Immutable.fromJS({...provision}), false)).to.be.false;
  });
});

describe('#addPropsForSave', () => {
  it('adds values for props', () => {
    let result = addPropsForSave({}, [], {});
    expect(result).to.eql({});
    result = addPropsForSave(result, ['a', 'b'], {a: 'aa', b: 'bb', c: 'cc'});
    expect(result).to.eql({a: 'aa', b: 'bb'});
    result = addPropsForSave(result, ['c', 'd'], {a: 'aa', b: 'bb', c: 'cc'});
    expect(result).to.eql({a: 'aa', b: 'bb', c: 'cc', d: undefined});
  });
});


describe('#getInstanceTypeValue', () => {
  it('returns label if label is not forund', () => {
    expect(getInstanceTypeValue('foo')).to.equal('foo');
  });
  it('returns value for valid label', () => {
    expect(getInstanceTypeValue('Standard m5d.8xlarge (32c/128gb)')).to.equal('m5d.8xlarge');
    expect(getInstanceTypeValue('High Memory r5d.4xlarge (16c/128gb)')).to.equal('r5d.4xlarge');
  });
});

describe('#preparePropertyListFieldForSave', () => {
  it('returns empty array by default', () => {
    expect(preparePropertyListFieldForSave({})).to.eql({ awsTags: [] });
  });
  it('returns key-value pairs for tags', () => {
    const values = {
      awsTags: [
        {id: 1, name: 'a', value: 'A'},
        {id: 2, name: 'b', value: 'B'}
      ]
    };
    const expected = {
      awsTags: [
        { key: 'a', value: 'A' },
        { key: 'b', value: 'B' }
      ]
    };
    expect(preparePropertyListFieldForSave(values)).to.eql(expected);
  });
});

describe('#getInitPropListValue', () => {
  it('returns empty array by default', () => {
    expect(getInitPropListValue(Immutable.fromJS({}), 'awsTags')).to.eql([]);
  });
  it('prepares init value for property list', () => {
    const provision = Immutable.fromJS({awsProps: {awsTags: [
      {key: 'a', value: 'A'},
      {key: 'b', value: 'B'}
    ]}});
    const expected = [
      {id: 0, name: 'a', value: 'A'},
      {id: 1, name: 'b', value: 'B'}
    ];
    expect(getInitPropListValue(provision, 'awsTags')).to.eql(expected);
  });
});

//TODO test prepareProvisionValuesForSave when the fields are finalized
