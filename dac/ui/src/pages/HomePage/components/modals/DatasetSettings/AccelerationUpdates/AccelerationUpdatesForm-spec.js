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
import { shallow } from 'enzyme';
import Immutable from 'immutable';
import { minimalFormProps } from 'testUtil';

import DataFreshnessSection from 'components/Forms/DataFreshnessSection';
import { AccelerationUpdatesForm } from './AccelerationUpdatesForm';

describe('AccelerationUpdatesForm', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    const fieldNames = ['method', 'refreshField'];
    const values = {
      method: 'FULL',
      accelerationRefreshPeriod: DataFreshnessSection.defaultFormValueRefreshInterval(),
      accelerationGracePeriod: DataFreshnessSection.defaultFormValueGracePeriod(),
      accelerationNeverExpire: false,
      accelerationNeverRefresh: false
    };
    minimalProps = {
      ...minimalFormProps(fieldNames),
      datasetFields: Immutable.List(),
      values
    };
    commonProps = {
      ...minimalProps,
      entityType: 'dataset',
      values: {
        ...values,
        method: 'INCREMENTAL',
        refreshField: 'col1'
      }
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AccelerationUpdatesForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
    expect(wrapper.find('Radio').at(1).props().label).to.be.eql('Incremental update');
  });

  it('should render Incremental option as disabled when #canUseIncremental() is false', () => {
    const stub = sinon.stub(AccelerationUpdatesForm.prototype, 'canUseIncremental').returns(false);
    const wrapper = shallow(<AccelerationUpdatesForm {...minimalProps}/>);
    expect(wrapper.find('Radio').at(1).props().disabled).to.be.true;
    stub.restore();
  });

  it('should render Incremental option as enabled when #canUseIncremental() is true', () => {
    const stub = sinon.stub(AccelerationUpdatesForm.prototype, 'canUseIncremental').returns(true);
    const wrapper = shallow(<AccelerationUpdatesForm {...minimalProps}/>);
    expect(wrapper.find('Radio').at(1).props().disabled).to.be.false;
    stub.restore();
  });

  it('should render Incremental option for folder', () => {
    const wrapper = shallow(<AccelerationUpdatesForm {...minimalProps} entityType='folder'/>);
    expect(wrapper.find('Radio').at(1).props().label).to.be.eql('Incremental update based on new files');
  });

  describe('#canUseIncremental()', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<AccelerationUpdatesForm {...minimalProps}/>);
      instance = wrapper.instance();
    });

    it('should return false when entity is file', () => {
      wrapper.setProps({ entityType: 'file' });
      expect(instance.canUseIncremental()).to.be.false;
    });

    it('should return true when entity is folder', () => {
      wrapper.setProps({ entityType: 'folder' });
      expect(instance.canUseIncremental()).to.be.true;
    });

    it('should return false when entity is physicalDataset and datasetFields is empty', () => {
      wrapper.setProps({ entityType: 'physicalDataset' });
      expect(instance.canUseIncremental()).to.be.false;
    });

    it('should return true when entity is physicalDataset and datasetFields is not empty', () => {
      wrapper.setProps({ entityType: 'physicalDataset', datasetFields: Immutable.List(['foo']) });
      expect(instance.canUseIncremental()).to.be.true;
    });
  });

  describe('#requiresIncrementalFieldSelection()', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<AccelerationUpdatesForm {...minimalProps}/>);
      instance = wrapper.instance();
    });

    it('should return false when method is not incremental', () => {
      expect(instance.requiresIncrementalFieldSelection({})).to.be.false;
    });

    it('should return false when method is incremental and entityType is not physicalDataset', () => {
      wrapper.setProps({ entityType: 'folder' });
      expect(instance.requiresIncrementalFieldSelection({method: 'INCREMENTAL'})).to.be.false;
    });

    it('should return true when method is incremental and entityType is physicalDataset', () => {
      wrapper.setProps({ entityType: 'physicalDataset' });
      expect(instance.requiresIncrementalFieldSelection({method: 'INCREMENTAL'})).to.be.true;
    });
  });

  describe('#mapFormValues', () => {
    it('should not include fieldList and refreshField fields when requiresIncrementalFieldSelection() is false', () => {
      const stub = sinon.stub(AccelerationUpdatesForm.prototype, 'requiresIncrementalFieldSelection').returns(false);

      const wrapper = shallow(<AccelerationUpdatesForm {...minimalProps}/>);
      const instance = wrapper.instance();
      expect(instance.mapFormValues(minimalProps.values)).to.be.eql(minimalProps.values);

      stub.restore();
    });

    it('should include fieldList and refreshField fields when requiresIncrementalFieldSelection() is true', () => {
      const stub = sinon.stub(AccelerationUpdatesForm.prototype, 'requiresIncrementalFieldSelection').returns(true);

      const wrapper = shallow(<AccelerationUpdatesForm {...minimalProps}/>);
      const instance = wrapper.instance();

      wrapper.setProps({ ...commonProps});
      const expectedValues = {
        method: 'INCREMENTAL',
        accelerationRefreshPeriod: DataFreshnessSection.defaultFormValueRefreshInterval(),
        accelerationGracePeriod: DataFreshnessSection.defaultFormValueGracePeriod(),
        accelerationNeverExpire: false,
        accelerationNeverRefresh: false,
        fieldList: ['col1'],
        refreshField: 'col1'
      };
      expect(instance.mapFormValues(commonProps.values)).to.be.eql(expectedValues);

      stub.restore();
    });


    it('should include never expire/never refresh', () => {
      const stub = sinon.stub(AccelerationUpdatesForm.prototype, 'requiresIncrementalFieldSelection').returns(true);

      const wrapper = shallow(<AccelerationUpdatesForm {...minimalProps}/>);
      const instance = wrapper.instance();

      const props = {
        ...minimalProps,
        accelerationNeverExpire: true,
        accelerationNeverRefresh: true
      };

      wrapper.setProps({ ...props});
      expect(instance.mapFormValues(props).accelerationNeverExpire).to.be.eql(true);
      expect(instance.mapFormValues(props).accelerationNeverExpire).to.be.eql(true);

      stub.restore();
    });
  });
});
