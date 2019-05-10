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
import { mount, shallow } from 'enzyme';
import Immutable from 'immutable';
import { minimalFormProps } from 'testUtil';

import { SettingsMicroForm } from './SettingsMicroForm';
import { LABELS } from './settingsConfig';

describe('SettingsMicroForm', () => {

  const dirtyFields = {value: {dirty: true}};

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    LABELS.$c = 'C Label';

    minimalProps = {
      ...minimalFormProps(),
      settingId: '123',
      putSetting: sinon.stub().returns(Promise.resolve()),
      getSetting: sinon.stub().returns(Promise.resolve()),
      fields: {value: {dirty: false}},
      viewId: 'view-id'
    };
    commonProps = {
      ...minimalProps,
      settingId: '$c',
      setting: Immutable.fromJS({id: '$c', type: 'TEXT'})
    };
  });

  afterEach(() => {
    delete LABELS.$c;
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SettingsMicroForm {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  describe('#render', () => {
    it('when label is defined', () => {
      const wrapper = mount(<SettingsMicroForm {...commonProps} />);
      expect(!!wrapper.text().match(/C Label/)).to.be.true;
    });

    it('when label is not defined', () => {
      delete LABELS.$c;
      const wrapper = mount(<SettingsMicroForm {...commonProps} />);
      expect(!!wrapper.text().match(/\$c/)).to.be.true;
    });

    it('when field is not dirty', () => {
      const wrapper = shallow(<SettingsMicroForm {...commonProps} />);
      expect(wrapper.find('SimpleButton').props().style.visibility).to.be.equal('hidden');
    });

    it('when field is dirty', () => {
      const wrapper = shallow(<SettingsMicroForm {...commonProps} fields={dirtyFields} />);
      expect(wrapper.find('SimpleButton').props().style.visibility).to.be.equal('visible');
    });

    it('with reset', () => {
      const resetSetting = () => {};
      const wrapper = shallow(<SettingsMicroForm {...commonProps} resetSetting={resetSetting} />);
      expect(wrapper.find('SimpleButton').length).to.be.equal(2);

      const saveButton = wrapper.find('SimpleButton').at(0);
      expect(saveButton.props().style.visibility).to.be.equal('hidden');
      expect(saveButton.props().style.display).to.be.equal('none');
    });
  });

  describe('#renderField', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<SettingsMicroForm {...commonProps} />);
      instance = wrapper.instance();
    });

    it('with integer or float type', () => {
      wrapper.setProps({
        setting: commonProps.setting.set('type', 'INTEGER')
      });
      expect(shallow(instance.renderField()).props().type).to.be.eql('number');
      expect(instance.renderField().type.displayName).to.be.eql('TextField');
    });

    it('with boolean type', () => {
      wrapper.setProps({
        setting: commonProps.setting.set('type', 'BOOLEAN')
      });
      expect(shallow(instance.renderField()).find('Toggle')).to.have.length(1);
    });

    it('with default type', () => {
      expect(shallow(instance.renderField()).props().type).to.be.eql('text');
      expect(instance.renderField().type.displayName).to.be.eql('TextField');
    });
  });

  describe('#submit', () => {
    it('when not dirty', () => {
      const instance = shallow(<SettingsMicroForm {...commonProps} />).instance();
      instance.submit();
      expect(commonProps.putSetting).to.not.have.been.called;
    });

    it('with non-numeric type', () => {
      const instance = shallow(<SettingsMicroForm {...commonProps} fields={dirtyFields} />).instance();
      const promise = instance.submit({value: 'foo'});
      expect(commonProps.putSetting).to.have.been.calledWith(
        { ...commonProps.setting.toJS(), value: 'foo' },
        { viewId: 'view-id' }
      );
      return promise;
    });

    it('with numeric type', () => {
      commonProps.setting = commonProps.setting.set('type', 'FLOAT');

      const instance = shallow(<SettingsMicroForm {...commonProps} fields={dirtyFields} />).instance();
      const promise = instance.submit({value: '1.1'});
      expect(commonProps.putSetting).to.have.been.calledWith(
        { ...commonProps.setting.toJS(), value: 1.1 },
        { viewId: 'view-id' }
      );
      return promise;
    });
  });

});
