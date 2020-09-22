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
import { shallow } from 'enzyme';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import * as VersionUtils from 'utils/versionUtils';
import UserForm, { FIELDS } from './UserForm';

describe('UserForm', () => {
  let minimalProps;
  beforeEach(() => {
    const fields = FIELDS.reduce((acc, key) => {
      acc[key] = {value: ''};
      return acc;
    }, {});
    minimalProps = {
      fields
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<UserForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render all fields by default', () => {
    const wrapper = shallow(<UserForm {...minimalProps}/>);
    const formLabels = wrapper.find('FieldWithError').map(e => e.prop('label'));
    const expectedLabels = [
      'First Name', 'Last Name', 'Username', 'Email', 'Password', 'Confirm Password'
    ];
    expect(formLabels).to.eql(expectedLabels);

    const disableStates = wrapper.find('TextField').map(e => e.prop('disabled'));
    expect(disableStates).to.eql(new Array(4).fill(false));
  });

  it('should render limited fields when isReadMode', () => {
    const wrapper = shallow(<UserForm {...minimalProps} isReadMode/>);
    const formLabels = wrapper.find('FieldWithError').map(e => e.prop('label'));
    const expectedLabels = [
      'First Name', 'Last Name', 'Username', 'Email'
    ];
    expect(formLabels).to.eql(expectedLabels);

    const disableStates = wrapper.find('TextField').map(e => e.prop('disabled'));
    expect(disableStates).to.eql(new Array(4).fill(true));
  });

  describe('#getIsEdit', () => {
    it('should be false when creating a user', () => {
      const instance = shallow(<UserForm {...minimalProps} />).instance();
      expect(instance.getIsEdit()).to.be.false;
    });
    it('should be true when editing a user', () => {
      const instance = shallow(<UserForm {...minimalProps} />).instance();
      instance.props.fields.tag.value = 'foo';
      expect(instance.getIsEdit()).to.be.true;
    });
  });

  describe('instanceId field', () => {
    it('should not show instanceId field', () => {
      sinon.stub(localStorageUtils, 'getInstanceId').returns('iid');
      const wrapper = shallow(<UserForm {...minimalProps} isReadMode/>);
      const formLabels = wrapper.find('FieldWithError').map(e => e.prop('label'));
      expect(formLabels.includes('Instance-id for Authentication')).to.be.false;
      localStorageUtils.getInstanceId.restore();
    });
    it('should show instanceId field', () => {
      sinon.stub(localStorageUtils, 'getInstanceId').returns(null);
      sinon.stub(VersionUtils, 'getEditionFromConfig').returns('ME');
      // global.window.dremioConfig = {edition: 'ME'};

      const wrapper = shallow(<UserForm {...minimalProps} isReadMode/>);
      const formLabels = wrapper.find('FieldWithError').map(e => e.prop('label'));
      expect(formLabels.includes('Instance-id for Authentication')).to.be.true;

      localStorageUtils.getInstanceId.restore();
      VersionUtils.getEditionFromConfig.restore();
    });
  });
});
