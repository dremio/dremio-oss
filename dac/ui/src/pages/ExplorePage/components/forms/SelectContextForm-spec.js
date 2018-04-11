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

import ResourceTreeController from 'components/Tree/ResourceTreeController';
import { minimalFormProps } from 'testUtil';

import { SelectContextForm, FIELDS } from './SelectContextForm';

describe('SelectContextForm', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      onCancel: sinon.spy(),
      onFormSubmit: sinon.spy(),
      ...minimalFormProps(FIELDS)
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SelectContextForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render ModalForm, FormBody and ResourceTree', () => {
    const wrapper = shallow(<SelectContextForm {...commonProps}/>);
    expect(wrapper.type()).to.equal('div');

    expect(wrapper.find('ModalForm')).to.have.length(1);
    expect(wrapper.find('FormBody')).to.have.length(1);
    expect(wrapper.find(ResourceTreeController)).to.have.length(1);
  });

  it('should render FieldWithError wrapping a TextField with props from this.props.fields.context', () => {
    const wrapper = shallow(<SelectContextForm {...commonProps}/>);
    expect(wrapper.find('FieldWithError').prop('value')).to.equal(commonProps.fields.context.value);
    expect(wrapper.find('TextField').prop('value')).to.equal(commonProps.fields.context.value);
  });

  it('should call fields.context.onChange when a folder is selected in the ResourceTree', () => {
    const wrapper = shallow(<SelectContextForm {...commonProps}/>);
    const selected = 'my.folder';
    wrapper.instance().handleChangeSelectedNode(selected);
    expect(commonProps.fields.context.onChange).to.have.been.calledWith(selected);
  });
});
