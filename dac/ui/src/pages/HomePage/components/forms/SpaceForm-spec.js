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
import { shallow } from 'enzyme';
import { injectIntl } from 'react-intl';
import { SpaceForm } from './SpaceForm';

const SpaceFormIntl = injectIntl(SpaceForm);

describe('SpaceForm', () => {

  let commonProps;
  let context;
  beforeEach(() => {
    commonProps = {
      fields: {
        name: {},
        description: {}
      },
      handleSubmit: sinon.stub().returns(sinon.spy()),
      onFormSubmit: sinon.stub(),
      onCancel: sinon.stub(),
      updateFormDirtyState: sinon.stub(),
      editing: false
    };
  });

  it('should render ModalForm, FormBody and General', () => {
    const wrapper = shallow(<SpaceFormIntl {...commonProps}/>, {context});
    expect(wrapper.find('ModalForm')).to.have.length(1);
    expect(wrapper.find('FormBody')).to.have.length(1);
    expect(wrapper.find('General')).to.have.length(1);

    const generalProps = wrapper.find('General').props();
    expect(generalProps.fields).to.equal(commonProps.fields);
    expect(generalProps.editing).to.equal(commonProps.editing);
  });
});
