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

import SpaceForm from 'pages/HomePage/components/forms/SpaceForm';
import { SpaceModal as SpaceModalBase } from 'pages/HomePage/components/modals/SpaceModal';
import SpaceModalMixin from './SpaceModalMixin';

@SpaceModalMixin
class SpaceModal extends SpaceModalBase {}

describe('SpaceModalMixin', () => {
  let commonProps;
  beforeEach(() => {
    commonProps = {
      isOpen: false,
      hide: sinon.spy(),
      createNewSpace: sinon.stub().returns(Promise.resolve()),
      updateSpace: sinon.stub().returns(Promise.resolve()),
      showConfirmationDialog: sinon.stub()
    };
  });

  it('renders <SpaceForm> with no initialValues when no entity', () => {
    const wrapper = shallow(<SpaceModal {...commonProps} />);
    const formProps = wrapper.find(SpaceForm).props();
    expect(formProps.editing).to.be.undefined;
  });
  describe('mutateFormValues', () => {
    let instance;
    beforeEach(() => {
      const wrapper = shallow(<SpaceModal {...commonProps}/>);
      instance = wrapper.instance();
    });
    it('should remove refreshPeriod, gracePeriod from formValues', () => {
      const formValues = {name: 'someName', accelerationRefreshPeriod: {}, accelerationGracePeriod: {}};
      instance.mutateFormValues(formValues);
      expect(formValues.accelerationRefreshPeriod).to.be.undefined;
      expect(formValues.accelerationGracePeriod).to.be.undefined;
    });
  });
});
