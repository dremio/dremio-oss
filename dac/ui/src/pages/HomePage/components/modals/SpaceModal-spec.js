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
import Immutable from 'immutable';

import SpaceForm from 'pages/HomePage/components/forms/SpaceForm';

import {SpaceModal} from './SpaceModal';

describe('SpaceModal', () => {

  const entity = Immutable.Map({name: 'bar', description: 'd'});
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
    const wrapper = shallow(<SpaceModal {...commonProps}/>);
    const formProps = wrapper.find(SpaceForm).props();
    expect(formProps.editing).to.be.undefined;
  });

  it('renders <SpaceForm> with initialValues when entity exists', () => {
    const wrapper = shallow(<SpaceModal {...commonProps} entity={entity}/>);
    const formProps = wrapper.find(SpaceForm).props();
    expect(formProps.initialValues.name).to.equal(entity.get('name'));
    expect(formProps.initialValues.description).to.equal(entity.get('description'));
    expect(formProps.editing).to.be.true;
  });

  describe('#submit', () => {
    it('should call mutateFormValues and updateSpace if entity', () => {
      const instance = shallow(<SpaceModal {...commonProps} entity={entity}/>).instance();
      sinon.spy(instance, 'mutateFormValues');
      instance.submit({name: 'foo'});
      expect(instance.mutateFormValues).to.be.called;
      expect(commonProps.updateSpace).to.be.calledOnce;
      expect(commonProps.createNewSpace).to.not.be.called;
    });

    it('should call createNewSpace if no entity', () => {
      const instance = shallow(<SpaceModal {...commonProps}/>).instance();
      instance.submit({name: 'foo'});
      expect(commonProps.updateSpace).to.not.be.called;
      expect(commonProps.createNewSpace).to.be.calledOnce;
    });
  });

});
