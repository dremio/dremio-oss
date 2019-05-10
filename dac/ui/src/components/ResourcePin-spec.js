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

import { ResourcePin } from './ResourcePin';

describe('ResourcePin-spec', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      entityId: 'test entity id',
      isPinned: false,
      toggleActivePin: sinon.spy()
    };
  });

  describe('render', () => {
    it('should render pin', () => {
      const wrapper = shallow(<ResourcePin {...commonProps}/>);
      expect(wrapper.hasClass('pin-wrap')).to.be.true;
    });

    it('should have active class if isPinned', () => {
      const wrapper = shallow(<ResourcePin {...commonProps} isPinned/>);
      expect(wrapper.find('.active')).to.have.length(1);
    });
  });

  describe('click', () => {
    it('should call toggleActivePin with name and active state', () => {
      const inactive = shallow(<ResourcePin {...commonProps} />);
      const event = {preventDefault: sinon.spy(), stopPropagation: sinon.spy};
      inactive.simulate('click', event);
      commonProps.toggleActivePin.calledWith(commonProps.name, false);
      expect(event.preventDefault.calledOnce).to.be.true;


      const active = shallow(<ResourcePin {...commonProps} isPinned/>);
      active.simulate('click', event);
      commonProps.toggleActivePin.calledWith(commonProps.name, true);
    });
  });
});
