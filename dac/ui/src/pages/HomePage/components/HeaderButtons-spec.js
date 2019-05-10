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
import { HeaderButtons } from './HeaderButtons';

describe('HeaderButtons-spec', () => {

  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    minimalProps = {
      toggleVisibility: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      rootEntityType: 'home'
    };
    context = {
      location: {}
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<HeaderButtons {...minimalProps}/>, {context});
    expect(wrapper).to.have.length(1);
  });

  it('should render Links', () => {
    const wrapper = shallow(<HeaderButtons {...commonProps}/>, {context});
    expect(wrapper.find('Link')).to.have.length(2);
  });

  describe('renderButton', () => {
    let instance;
    beforeEach(() => {
      instance = shallow(<HeaderButtons {...commonProps}/>, {context}).instance();
    });

    it('should render Link', () => {
      const result = instance.renderButton({});
      expect(result.type.displayName).to.eql('Link');
    });

    it('should render + if isAdd is set', () => {
      const result = shallow(instance.renderButton({
        isAdd: true
      }, 0));
      expect(result.find('Art').first().props().src).to.eql('SimpleAdd.svg');
    });

    it('should render upload button w/o plus', () => {
      const buttons = instance.getHomeButtons();
      const result = shallow(instance.renderButton(buttons[1], 1));
      expect(result.find('Art').first().props().src).to.not.eql('SimpleAdd.svg');
    });
  });
});
