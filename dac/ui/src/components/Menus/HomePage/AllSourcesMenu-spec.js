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
import { AllSourcesMenu } from './AllSourcesMenu';

describe('AllSourcesMenu', () => {
  let minimalProps;
  let commonProps;
  let contextTypes;
  beforeEach(() => {
    minimalProps = {
      source: Immutable.fromJS({
        links: {
          self: ''
        }
      }),
      removeSource: sinon.spy(),
      showConfirmationDialog: sinon.spy(),
      closeMenu: sinon.spy()
    };
    commonProps = {
      ...minimalProps
    };
    contextTypes = {
      location: {}
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AllSourcesMenu {...minimalProps}/>, {context: contextTypes});
    expect(wrapper).to.have.length(1);
  });

  describe('#handleRemoveSource', () => {
    it('should show confirmation dialog before removing', () => {
      const wrapper = shallow(<AllSourcesMenu {...commonProps} />, {context: contextTypes});
      const instance = wrapper.instance();
      instance.handleRemoveSource();
      expect(commonProps.showConfirmationDialog).to.be.called;
      expect(commonProps.removeSource).to.not.be.called;
    });

    it('should call remove source when confirmed', () => {
      const props = {
        ...commonProps,
        showConfirmationDialog: (opts) => opts.confirm()
      };
      const wrapper = shallow(<AllSourcesMenu {...props}/>, {context: contextTypes});
      const instance = wrapper.instance();
      instance.handleRemoveSource();
      expect(props.removeSource).to.be.called;
    });
  });
});
