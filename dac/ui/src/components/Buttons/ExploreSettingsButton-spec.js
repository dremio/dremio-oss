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

import ExploreSettingsButton from './ExploreSettingsButton';

describe('ExploreSettingsButton', () => {

  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      loadAccelerationData: sinon.spy(),
      getDatasetAccelerationRequest: sinon.stub().returns(Promise.resolve()),
      disabled: false,
      dataset: Immutable.fromJS({
        apiLinks: { namespaceEntity: '/dataset/entity'},
        datasetType: 'datasetUI'
      }),
      side: 'left'
    };
    context = {
      router: { push: sinon.spy()}
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ExploreSettingsButton {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<ExploreSettingsButton {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render SimpleButton with commonProps', () => {
    const wrapper = shallow(<ExploreSettingsButton {...commonProps}/>);
    expect(wrapper.find('SimpleButton')).to.have.length(1);
  });

  it('should render Overlay with commonProps', () => {
    const wrapper = shallow(<ExploreSettingsButton {...commonProps}/>);
    expect(wrapper.find('Tooltip')).to.have.length(1);
  });

  describe('#handleMouseEnter', () => {
    it('should not open overlay when button is enabled', () => {
      const instance = shallow(<ExploreSettingsButton {...commonProps}/>).instance();
      instance.handleMouseEnter();
      expect(instance.state.isOpenOverlay).to.be.false;
    });

    it('should open overlay when button is disabled', () => {
      const props = {...commonProps, disabled: true};
      const instance = shallow(<ExploreSettingsButton {...props}/>).instance();
      instance.handleMouseEnter();
      expect(instance.state.isOpenOverlay).to.be.true;
    });
  });

  describe('#handleMouseLeave', () => {
    it('should hide overlay', () => {
      const instance = shallow(<ExploreSettingsButton {...commonProps}/>).instance();
      instance.setState({
        isOpenOverlay: true
      });
      instance.handleMouseLeave();
      expect(instance.state.isOpenOverlay).to.be.false;
    });
  });

  describe('#handleOnClick', () => {
    it('should open dataset settings modal for current dataset with appropriate configuration', () => {
      shallow(<ExploreSettingsButton {...commonProps}/>, {context}).instance().handleOnClick();
      expect(context.router.push).to.be.calledWith({
        state: {
          datasetUrl: '/dataset/entity',
          datasetType: 'datasetUI',
          query: { then: 'query' },
          modal: 'DatasetSettingsModal'
        }
      });
    });
    it('should not open dataset settings modal when disabled', () => {
      shallow(<ExploreSettingsButton {...commonProps} disabled/>, {context}).instance().handleOnClick();
      expect(context.router.push).to.be.not.called;
    });
  });
});
