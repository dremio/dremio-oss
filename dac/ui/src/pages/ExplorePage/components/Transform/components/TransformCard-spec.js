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

import TransformCard from './TransformCard';

describe('TransformCard', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
    };
    commonProps = {
      ...minimalProps,
      front: <section className='front'/>,
      back: <section className='back'/>,
      card: Immutable.Map(),
      active: true,
      onClick: sinon.spy()
    };
    wrapper = shallow(<TransformCard {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<TransformCard {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should set state.editing to !hasExamples', () => {
    expect(wrapper.state().editing).to.be.true;

    const cardWithExamples = Immutable.fromJS({
      examplesList: ['example']
    });
    wrapper = shallow(<TransformCard {...commonProps} card={cardWithExamples} />);
    expect(wrapper.state().editing).to.be.false;
  });

  it('should render CardFooter', () => {
    expect(wrapper.find('CardFooter')).to.have.length(1);
  });

  it('should render front or back backed on state.editing', () => {
    wrapper.setState({editing: false});
    expect(wrapper.find('section').first().props().className).to.eql('front');

    wrapper.setState({editing: true});
    expect(wrapper.find('section').first().props().className).to.eql('back');
  });

  it('should render edit button only if front exists', () => {
    expect(wrapper.find('FontIcon')).to.have.length(1);
    wrapper.setProps({front: undefined});
    expect(wrapper.find('FontIcon')).to.have.length(0);
  });

  describe('#componentWillReceiveProps', () => {
    it('should set state.editing=false only if !active', () => {
      wrapper.setState({editing: true});
      instance.componentWillReceiveProps(commonProps);
      expect(wrapper.state().editing).to.be.true;

      wrapper.setProps({active: false});
      expect(wrapper.state().editing).to.be.false;
    });
  });
});
