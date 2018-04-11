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

import ProdErrorModal from './ProdErrorModal';

describe('ProdErrorModal', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  beforeEach(() => {
    minimalProps = {
      error: new Error('the error')
    };
    commonProps = {
      ...minimalProps
    };

    wrapper = shallow(<ProdErrorModal {...commonProps}/>);
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<ProdErrorModal {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render Modal', () => {
    expect(wrapper.find('Modal')).to.have.length(1);
  });

  it('should render reload', () => {
    expect(wrapper.find('SimpleButton').first().props().children).to.contain('Reload');
  });

  it('should render file a bug if config.shouldEnableBugFiling', () => {
    expect(wrapper.find('SimpleButton').length).to.equal(1);

    wrapper.setProps({showFileABug: true});
    expect(wrapper.find('SimpleButton').length).to.equal(2);
    expect(wrapper.find('SimpleButton').at(0).props().children).to.contain('File a Bug');
  });

  it('should render "Go Home" if props.showGoHome', () => {
    expect(wrapper.find('SimpleButton').length).to.equal(1);
    wrapper.setProps({showGoHome: true});
    expect(wrapper.find('SimpleButton').length).to.equal(2);
    expect(wrapper.find('SimpleButton').at(0).props().children).to.contain('Go Home');
  });
});
