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

import ProdErrorModal from 'components/Modals/ProdErrorModal';
import { ProdErrorContainer, SHOW_GO_HOME_AFTER_PERIOD } from './ProdError';

describe('ProdError', () => {

  let minimalProps;
  let commonProps;
  let clock;
  let wrapper;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      error: new Error('some error')
    };

    clock = sinon.useFakeTimers();
    wrapper = shallow(<ProdErrorContainer {...commonProps}/>);
  });

  afterEach(() => {
    clock.restore();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<ProdErrorContainer {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render ProdErrorModal if error exists', () => {
    expect(wrapper.find(ProdErrorModal)).to.have.length(1);
    wrapper.setProps({error: undefined});
    expect(wrapper.find(ProdErrorModal)).to.have.length(0);
  });

  it('should set showGoHome only if error is soon after init', () => {
    wrapper.setProps({error: new Error('sdf')});
    expect(wrapper.find(ProdErrorModal).props().showGoHome).to.be.true;

    clock.tick(SHOW_GO_HOME_AFTER_PERIOD + 1);
    wrapper.setProps({error: new Error('different')});
    expect(wrapper.find(ProdErrorModal).props().showGoHome).to.be.false;
  });
});
