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
import { mount } from 'enzyme';
import PropTypes from 'prop-types';
import { createContext } from './ReactContext';

describe('ReactContext', () => {
  const defaultValue = 1;

  let renderer;
  let Provider;
  let Consumer;

  beforeEach(() => {
    renderer = sinon.stub().returns(<div></div>);
    const context = createContext(defaultValue, PropTypes.number);
    Provider = context.Provider;
    Consumer = context.Consumer;
  });

  it('A default value is passed to a consumer', () => {
    mount(<Provider>
      <Consumer>
        {renderer}
      </Consumer>
    </Provider>);
    expect(renderer).to.be.calledWith(defaultValue);
  });

  it('Provider overwrites a default value', () => {
    const value = 2;
    mount(<Provider value={value}>
      <Consumer>
        {renderer}
      </Consumer>
    </Provider>);
    expect(renderer).to.be.calledWith(value); // renderer should be called with value, that is provided to Provider
  });

  it('Updated value is passed to a Consumer', () => {
    const wrapper = mount(<Provider value={2}>
      <Consumer>
        {renderer}
      </Consumer>
    </Provider>);

    renderer.resetHistory();
    const value = 3;
    wrapper.setProps({ value }); // update a value
    expect(renderer.lastCall).to.be.calledWith(value); // renderer should be called with updated value
  });

  it('Value is passed from bottom most provider', () => {
    mount(<Provider value={2}>
      <Provider value={5}>
        <Consumer>
          {renderer}
        </Consumer>
      </Provider>
    </Provider>);
    expect(renderer).to.be.calledWith(5); // should be called with a value, that is passed to bottom most Provider
  });
});
