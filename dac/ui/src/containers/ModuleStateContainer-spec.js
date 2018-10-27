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

import { shallow, mount } from 'enzyme';
import { ModuleStateView } from './ModuleStateContainer';

const childId = 'child_id';
const initState = sinon.spy();
const resetState = sinon.spy();
const commonProps = {
  moduleKey: 'module_key',
  isStateInitialized: true,
  reducer: () => {},
  initState,
  resetState,
  children: <div id={childId}></div>
};

describe('ModuleStateView', () => {
  beforeEach(() => {
    initState.reset();
    resetState.reset();
  });

  it('init state is called on mount', () => {
    mount(<ModuleStateView {...commonProps} />);

    expect(initState).have.been.calledWith(commonProps.moduleKey, commonProps.reducer);
  });

  it('state is reset on unmount', () => {
    const wrapper = mount(<ModuleStateView {...commonProps} />);

    wrapper.unmount();
    expect(resetState).have.been.calledWith(commonProps.moduleKey);
  });

  it('renders children if isStateInitialized = true', () => {
    const wrapper = shallow(<ModuleStateView {...commonProps} />);

    expect(wrapper.find(`#${childId}`)).have.length(1);
  });

  it('renders children if isStateInitialized = false', () => {
    const wrapper = shallow(<ModuleStateView {...commonProps} isStateInitialized={false} />);
    expect(wrapper.find(`#${childId}`)).have.length(0);
  });

  it('init state is called on module key change', () => {
    const wrapper = mount(<ModuleStateView {...commonProps} />);
    const newKey = 'other_module_key';

    wrapper.setProps({
      ...commonProps,
      moduleKey: newKey
    });

    expect(resetState.lastCall).have.been.calledWith(commonProps.moduleKey);
    expect(initState.lastCall).have.been.calledWith(newKey, commonProps.reducer);
  });

  it('init state is called on reducer change', () => {
    const wrapper = mount(<ModuleStateView {...commonProps} />);
    const newReducer = () => {};

    wrapper.setProps({
      ...commonProps,
      reducer: newReducer
    });

    expect(resetState.lastCall).have.been.calledWith(commonProps.moduleKey);
    expect(initState.lastCall).have.been.calledWith(commonProps.moduleKey, newReducer);
  });
});
