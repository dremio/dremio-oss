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

import { DevErrorView as DevError } from './DevError';

describe('DevError', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      onDismiss: sinon.spy()
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DevError {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render CustomRedBox only if props.error is set', () => {
    const wrapper = shallow(<DevError {...commonProps}/>);
    expect(wrapper.getElement()).to.be.null;

    wrapper.setProps({error: new Error()});
    expect(wrapper.getElement().type.displayName).to.eql('RedBox');
  });
});
