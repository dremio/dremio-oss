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
import InternalSupportEmail from './InternalSupportEmail';
describe('InternalSupportEmail', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      renderSettings: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      descriptionStyle: {}
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<InternalSupportEmail {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should call renderSettings two times', () => {
    const wrapper = shallow(<InternalSupportEmail {...commonProps}/>);
    expect(wrapper).to.have.length(1);
    expect(commonProps.renderSettings).to.be.calledTwice;
    expect('support.email.addr').to.eql(commonProps.renderSettings.getCall(0).args[0]);
    expect('support.email.jobs.subject').to.eql(commonProps.renderSettings.getCall(1).args[0]);
  });
});
