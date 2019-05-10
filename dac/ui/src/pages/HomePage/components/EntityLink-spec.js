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
import { EntityLinkProviderView } from './EntityLink';

/**
 * Gets a content of EntityLinkProvider component form shallow wrapper
 *
 * @param {shallowWrapper} wrapper
 * @param {string} linkUrlToInject - a link that would be injected as render props function argument
 */
export const getRenderEntityLinkContent = (wrapper, linkUrlToInject = 'test/url') => (
  wrapper.prop('children')(linkUrlToInject)
);

it('EntityLinkProvider renders a result of render props function', () => {
  const urlToInject = '/space/test_space';
  const renderProps = sinon.stub().returns(<div className='test-selector'>test</div>);
  const wrapper = shallow(<EntityLinkProviderView linkTo={urlToInject}>
    {renderProps}
  </EntityLinkProviderView>);

  expect(renderProps).to.be.calledWith(urlToInject);
  expect(wrapper.find('.test-selector')).to.have.length(1, 'result which is returned by children function should be rendered');
});
