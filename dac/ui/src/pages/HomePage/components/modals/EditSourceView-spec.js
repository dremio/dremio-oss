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

import { EditSourceView } from './EditSourceView';

describe('EditSourceView', () => {
  let minimalProps;

  const contextTypes = {
    location: {},
    router: {}
  };

  beforeEach(() => {
    minimalProps = {
      sourceName: 'name',
      sourceType: 'type',
      hide: sinon.spy(),
      createSource: sinon.spy(),
      removeSource: sinon.spy(),
      source: new Immutable.Map({accessControlList: new Immutable.Map()}),
      loadSource: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<EditSourceView {...minimalProps} />, {context: contextTypes});
    expect(wrapper).to.have.length(1);
  });

  it('should render with minimal props without exploding', (done) => {
    const wrapper = shallow(<EditSourceView {...minimalProps} />, {context: contextTypes});
    const instance = wrapper.instance();

    instance.checkIsMetadataImpacting = sinon.stub().returns(Promise.resolve());
    instance.reallySubmitEdit = () => {
      return Promise.resolve({});
    };

    instance.submitEdit({config: {}}).then(() => {
      expect(instance.checkIsMetadataImpacting).to.have.been.called;
      done();
    });

  });
});
