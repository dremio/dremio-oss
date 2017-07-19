/*
 * Copyright (C) 2017 Dremio Corporation
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

import { Elastic } from './Elastic.js';

describe('Elastic', () => {

  let minimalProps;
  let commonProps;
  let onFormSubmitReturn;
  beforeEach(() => {
    minimalProps = {
      onFormSubmit: sinon.stub().returns((onFormSubmitReturn = {})),
      onCancel: sinon.stub(),
      handleSubmit: sinon.stub().returns(() => {}),
      fields: { config: {} }
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<Elastic {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#onSubmit', () => {
    it('should mapFormatValues', () => {
      const instance = shallow(<Elastic {...commonProps}/>).instance();
      const output = {};
      const input = {};
      sinon.stub(instance, 'mapFormatValues').returns(output);
      expect(instance.onSubmit(input)).to.equal(onFormSubmitReturn);
      expect(instance.mapFormatValues).to.have.been.calledWith(input);
      expect(minimalProps.onFormSubmit).to.have.been.calledWith(output);
    });
  });

  describe('#mapFormatValues', () => {
    it('should convert timeouts into milliseconds for server', () => {
      const instance = shallow(<Elastic {...commonProps}/>).instance();
      const unchangedValues = {
        config: {
          username: 'username',
          password: 'password',
          scriptsEnabled: true,
          showHiddenIndices: false,
          sslEnabled: false,
          showIdColumn : false
        }
      };
      expect(instance.mapFormatValues({
        ...unchangedValues,
        scrollTimeoutSeconds : 300,
        readTimeoutSeconds : 60
      })).to.eql( {
        ...unchangedValues,
        scrollTimeoutSeconds : 300,
        readTimeoutSeconds : 60
      });
    });
  });
});
