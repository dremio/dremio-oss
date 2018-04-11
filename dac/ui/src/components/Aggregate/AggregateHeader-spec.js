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

import AggregateHeader from './AggregateHeader';

describe('AggregateHeader', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      dataset: Immutable.Map()
    };
    commonProps = {
      ...minimalProps,
      onClearAllDimensions: sinon.spy(),
      onClearAllMeasures: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AggregateHeader {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render columns without Clear All when onClearAllDimensions and onClearAllMeasures are not defined', () => {
    const wrapper = shallow(<AggregateHeader {...minimalProps}/>);
    expect(wrapper.children().at(1).find('SimpleButton')).to.have.length(0);
    expect(wrapper.children().at(2).find('SimpleButton')).to.have.length(0);
  });

  it('should render columns with Clear All option when onClearAllDimensions and onClearAllMeasures are defined', () => {
    const wrapper = shallow(<AggregateHeader {...commonProps}/>);
    expect(wrapper.children().at(1).find('SimpleButton')).to.have.length(1);
    expect(wrapper.children().at(2).find('SimpleButton')).to.have.length(1);
  });

  describe('#renderClearAll', () => {
    it('should return "Clear All" element when passed clearFunction param is defined', () => {
      const wrapper = shallow(shallow(<AggregateHeader {...commonProps}/>).instance().renderClearAll(() => {}));
      expect(wrapper).to.have.length(1);
    });

    it('should return null when passed clearFunction is not defined', () => {
      expect(shallow(<AggregateHeader {...minimalProps}/>).instance().renderClearAll()).to.be.null;
    });

    it('should call passed clearFunction when Clear All clicked', () => {
      const clearFunction = sinon.spy();
      const instance = shallow(<AggregateHeader {...commonProps}/>).instance();
      const wrapper = shallow(instance.renderClearAll(clearFunction));
      wrapper.simulate('click');
      expect(clearFunction).to.be.called;
    });
  });
});
