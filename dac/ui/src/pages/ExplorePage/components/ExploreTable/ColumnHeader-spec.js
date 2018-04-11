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
import { Cell } from 'fixed-data-table-2';

import ColumnHeader from './ColumnHeader';

describe('ColumnHeader', () => {

  let minimalProps;
  let commonProps;
  let preconfirmPromise;
  beforeEach(() => {
    preconfirmPromise = Promise.resolve();
    minimalProps = {
      column: {
        name: 'someName',
        type: 'TEXT'
      },
      dragType: 'groupBy',
      updateColumnName: sinon.spy(),
      makeTransform: sinon.spy(),
      preconfirmTransform: sinon.stub().returns(preconfirmPromise)
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ColumnHeader {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render Cell', () => {
    const wrapper = shallow(<ColumnHeader {...commonProps}/>);
    expect(wrapper.find(Cell)).to.have.length(1);
  });

  describe('#handleFocus', () => {
    let instance;
    beforeEach(() => {
      instance = shallow(<ColumnHeader {...commonProps}/>).instance();
      instance.input = {
        focus: sinon.spy(),
        blur: sinon.spy()
      };
    });

    it('should do nothing when forceFocus is set', () => {
      instance.forceFocus = true;
      instance.handleFocus();
      expect(instance.input.focus).to.not.be.called;
      expect(instance.input.blur).to.not.be.called;
    });

    it('should blur input, and call preconfirmTransform', (done) => {
      const clock = sinon.useFakeTimers();
      instance.handleFocus();
      clock.tick(1);

      expect(instance.input.blur).to.be.called;
      expect(commonProps.preconfirmTransform).to.be.called;
      clock.restore();

      preconfirmPromise.then(() => {
        expect(instance.input.focus).to.be.called;
        done();
      });
    });
  });
});
