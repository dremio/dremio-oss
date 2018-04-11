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
import { shallow, render } from 'enzyme';

import ReplaceValues, { MIN_VALUES_TO_SHOW_SEARCH } from './ReplaceValues';

describe('ReplaceValues', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      fields: {}
    };
    commonProps = {
      ...minimalProps,
      fields: {
        replaceValues: {
          onChange: sinon.spy(),
          value: []
        }
      },
      valueOptions: Immutable.fromJS({
        values: [
          {
            value: 'foo'
          }, {
            value: 'bar'
          }
        ]
      })
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ReplaceValues {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#handleAllClick', () => {
    it('should call fields.replaceValues.onChange with all values', () => {
      const testForm = ['foo', 'bar'];
      const e = {
        preventDefault: sinon.spy()
      };

      const wrapper = shallow(<ReplaceValues {...commonProps}/>);
      wrapper.instance().handleAllClick(e);

      expect(commonProps.fields.replaceValues.onChange).to.have.been.calledWith(testForm);
    });
  });

  describe('#handleNoneClick', () => {
    it('should call fields.replaceValues.onChange with empty object', () => {
      const e = {
        preventDefault: sinon.spy()
      };

      const wrapper = shallow(<ReplaceValues {...commonProps}/>);
      wrapper.instance().handleNoneClick(e);

      expect(commonProps.fields.replaceValues.onChange).to.have.been.calledWith([]);
    });
  });

  describe('#renderSelectedValuesCount', () => {
    it('should render proper color and text if something is selected (using allClick, for example)', () => {
      const checkedProps = {
        ...commonProps,
        fields: {
          replaceValues: {
            value: ['foo', 'bar']
          }
        }
      };

      const wrapper = shallow(<ReplaceValues {...checkedProps}/>);
      const instance = wrapper.instance();

      expect(render(instance.renderSelectedValuesCount()).text()).to.contain('2 of 2 selected');
      expect(shallow(instance.renderSelectedValuesCount()).props().style.color).to.equal('#333');
    });
    it('should return if nothing to replace (redux bug)', () => {
      const brokenProps = {
        ...commonProps,
        fields: {
          replaceValues: {
            value: null
          }
        }
      };

      const wrapper = shallow(<ReplaceValues {...brokenProps}/>);
      const instance = wrapper.instance();

      expect(instance.renderSelectedValuesCount()).to.be.empty;
    });
    it('should throw an error if fields is not present', () => {
      const noFieldProps = {
        ...minimalProps,
        fields: null
      };
      expect(function() {
        shallow(<ReplaceValues {...noFieldProps}/>);
      }).to.throw(Error);
    });
  });

  describe('#filterValuesList', () => {
    it('should return all values if filter is empty string', () => {
      const wrapper = shallow(<ReplaceValues {...commonProps}/>);
      wrapper.setState({ filter: '' });
      const result = wrapper.instance().filterValuesList(commonProps.valueOptions.get('values'));

      expect(result).to.eql(commonProps.valueOptions.get('values'));
    });

    it('should return empty list if matches are not found', () => {
      const wrapper = shallow(<ReplaceValues {...commonProps}/>);
      wrapper.setState({ filter: 'baz' });
      const result = wrapper.instance().filterValuesList(commonProps.valueOptions.get('values'));

      expect(result).to.be.an.instanceof(Immutable.List);
      expect(result).to.have.size(0);
    });

    it('should return filtered values list when it has matches', () => {
      const nextProps = {
        ...commonProps,
        valueOptions: Immutable.fromJS({
          values: [
            { value: 'foo' },
            { value: 'bar' },
            { value: 'foo1' }
          ]
        })
      };
      const nextState = {
        filter: 'Foo '
      };

      const wrapper = shallow(<ReplaceValues {...nextProps}/>);
      wrapper.setState(nextState);
      const result = wrapper.instance().filterValuesList(nextProps.valueOptions.get('values'));

      expect(result).to.eql(
        Immutable.fromJS([{ value: 'foo' }, {value: 'foo1' }])
      );
    });
  });

  describe('#renderValuesList', () => {
    it('should render SelectFrequentValues when values list is not empty', () => {
      const wrapper = shallow(<ReplaceValues {...commonProps}/>);
      expect(wrapper.find('SelectFrequentValues')).to.have.length(1);
    });

    it('should render div with "Not found" text when values list is empty', () => {
      const wrapper = shallow(<ReplaceValues {...commonProps}/>);
      wrapper.setState({ filter: 'baz' });

      expect(wrapper.find('SelectFrequentValues')).to.have.length(0);
      expect(wrapper.contains('Not found')).to.eql(true);
    });
  });

  describe('#renderSearchField', () => {
    it('should render SearchField when values list length more than minimal showing value', () => {
      const nextProps = {
        ...commonProps,
        valueOptions: Immutable.fromJS({
          values: [
            { value: 'foo' },
            { value: 'bar' },
            { value: 'baz' },
            { value: 'qux' },
            { value: 'ose' },
            { value: 'rol' },
            { value: 'zed' }
          ]
        })
      };

      const wrapper = shallow(<ReplaceValues {...nextProps}/>);
      expect(nextProps.valueOptions.get('values').size > MIN_VALUES_TO_SHOW_SEARCH).to.eql(true);
      expect(wrapper.find('SearchField')).to.have.length(1);
    });

    it('should render SearchField when values list length more than minimal showing value', () => {
      const wrapper = shallow(<ReplaceValues {...commonProps}/>);
      expect(commonProps.valueOptions.get('values').size < MIN_VALUES_TO_SHOW_SEARCH).to.eql(true);
      expect(wrapper.find('SearchField')).to.have.length(0);
    });
  });
});
