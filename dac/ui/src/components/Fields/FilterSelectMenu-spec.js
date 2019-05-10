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
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import FilterSelectMenu from './FilterSelectMenu';

// this component is needed to test test a content rendering. As enzyme could work only with components
// not with standard component (span, div)
const TestRenderer = ({ children }) => <div>{children}</div>;
TestRenderer.propTypes = {
  children: PropTypes.any
};
const getSelectViewContent = wrapper => {
  return shallow(<TestRenderer>{wrapper.find('SelectView').prop('content')}</TestRenderer>);
};

describe('FilterSelectMenu', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      label: 'label',
      items: [
        {label: 'item3', id: 3},
        {label: 'item2', id: 2},
        {label: 'item1', id: 1}
      ],
      selectedValues: Immutable.List([2, 3]),
      onItemSelect: sinon.spy(),
      onItemUnselect: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<FilterSelectMenu {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render SelectView', () => {
    const wrapper = shallow(<FilterSelectMenu {...commonProps}/>);
    expect(wrapper.find('SelectView')).to.have.length(1);
  });

  it('should render label prop when nothing selected', () => {
    const wrapper = shallow(<FilterSelectMenu {...commonProps}/>);
    expect(getSelectViewContent(wrapper).find('span').first().text()).to.eql('');
    wrapper.setProps({selectedValues: Immutable.List()});
    expect(getSelectViewContent(wrapper).find('span').first().text()).to.eql(commonProps.label);
  });

  it('should render SearchField only when there are unselected items', () => {
    const wrapper = shallow(<FilterSelectMenu {...commonProps}/>);
    expect(wrapper.find('SearchField').prop('placeholder')).to.eql('Search');

    wrapper.setProps({selectedValues: Immutable.List([1, 2, 3])});
    expect(wrapper.find('SearchField')).to.have.length(0);
  });

  it('should render SearchField if noSearch is set', () => {
    const wrapper = shallow(<FilterSelectMenu {...commonProps} noSearch/>);
    expect(wrapper.find('SearchField')).to.have.length(0);
  });

  describe('getSelectedItems', () => {
    it('should return array of items in selectedValues', () => {
      const wrapper = shallow(<FilterSelectMenu {...commonProps}/>);
      expect(wrapper.instance().getSelectedItems()).to.eql([commonProps.items[0], commonProps.items[1]]);

      wrapper.setProps({selectedValues: Immutable.List([1])});
      expect(wrapper.instance().getSelectedItems()).to.eql([commonProps.items[2]]);
    });
  });

  describe('getUnselectedItems', () => {
    it('should return array of items not in selectedValues', () => {
      const wrapper = shallow(<FilterSelectMenu {...commonProps}/>);
      expect(wrapper.instance().getUnselectedItems()).to.eql([commonProps.items[2]]);

      wrapper.setProps({selectedValues: Immutable.List([1])});
      expect(wrapper.instance().getUnselectedItems()).to.eql([commonProps.items[0], commonProps.items[1]]);
    });

    it('should filter results based on pattern and case insensitive', () => {
      const wrapper = shallow(<FilterSelectMenu {...commonProps} selectedValues={Immutable.List()}/>);
      const instance = wrapper.instance();
      wrapper.setState({pattern: 'item2'});
      let items = instance.getUnselectedItems();
      expect(items).to.eql([commonProps.items[1]]);

      wrapper.setState({pattern: 'ITEM2'});
      items = instance.getUnselectedItems();
      expect(items).to.eql([commonProps.items[1]]);

      wrapper.setProps({
        items: [
          {label: 'ITEM1', id: 1},
          {label: 'ITEM2', id: 2}
        ]
      });
      wrapper.setState({pattern: 'item2'});
      items = instance.getUnselectedItems();
      expect(items).to.eql([{label: 'ITEM2', id: 2}]);
    });
  });

  describe('renderSelectedLabel', () => {
    it('should render values in label', () => {
      const wrapper = shallow(<FilterSelectMenu {...commonProps}/>);
      expect(getSelectViewContent(wrapper).find('.filter-select-label').props().text).to.eql('item3, item2');
    });

    it('should render All if none selected', () => {
      const wrapper = shallow(<FilterSelectMenu {...commonProps} selectedValues={Immutable.List()}/>);
      expect(getSelectViewContent(wrapper).find('.filter-select-label').props().text).to.eql(': {"0":{"id":"Common.All"}}');
    });
  });

  describe('events', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<FilterSelectMenu {...commonProps}/>);
      instance = wrapper.instance();
    });

    describe('handleItemChange', () => {
      it('should call onItemSelect if unchecked', () => {
        instance.handleItemChange(false, 'id');
        expect(commonProps.onItemSelect).to.have.been.calledWith('id');
        expect(commonProps.onItemUnselect).to.not.have.been.called;
      });

      it('should call onItemUnselect if checked', () => {
        instance.handleItemChange(true, 'id');
        expect(commonProps.onItemSelect).to.not.have.been.called;
        expect(commonProps.onItemUnselect).to.have.been.calledWith('id');
      });
    });
  });
});
