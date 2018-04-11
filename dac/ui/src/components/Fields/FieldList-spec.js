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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { shallow, mount } from 'enzyme';

import FieldList, {RemoveButton} from './FieldList';

class Item extends Component {
  static propTypes = {
    style: PropTypes.object,
    item: PropTypes.object,
    onRemove: PropTypes.func
  }

  render() {
    const {style, item, onRemove} = this.props;
    return (
      <div style={style}>
        <span>{item.value}</span>&nbsp;
        {onRemove && <RemoveButton onClick={onRemove}/>}
      </div>
    );
  }
}

describe('<FieldList/>', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      items: [{value: 'foo'}, {value: 'bar'}],
      itemHeight: 30,
      getKey: item => item.value
    };
  });

  it('should render <div> with style', () => {
    const style = {color: 'black'};
    const wrapper = shallow(<FieldList {...commonProps} style={style}><Item/></FieldList>);
    expect(wrapper.type()).to.eql('div');
    expect(wrapper.props().style).to.eql(style);
  });


  describe('rendering children', () => {
    it('should render one child per item', () => {
      const wrapper = mount(<FieldList {...commonProps}><Item/></FieldList>);
      expect(wrapper.find(Item)).to.have.length(2);
      const firstProps = wrapper.find(Item).first().props();
      expect(firstProps.item).to.eql({value:'foo'});
      expect(firstProps.onRemove).not.to.be.undefined;
    });

    it('should not pass children onRemove when <= minItems', () => {
      const wrapper = mount(<FieldList {...commonProps} minItems={2}><Item/></FieldList>);
      const firstProps = wrapper.find(Item).first().props();
      expect(firstProps.onRemove).to.be.undefined;
    });
  });

  it('should render with optional listContainer', () => {
    const wrapper = mount(<FieldList {...commonProps} listContainer={<foo />}><Item/></FieldList>);
    const fooEle = wrapper.find('foo').first();
    expect(fooEle).to.not.be.undefined;
    expect(fooEle.children().length).to.eql(2);
  });
});
