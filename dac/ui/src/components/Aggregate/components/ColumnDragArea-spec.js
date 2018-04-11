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

import ColumnDragItem from 'utils/ColumnDragItem';
import ColumnDragArea from './ColumnDragArea';

describe('ColumnDragArea', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    const columnsField = [];
    columnsField.addField = sinon.spy();
    columnsField.removeField = sinon.spy();

    minimalProps = {
      columnsField
    };
    commonProps = {
      ...minimalProps,
      isDragInProgress: true,
      dragOrigin: 'dimensions',
      dragItem: new ColumnDragItem('id', 'measures')
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ColumnDragArea {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render ExploreDragArea with isDragged true only if type and draItem.type is different,' +
    ' isDragInProgress is true, canSelectColumn returns true', () => {
    const wrapper = shallow(<ColumnDragArea {...commonProps}/>);
    const instance = wrapper.instance();
    const canSelectColumnStub = sinon.stub(instance, 'canSelectColumn').returns(true);
    let exploreDragArea = wrapper.find('ExploreDragArea');

    expect(exploreDragArea).to.have.length(1);
    expect(exploreDragArea.prop('isDragged')).to.be.true;

    canSelectColumnStub.returns(false);
    //setting same props to force a re-render
    wrapper.setProps({
      dragOrigin: 'dimensions'
    });
    exploreDragArea = wrapper.find('ExploreDragArea');
    expect(exploreDragArea.prop('isDragged')).to.be.false;
    expect(canSelectColumnStub).to.be.calledWith(commonProps.dragItem.id);

    wrapper.setProps({
      dragOrigin: 'measures'
    });
    exploreDragArea = wrapper.find('ExploreDragArea');
    expect(exploreDragArea.prop('isDragged')).to.be.false;

    wrapper.setProps({
      isDragInProgress: false
    });
    exploreDragArea = wrapper.find('ExploreDragArea');
    expect(exploreDragArea.prop('isDragged')).to.be.false;
  });

  describe('#canSelectColumn', function() {
    let wrapper;
    let instance;
    let props;
    beforeEach(() => {
      props = {
        ...commonProps,
        dragType: 'measures',
        allColumns: Immutable.fromJS([
          { name: 'col1', disabled: true},
          { name: 'col2', disabled: true},
          { name: 'col3', disabled: false}
        ]),
        columnsField: [{column: {value: 'col1'}}]
      };
      wrapper = shallow(<ColumnDragArea {...props}/>);
      instance = wrapper.instance();
    });

    it('should return true when dragItem.dragOrigin and dragOrigin are different and column is not selected', () => {
      expect(instance.canSelectColumn('col2')).to.be.true;
    });

    it('should return false when dragItem.dragOrigin and dragOrigin are different but column already selected', () => {
      expect(instance.canSelectColumn('col1')).to.be.false;
    });

    it('should return false when canUseFieldAsBothDimensionAndMeasure is false and column is disabled', () => {
      wrapper.setProps({
        canUseFieldAsBothDimensionAndMeasure: false
      });
      expect(instance.canSelectColumn('col2')).to.be.false;
    });

    it('should return true when canUseFieldAsBothDimensionAndMeasure is false and column is enabled', () => {
      wrapper.setProps({
        canUseFieldAsBothDimensionAndMeasure: false
      });
      expect(instance.canSelectColumn('col3')).to.be.true;
    });
  });
});
