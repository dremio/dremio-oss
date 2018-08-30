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
import AccelerationGridController from './AccelerationGridController';

function setReduxFormHandlers(list) {
  list.forEach((layout) => {
    const keys = Object.keys(layout);
    keys.forEach((key) => {
      layout[key].addField = sinon.spy();
    });
  });
}

describe('AccelerationGridController', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      dataset: Immutable.fromJS({
        id: '1',
        path: ['path', 'name'],
        fields: [
          {name: 'columnA', type: 'TEXT'},
          {name: 'columnB', type: 'TEXT'}
        ]
      }),
      reflections: Immutable.fromJS({}),
      layoutFields: []
    };
    commonProps = {
      ...minimalProps,
      layoutFields: [
        {
          dimensionFields: [{ name: { value: 'columnB' } }],
          measureFields: [{ name: { value: 'columnA' } }],
          partitionFields: [{ name: {value: 'columnB'} }, { name: { value: 'columnA' } }],
          sortFields: []
        },
        {
          dimensionFields: [],
          measureFields: [{ name: { value: 'columnA' } }],
          partitionFields: [{ name: { value: 'columnA' } }, { name: { value: 'columnB' } }],
          sortFields: [
            { field: { name: { value: 'columnA' } } },
            { field: { name: { value: 'columnB' } } }
          ]
        }
      ]
    };
    setReduxFormHandlers(commonProps.layoutFields);
    wrapper = shallow(<AccelerationGridController {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    const wrap = shallow(<AccelerationGridController {...minimalProps}/>);
    expect(wrap).to.have.length(1);
  });

  describe('findCurrentIndexInFieldsList', () => {
    it('should return current index of column in field list', () => {
      const currentColumn = {name: 'columnA'};
      instance.setState({ currentCell: {columnIndex: 0} });
      expect(
        instance.findCurrentIndexInFieldsList(currentColumn, 'partitionFields')
      ).to.equal(1);
    });

    it('should return -1 if we have not current column in field list', () => {
      const currentColumn = {name: 'columnA'};
      instance.setState({ currentCell: {columnIndex: 0} });
      expect(
        instance.findCurrentIndexInFieldsList(currentColumn, 'sortFields')
      ).to.equal(-1);
    });

    it('should get index of column from func parametrs if we have it', () => {
      const currentColumn = {name: 'columnA'};
      wrapper.setProps({
        layoutFields: [
          {
            dimensionFields: [{name: {value: 'columnB'}}],
            measureFields: [{name: {value: 'columnA'}}],
            partitionFields: [{name: {value: 'columnB'}}, {name: {value: 'columnA'}}],
            sortFields: []
          },
          {
            dimensionFields: [],
            measureFields: [{name: {value: 'columnA'}}],
            partitionFields: [{name: {value: 'columnA'}}, {name: {value: 'columnB'}}],
            sortFields: []
          }
        ]
      });
      instance.setState({ currentCell: {columnIndex: 0} });
      expect(
        instance.findCurrentIndexInFieldsList(currentColumn, 'partitionFields', 1)
      ).to.eql(0);
    });
  });

  describe('findCurrentColumnInLayouts', () => {
    it('should return current column in field list', () => {
      expect(instance.findCurrentColumnInLayouts('partitionFields', 1, 0)).to.eql({
        name: {value: 'columnB'}
      });
    });

    it('should return undefined if field have not current column', () => {
      expect(instance.findCurrentColumnInLayouts('dimensionFields', 0, 1)).to.eql(undefined);
    });
  });

  describe('findCurrentCellAllMeasureTypes', () => {
    it('should return null for raw page', () => {
      wrapper.setProps({activeTab: 'raw'});
      expect(instance.findCurrentCellAllMeasureTypes({})).to.be.null;
    });
  });

  describe('getRowByIndex', () => {
    it('should return row by index', () => {
      sinon.stub(instance, 'filterFieldList').returns(Immutable.List([{name: 'foo'}]));
      expect(instance.getRowByIndex(0)).to.be.eql({name: 'foo'});
    });
  });

  describe('removeFieldByIndex', () => {
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            measureFields: { removeField: sinon.spy() }
          }
        ]
      });
    });

    it('should remove current field from measureFields', () => {
      sinon.stub(instance, 'findCurrentIndexInFieldsList').returns(1);
      instance.removeFieldByIndex(0, 'measureFields', 0);
      expect(instance.findCurrentIndexInFieldsList).to.have.been.called;
      expect(instance.props.layoutFields[0].measureFields.removeField).to.have.been.calledWith(1);
    });

    it('should not remove current field if we have not this field in list of fields', () => {
      sinon.stub(instance, 'findCurrentIndexInFieldsList').returns(-1);
      instance.removeFieldByIndex(0, 'measureFields', 0);
      expect(instance.props.layoutFields[0].measureFields.removeField.called).to.be.false;
    });
  });

  describe('addFieldByIndex', () => {
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            measureFields: { addField: sinon.spy() }
          }
        ]
      });
    });

    it('should add current field to measureFields', () => {
      sinon.stub(instance, 'findCurrentIndexInFieldsList').returns(-1);
      instance.addFieldByIndex({name: 'col_name'}, 'measureFields', 0);
      expect(instance.findCurrentIndexInFieldsList).to.have.been.called;
      expect(
        instance.props.layoutFields[0].measureFields.addField
      ).to.have.been.calledWith({name: 'col_name'});
    });

    it('should add current field if we have not this field in list of fields', () => {
      sinon.stub(instance, 'findCurrentIndexInFieldsList').returns(0);
      instance.addFieldByIndex(0, 'measureFields', 0);
      expect(instance.props.layoutFields[0].measureFields.addField).to.have.not.been.called;
    });
  });

  describe('toggleField', () => {
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            measureFields: { addField: sinon.spy() }
          }
        ]
      });
      sinon.stub(instance, 'getRowByIndex').returns({name: 'columnA'});
      sinon.stub(instance, 'removeFieldByIndex');
    });

    it('should remove current field from measureFields', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns({});
      instance.toggleField('measureFields', 0, 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith({name: 'columnA'}, 'measureFields', 0);
    });

    it('should add current field to measureFields', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns(undefined);
      instance.toggleField('measureFields', 0, 0);
      expect(instance.props.layoutFields[0].measureFields.addField).to.be.calledWith({name: 'columnA'});
    });
  });

  describe('toggleSortField', () => {
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            sortFields: { addField: sinon.spy() }
          }
        ]
      });
      instance.setState({
        currentCell: {
          field: 'sortFields'
        }
      });
      sinon.stub(instance, 'getRowByIndex').returns({name: 'columnA'});
      sinon.stub(instance, 'removeFieldByIndex');
    });

    it('should remove current field from sortFields', () => {
      instance.toggleSortField('', 0, 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith({name: 'columnA'}, 'sortFields', 0);
    });

    it('should add current field to sortFields', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns(undefined);
      instance.toggleSortField('sortFields', 0, 0);
      expect(instance.props.layoutFields[0].sortFields.addField).to.be.calledWith({name: 'columnA'});
    });

    it('should not add current field to sortFields when already added', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns({});
      instance.toggleSortField('sortFields', 0, 0);
      expect(instance.props.layoutFields[0].sortFields.addField).to.not.be.called;
    });
  });

  describe('applyConstraints', () => {
    beforeEach(() => {
      sinon.stub(instance, 'applyAggregationConstraints');
      sinon.stub(instance, 'applyRawConstraints');
    });

    it('should call applyRawConstraints when activeTab is raw', () => {
      wrapper.setProps({activeTab: 'raw'});
      instance.applyConstraints('measureFields', 0, 0);
      expect(
        instance.applyRawConstraints
      ).to.have.been.calledWith('measureFields', 0, 0);
    });

    it('should call applyAggregationConstraints when activeTab is not raw', () => {
      wrapper.setProps({activeTab: ''});
      instance.applyConstraints('measureFields', 0, 0);
      expect(
        instance.applyAggregationConstraints
      ).to.have.been.calledWith('measureFields', 0, 0);
    });
  });

  describe('handleOnSortCheckboxItem', () => {
    beforeEach(() => {
      instance.setState({
        currentCell: {
          columnIndex: 0,
          rowIndex: 0,
          labelCell: 'sort'
        }
      });
      sinon.stub(instance, 'toggleSortField');
      sinon.stub(instance, 'applyConstraintsNext');
      sinon.stub(instance, 'handleRequestClose');
    });

    it('should call toggleSortField', () => {
      instance.handleOnSortCheckboxItem(0, 0);
      expect(instance.toggleSortField).to.have.been.calledWith('sortFields', 0, 0);
    });
    it('should call applyConstraintsNext', () => {
      instance.handleOnSortCheckboxItem(0, 0);
      expect(instance.applyConstraintsNext).to.have.been.calledWith('sortFields', 0, 0);
    });
    it('should call handleRequestClose', () => {
      instance.handleOnSortCheckboxItem(0, 0);
      expect(instance.handleRequestClose).to.have.been.calledOnce;
    });
  });

  describe('handleOnCheckboxItem', () => {
    beforeEach(() => {
      sinon.stub(instance, 'toggleField');
      sinon.stub(instance, 'applyConstraintsNext');
    });

    it('should call toggleField', () => {
      instance.handleOnCheckboxItem('dimensionFields', 0, 1);
      expect(instance.toggleField).to.have.been.calledWith('dimensionFields', 0, 1);
    });
    it('should call applyConstraintsNext', () => {
      instance.handleOnCheckboxItem('dimensionFields', 0, 1);
      expect(instance.applyConstraintsNext).to.have.been.calledWith('dimensionFields', 0, 1);
    });
  });


  describe('applyAggregationConstraints', () => {
    let currentRow;
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            dimensionFields: { addField: sinon.spy() }
          }
        ]
      });
      currentRow = { name: 'columnA' };
      sinon.stub(instance, 'getRowByIndex').returns(currentRow);
      sinon.stub(instance, 'removeFieldByIndex');
      sinon.stub(instance, 'addFieldByIndex');
    });

    it('should remove sortFields, partitionFields and distributionFields when dimensionFields and measureFields unselected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns(undefined);
      instance.applyAggregationConstraints('dimensionFields', 0, 0);

      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'sortFields', 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'partitionFields', 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'distributionFields', 0);
    });

    it('should remove sortFields when dimensionFields unselected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts')
        .withArgs('dimensionFields', 0, 0).returns(undefined)
        .withArgs('measureFields', 0, 0).returns({});
      instance.applyAggregationConstraints('dimensionFields', 0, 0);

      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'sortFields', 0);
    });

    it('should leave measureFields and add dimensionFields when sortFields selected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts')
        .withArgs('dimensionFields', 0, 0).returns(undefined)
        .withArgs('measureFields', 0, 0).returns({})
        .withArgs('sortFields', 0, 0).returns({});
      instance.applyAggregationConstraints('sortFields', 0, 0);

      expect(
        instance.removeFieldByIndex
      ).to.not.have.been.calledWith(currentRow, 'measureFields', 0);
      expect(
        instance.addFieldByIndex
      ).to.have.been.calledWith(currentRow, 'dimensionFields', 0);
    });

    it('should add dimensionFields when distributionFields or partitionFields or sortFields selected (leaving measureFields)', () => {
      const findCurrentColumnStub = sinon.stub(instance, 'findCurrentColumnInLayouts')
        .withArgs('partitionFields', 0, 0).returns({});
      const resetFieldsStub = () => {
        instance.addFieldByIndex.reset();
        instance.removeFieldByIndex.reset();
      };
      const verify = () => {
        expect(
          instance.addFieldByIndex
        ).to.have.been.calledWith(currentRow, 'dimensionFields', 0);
        expect(
          instance.removeFieldByIndex
        ).to.not.have.been.calledWith(currentRow, 'measureFields', 0);
      };

      instance.applyAggregationConstraints('partitionFields', 0, 0);
      verify();

      findCurrentColumnStub.withArgs('partitionFields', 0, 0).returns(undefined)
        .withArgs('distributionFields', 0, 0).returns({});
      resetFieldsStub();
      instance.applyAggregationConstraints('distributionFields', 0, 0);
      verify();

      findCurrentColumnStub.withArgs('distributionFields', 0, 0).returns(undefined)
        .withArgs('sortFields', 0, 0).returns({});
      resetFieldsStub();
      instance.applyAggregationConstraints('sortFields', 0, 0);
      verify();
    });

    it('should leave dimensionFields and sortFields when measureFields is selected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts')
        .withArgs('dimensionFields', 0, 0).returns({})
        .withArgs('measureFields', 0, 0).returns({});
      instance.applyAggregationConstraints('measureFields', 0, 0);
      expect(
        instance.removeFieldByIndex
      ).to.not.have.been.calledWith(currentRow, 'dimensionFields', 0);
      expect(
        instance.removeFieldByIndex
      ).to.not.have.been.calledWith(currentRow, 'sortFields', 0);
    });

    it('should leave measureFields when dimensionFields is selected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts')
        .withArgs('dimensionFields', 0, 0).returns({})
        .withArgs('measureFields', 0, 0).returns({});
      instance.applyAggregationConstraints('dimensionFields', 0, 0);
      expect(
        instance.removeFieldByIndex
      ).to.not.have.been.calledWith(currentRow, 'measureFields', 0);
    });
  });

  describe('applyRawConstraints', () => {
    let currentRow;
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            displayFields: { addField: sinon.spy() }
          }
        ]
      });
      currentRow = { name: 'columnA' };
      sinon.stub(instance, 'getRowByIndex').returns(currentRow);
      sinon.stub(instance, 'removeFieldByIndex');
    });

    it('should remove sortFields, partitionFields and distributionFields ' +
      'when displayFields unselected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns(undefined);
      instance.applyRawConstraints('displayFields', 0, 0);

      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'sortFields', 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'partitionFields', 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'distributionFields', 0);
    });

    it('should add displayFields when sortFields, partitionFields or distributionFields selected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns(undefined);
      instance.applyRawConstraints('sortFields', 0, 0);

      expect(
        instance.props.layoutFields[0].displayFields.addField
      ).to.have.been.calledWith(currentRow);
    });
  });
});
