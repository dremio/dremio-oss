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
import Immutable from 'immutable';
import AccelerationGridController from './AccelerationGridController';

function setReduxFormHandlers(list) {
  list.forEach((layout) => {
    const keys = Object.keys(layout.details);
    keys.forEach((key) => {
      layout.details[key].addField = sinon.spy();
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
      acceleration: Immutable.fromJS({
        context: {
          datasetSchema: {
            fieldList: []
          }
        }
      }),
      layoutFields: []
    };
    commonProps = {
      ...minimalProps,
      acceleration: Immutable.fromJS({
        context: {
          datasetSchema: {
            fieldList: [{name: 'columnA'}, {name: 'columnB'}]
          }
        }
      }),
      layoutFields: [
        {
          details: {
            dimensionFieldList: [{ name: { value: 'columnB' } }],
            measureFieldList: [{ name: { value: 'columnA' } }],
            partitionFieldList: [{ name: {value: 'columnB'} }, { name: { value: 'columnA' } }],
            sortFieldList: []
          }
        },
        {
          details: {
            dimensionFieldList: [],
            measureFieldList: [{ name: { value: 'columnA' } }],
            partitionFieldList: [{ name: { value: 'columnA' } }, { name: { value: 'columnB' } }],
            sortFieldList: [
              { field: { name: { value: 'columnA' } } },
              { field: { name: { value: 'columnB' } } }
            ]
          }
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

  describe('#renderDistributionCell', () => {
    it('should return undefined if !this.shouldShowDistribution()', () => {
      sinon.stub(instance, 'shouldShowDistribution').returns(false);
      expect(instance.renderDistributionCell()).to.be.undefined;

      instance.shouldShowDistribution.returns(true);
      expect(instance.renderDistributionCell()).to.not.be.undefined;
    });
  });

  describe('findCurrentIndexInFieldsList', () => {
    it('should return current index of column in field list', () => {
      const currentColumn = {name: 'columnA'};
      instance.setState({ currentCell: {columnIndex: 0} });
      expect(
        instance.findCurrentIndexInFieldsList(currentColumn, 'partitionFieldList')
      ).to.equal(1);
    });

    it('should return -1 if we have not current column in field list', () => {
      const currentColumn = {name: 'columnA'};
      instance.setState({ currentCell: {columnIndex: 0} });
      expect(
        instance.findCurrentIndexInFieldsList(currentColumn, 'sortFieldList')
      ).to.equal(-1);
    });

    it('should get index of column from func parametrs if we have it', () => {
      const currentColumn = {name: 'columnA'};
      wrapper.setProps({
        layoutFields: [
          {
            details: {
              dimensionFieldList: [{name: {value: 'columnB'}}],
              measureFieldList: [{name: {value: 'columnA'}}],
              partitionFieldList: [{name: {value: 'columnB'}}, {name: {value: 'columnA'}}],
              sortFieldList: []
            }
          },
          {
            details: {
              dimensionFieldList: [],
              measureFieldList: [{name: {value: 'columnA'}}],
              partitionFieldList: [{name: {value: 'columnA'}}, {name: {value: 'columnB'}}],
              sortFieldList: []
            }
          }
        ]
      });
      instance.setState({ currentCell: {columnIndex: 0} });
      expect(
        instance.findCurrentIndexInFieldsList(currentColumn, 'partitionFieldList', 1)
      ).to.eql(0);
    });
  });

  describe('findCurrentColumnInLayouts', () => {
    it('should return current column in field list', () => {
      expect(instance.findCurrentColumnInLayouts('partitionFieldList', 1, 0)).to.eql({
        name: {value: 'columnB'}
      });
    });

    it('should return undefined if field have not current column', () => {
      expect(instance.findCurrentColumnInLayouts('dimensionFieldList', 0, 1)).to.eql(undefined);
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
            details: {
              measureFieldList: { removeField: sinon.spy() }
            }
          }
        ]
      });
    });

    it('should remove current field from measureFieldList', () => {
      sinon.stub(instance, 'findCurrentIndexInFieldsList').returns(1);
      instance.removeFieldByIndex(0, 'measureFieldList', 0);
      expect(instance.findCurrentIndexInFieldsList).to.have.been.called;
      expect(instance.props.layoutFields[0].details.measureFieldList.removeField).to.have.been.calledWith(1);
    });

    it('should not remove current field if we have not this field in list of fields', () => {
      sinon.stub(instance, 'findCurrentIndexInFieldsList').returns(-1);
      instance.removeFieldByIndex(0, 'measureFieldList', 0);
      expect(instance.props.layoutFields[0].details.measureFieldList.removeField.called).to.be.false;
    });
  });

  describe('addFieldByIndex', () => {
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            details: {
              measureFieldList: { addField: sinon.spy() }
            }
          }
        ]
      });
    });

    it('should add current field to measureFieldList', () => {
      sinon.stub(instance, 'findCurrentIndexInFieldsList').returns(-1);
      instance.addFieldByIndex({name: 'col_name'}, 'measureFieldList', 0);
      expect(instance.findCurrentIndexInFieldsList).to.have.been.called;
      expect(
        instance.props.layoutFields[0].details.measureFieldList.addField
      ).to.have.been.calledWith({name: 'col_name'});
    });

    it('should add current field if we have not this field in list of fields', () => {
      sinon.stub(instance, 'findCurrentIndexInFieldsList').returns(0);
      instance.addFieldByIndex(0, 'measureFieldList', 0);
      expect(instance.props.layoutFields[0].details.measureFieldList.addField).to.have.not.been.called;
    });
  });

  describe('toggleField', () => {
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            details: {
              measureFieldList: { addField: sinon.spy() }
            }
          }
        ]
      });
      sinon.stub(instance, 'getRowByIndex').returns({name: 'columnA'});
      sinon.stub(instance, 'removeFieldByIndex');
    });

    it('should remove current field from measureFieldList', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns({});
      instance.toggleField('measureFieldList', 0, 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith({name: 'columnA'}, 'measureFieldList', 0);
    });

    it('should add current field to measureFieldList', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns(undefined);
      instance.toggleField('measureFieldList', 0, 0);
      expect(instance.props.layoutFields[0].details.measureFieldList.addField).to.be.calledWith({name: 'columnA'});
    });
  });

  describe('toggleSortField', () => {
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            details: {
              sortFieldList: { addField: sinon.spy() }
            }
          }
        ]
      });
      instance.setState({
        currentCell: {
          field: 'sortFieldList'
        }
      });
      sinon.stub(instance, 'getRowByIndex').returns({name: 'columnA'});
      sinon.stub(instance, 'removeFieldByIndex');
    });

    it('should remove current field from sortFieldList', () => {
      instance.toggleSortField('', 0, 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith({name: 'columnA'}, 'sortFieldList', 0);
    });

    it('should add current field to sortFieldList', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns(undefined);
      instance.toggleSortField('sortFieldList', 0, 0);
      expect(instance.props.layoutFields[0].details.sortFieldList.addField).to.be.calledWith({name: 'columnA'});
    });

    it('should not add current field to sortFieldList when already added', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns({});
      instance.toggleSortField('sortFieldList', 0, 0);
      expect(instance.props.layoutFields[0].details.sortFieldList.addField).to.not.be.called;
    });
  });

  describe('applyConstraints', () => {
    beforeEach(() => {
      sinon.stub(instance, 'applyAggregationConstraints');
      sinon.stub(instance, 'applyRawConstraints');
    });

    it('should call applyRawConstraints when activeTab is raw', () => {
      wrapper.setProps({activeTab: 'raw'});
      instance.applyConstraints('measureFieldList', 0, 0);
      expect(
        instance.applyRawConstraints
      ).to.have.been.calledWith('measureFieldList', 0, 0);
    });

    it('should call applyAggregationConstraints when activeTab is not raw', () => {
      wrapper.setProps({activeTab: ''});
      instance.applyConstraints('measureFieldList', 0, 0);
      expect(
        instance.applyAggregationConstraints
      ).to.have.been.calledWith('measureFieldList', 0, 0);
    });
  });


  describe('handleOnSelectSortItem', () => {
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
      instance.handleOnSelectSortItem('sortFieldList');
      expect(instance.toggleSortField).to.have.been.calledWith('sortFieldList', 0, 0);
    });
    it('should call applyConstraintsNext', () => {
      instance.handleOnSelectSortItem('sortFieldList');
      expect(instance.applyConstraintsNext).to.have.been.calledWith('sortFieldList', 0, 0);
    });
    it('should call handleRequestClose', () => {
      instance.handleOnSelectSortItem('sortFieldList');
      expect(instance.handleRequestClose).to.have.been.calledOnce;
    });
  });

  describe('handleOnCheckboxItem', () => {
    beforeEach(() => {
      sinon.stub(instance, 'toggleField');
      sinon.stub(instance, 'applyConstraintsNext');
    });

    it('should call toggleField', () => {
      instance.handleOnCheckboxItem('dimensionFieldList', 0, 1);
      expect(instance.toggleField).to.have.been.calledWith('dimensionFieldList', 0, 1);
    });
    it('should call applyConstraintsNext', () => {
      instance.handleOnCheckboxItem('dimensionFieldList', 0, 1);
      expect(instance.applyConstraintsNext).to.have.been.calledWith('dimensionFieldList', 0, 1);
    });
  });


  describe('applyAggregationConstraints', () => {
    let currentRow;
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            details: {
              dimensionFieldList: { addField: sinon.spy() }
            }
          }
        ]
      });
      currentRow = { name: 'columnA' };
      sinon.stub(instance, 'getRowByIndex').returns(currentRow);
      sinon.stub(instance, 'removeFieldByIndex');
      sinon.stub(instance, 'addFieldByIndex');
    });

    it('should remove sortFieldList, partitionFieldList and distributionFieldList ' +
    'when dimensionFieldList and measureFieldList unselected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns(undefined);
      instance.applyAggregationConstraints('dimensionFieldList', 0, 0);

      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'sortFieldList', 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'partitionFieldList', 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'distributionFieldList', 0);
    });

    it('should remove sortFieldList when dimensionFieldList unselected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts')
        .withArgs('dimensionFieldList', 0, 0).returns(undefined)
        .withArgs('measureFieldList', 0, 0).returns({});
      instance.applyAggregationConstraints('dimensionFieldList', 0, 0);

      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'sortFieldList', 0);
    });

    it('should remove measureFieldList and add dimensionFieldList when sortFieldList selected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts')
        .withArgs('dimensionFieldList', 0, 0).returns(undefined)
        .withArgs('measureFieldList', 0, 0).returns({})
        .withArgs('sortFieldList', 0, 0).returns({});
      instance.applyAggregationConstraints('sortFieldList', 0, 0);

      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'measureFieldList', 0);
      expect(
        instance.addFieldByIndex
      ).to.have.been.calledWith(currentRow, 'dimensionFieldList', 0);
    });

    it('should add dimensionFieldList when distributionFieldList or partitionFieldList or sortFieldList selected', () => {
      const findCurrentColumnStub = sinon.stub(instance, 'findCurrentColumnInLayouts')
        .withArgs('partitionFieldList', 0, 0).returns({});
      const resetFieldsStub = () => {
        instance.addFieldByIndex.reset();
        instance.removeFieldByIndex.reset();
      };
      const verify = () => {
        expect(
          instance.addFieldByIndex
        ).to.have.been.calledWith(currentRow, 'dimensionFieldList', 0);
        expect(
          instance.removeFieldByIndex
        ).to.have.been.calledWith(currentRow, 'measureFieldList', 0);
      };

      instance.applyAggregationConstraints('partitionFieldList', 0, 0);
      verify();

      findCurrentColumnStub.withArgs('partitionFieldList', 0, 0).returns(undefined)
        .withArgs('distributionFieldList', 0, 0).returns({});
      resetFieldsStub();
      instance.applyAggregationConstraints('distributionFieldList', 0, 0);
      verify();

      findCurrentColumnStub.withArgs('distributionFieldList', 0, 0).returns(undefined)
        .withArgs('sortFieldList', 0, 0).returns({});
      resetFieldsStub();
      instance.applyAggregationConstraints('sortFieldList', 0, 0);
      verify();
    });

    it('should remove dimensionFieldList and sortFieldList when measureFieldList is selected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts')
        .withArgs('dimensionFieldList', 0, 0).returns({})
        .withArgs('measureFieldList', 0, 0).returns({});
      instance.applyAggregationConstraints('measureFieldList', 0, 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'dimensionFieldList', 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'sortFieldList', 0);
    });

    it('should remove measureFieldList when dimensionFieldList is selected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts')
        .withArgs('dimensionFieldList', 0, 0).returns({})
        .withArgs('measureFieldList', 0, 0).returns({});
      instance.applyAggregationConstraints('dimensionFieldList', 0, 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'measureFieldList', 0);
    });
  });

  describe('applyRawConstraints', () => {
    let currentRow;
    beforeEach(() => {
      wrapper.setProps({
        layoutFields: [
          {
            details: {
              displayFieldList: { addField: sinon.spy() }
            }
          }
        ]
      });
      currentRow = { name: 'columnA' };
      sinon.stub(instance, 'getRowByIndex').returns(currentRow);
      sinon.stub(instance, 'removeFieldByIndex');
    });

    it('should remove sortFieldList, partitionFieldList and distributionFieldList ' +
      'when displayFieldList unselected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns(undefined);
      instance.applyRawConstraints('displayFieldList', 0, 0);

      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'sortFieldList', 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'partitionFieldList', 0);
      expect(
        instance.removeFieldByIndex
      ).to.have.been.calledWith(currentRow, 'distributionFieldList', 0);
    });

    it('should add displayFieldList when sortFieldList, partitionFieldList or distributionFieldList selected', () => {
      sinon.stub(instance, 'findCurrentColumnInLayouts').returns(undefined);
      instance.applyRawConstraints('sortFieldList', 0, 0);

      expect(
        instance.props.layoutFields[0].details.displayFieldList.addField
      ).to.have.been.calledWith(currentRow);
    });
  });
});
