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
import ReactDOM from 'react-dom';
import exploreUtils from 'utils/explore/exploreUtils';
import ExploreCellLargeOverlay from './ExploreCellLargeOverlay';

describe('ExploreCellLargeOverlay', () => {
  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    minimalProps = {
      anchor: document.createElement('span'),
      hide: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      columnType: 'TEXT',
      columnName: 'revenue',
      cellValue: 'dremio forever',
      location: { state: {} },
      selectAll: sinon.spy(),
      selectItemsOfList: sinon.spy(),
      onSelect: sinon.spy()
    };
    context = { router: { push: sinon.spy() } };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ExploreCellLargeOverlay {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should not render Select All if !props.onSelect ', () => {
    const wrapper = shallow(<ExploreCellLargeOverlay {...commonProps} onSelect={null}/>);
    expect(wrapper.contains('Select all')).to.eql(false);
  });

  it('should render Select All if type not eql List or MAP ', () => {
    let wrapper = shallow(<ExploreCellLargeOverlay {...commonProps}/>);
    expect(wrapper.contains('Select all')).to.eql(true);

    wrapper = shallow(<ExploreCellLargeOverlay {...commonProps} columnType='LIST' />);
    expect(wrapper.contains('Select all')).to.eql(false);
  });

  it('should render CellPopover if columnType= LIST or MAP', () => {
    let wrapper = shallow(<ExploreCellLargeOverlay {...commonProps}/>);
    expect(wrapper.find('CellPopover')).to.have.length(0);

    wrapper = shallow(<ExploreCellLargeOverlay {...commonProps}  columnType='LIST'/>);
    expect(wrapper.find('CellPopover')).to.have.length(1);

    wrapper = shallow(<ExploreCellLargeOverlay {...commonProps}  columnType='MAP'/>);
    expect(wrapper.find('CellPopover')).to.have.length(1);
  });

  it('should select all content in overlay', () => {
    const wrapper = shallow(<ExploreCellLargeOverlay {...commonProps}/>);
    const instance = wrapper.instance();
    instance.handleSelectAll();

    expect(commonProps.selectAll.calledWith(
      undefined, commonProps.columnType, commonProps.columnName, commonProps.cellValue
    )).to.be.true;
  });

  describe('onMouseUp', () => {
    let selection;
    let data;
    beforeEach(() => {
      selection = {
        oRange: { startContainer: { data: 'dremio forever' } },
        text: 'drem',
        startOffset: 2,
        endOffset: 5,
        oRect: { top: 0, left: 0, right: 0, width: 0 }
      };
      data = { model: { m: '1' }, position: { p: '1' } };
      sinon.stub(exploreUtils, 'getSelectionData').returns(selection);
      sinon.stub(exploreUtils, 'getSelection').returns(data);
    });
    afterEach(() => {
      exploreUtils.getSelectionData.restore();
      exploreUtils.getSelection.restore();
    });

    it('should do nothing if !this.props.onSelect', () => {
      const wrapper = shallow(<ExploreCellLargeOverlay {...commonProps} onSelect={null}/>);
      const instance = wrapper.instance();
      instance.onMouseUp();
    });

    it('should select all content if column type is numeric', () => {
      const { columnName, cellValue } = commonProps;
      const wrapper = shallow(<ExploreCellLargeOverlay {...commonProps} columnType='INTEGER'/>);
      const instance = wrapper.instance();
      instance.onMouseUp();

      expect(commonProps.selectAll.calledWith(
        undefined, 'INTEGER', columnName, cellValue
      )).to.be.true;
    });

    it('should select content if column type is not numeric', () => {
      const { location, columnName, columnType } = commonProps;
      const wrapper = shallow(<ExploreCellLargeOverlay {...commonProps}/>, { context });
      const instance = wrapper.instance();
      instance.onMouseUp();

      expect(context.router.push.calledWith({
        ...location,
        state: {
          ...location.state,
          columnName,
          columnType,
          selection: Immutable.fromJS({...data.model, columnName})
        }
      })).to.be.true;

      expect(commonProps.onSelect.calledWith({ ...data.position, columnType })).to.be.true;

    });

  });

  describe('changePlacementIfNecessary', () => {
    let findDOMNodeStub;
    beforeEach(() => {
      findDOMNodeStub = sinon.stub(ReactDOM, 'findDOMNode');
    });
    afterEach(() => {
      ReactDOM.findDOMNode.restore();
    });
    it('should change placement to bottom if overlay is too big', () => {
      findDOMNodeStub.returns({
        getBoundingClientRect: () => ({ height: 200 })
      });
      const wrapper = shallow(<ExploreCellLargeOverlay {...commonProps}/>);
      const instance = wrapper.instance();
      instance.props.anchor.getBoundingClientRect = () => ({ top: 100 });
      instance.changePlacementIfNecessary();
      expect(wrapper.state().placement).to.eql('bottom');
    });
    it('should not change placement to bottom if overlay is small', () => {
      findDOMNodeStub.onFirstCall().returns({
        getBoundingClientRect: () => ({ top: 200 })
      });
      findDOMNodeStub.onSecondCall().returns({
        getBoundingClientRect: () => ({ height: 50 })
      });
      const wrapper = shallow(<ExploreCellLargeOverlay {...commonProps}/>);
      const instance = wrapper.instance();
      instance.changePlacementIfNecessary();
      expect(wrapper.state().placement).to.eql('top');
    });
  });

  describe('calculatePlacementLeft', () => {
    let findDOMNodeStub;
    let clock;
    beforeEach(() => {
      findDOMNodeStub = sinon.stub(ReactDOM, 'findDOMNode');
      clock = sinon.useFakeTimers();
    });
    afterEach(() => {
      ReactDOM.findDOMNode.restore();
      clock.restore();
    });
    it('should calculate placement left for pointer', (done) => {
      findDOMNodeStub.returns({
        getBoundingClientRect: () => ({ left: 53 })
      });
      const wrapper = shallow(<ExploreCellLargeOverlay {...commonProps}/>);
      const instance = wrapper.instance();
      instance.props.anchor.getBoundingClientRect = () => ({ left: 100 });
      instance.calculatePlacementLeft();
      clock.tick(100);
      expect(wrapper.state().placementLeft).to.eql(44);
      done();
    });
  });
});
