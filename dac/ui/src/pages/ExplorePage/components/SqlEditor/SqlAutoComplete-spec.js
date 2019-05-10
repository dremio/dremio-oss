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

import SelectContextForm from 'pages/ExplorePage/components/forms/SelectContextForm';
import DragTarget from 'components/DragComponents/DragTarget';

import SqlAutoComplete from './SqlAutoComplete';

const fakeRange1 = {startLineNumber: 1, startColumn: 1, endLineNumber: 1, endColumn: 1};
const fakeRange2 = {startLineNumber: 2, startColumn: 2, endLineNumber: 3, endColumn: 3};

describe('SqlAutoComplete', () => {
  let commonProps;
  let context;
  let wrapper;
  let instance;
  let editor;
  let sqlEditor;
  beforeEach(() => {
    commonProps = {
      onChange: sinon.spy(),
      defaultValue: 'select * from foo',
      isGrayed: false,
      onFocus: sinon.spy(),
      tooltip: 'hello',
      context: Immutable.List(['my-space', 'my.folder']),
      name: 'name',
      sqlSize: 300,
      datasetsPanel: false,
      funcHelpPanel: false,
      changeQueryContext: sinon.spy(),
      autoCompleteEnabled: true
    };
    context = {
      router: {push: sinon.spy()},
      routeParams: {tableId: 'table'},
      location: {query: {version: 'version'}}
    };

    wrapper = shallow(<SqlAutoComplete {...commonProps}/>, {context});
    instance = wrapper.instance();

    editor = {
      getSelections: sinon.stub().returns([fakeRange1, fakeRange2]),
      executeEdits: sinon.stub(),
      pushUndoStop: sinon.stub(),
      getTargetAtClientPoint(x, y) {
        return {range: {startLineNumber: 1, startColumn: x, endLineNumber: 1, endColumn: y}};
      }
    };
    instance.getMonacoEditorInstance = () => editor;
    instance.sqlEditor = sqlEditor = { // mock sqlEditor ref
      focus: sinon.stub()
    };
  });

  it('renders DragTarget wrapper, .sql-autocomplete, CodeMirror component', () => {
    expect(wrapper.type()).to.eql(DragTarget);
    // finding SQLEditor after it's been connected to redux
    expect(wrapper.find('Connect(SQLEditor)')).to.have.length(1);
    expect(wrapper.find('Modal')).to.have.length(0);
  });

  describe('edit query context', () => {
    it('should render edit context button', () => {
      expect(wrapper.find('.context').text()).to.eql('Context: my-space.my.folder');

      wrapper.setProps({context: null});
      expect(wrapper.find('.context').text()).to.eql('Context: <none>');
    });

    it('should set showSelectContextModal when edit is clicked', () => {
      instance.handleClickEditContext();
      expect(wrapper.state('showSelectContextModal')).to.be.true;
    });

    it('should unset showSelectContextModal on hide', () => {
      instance.hideSelectContextModal();
      expect(wrapper.state('showSelectContextModal')).to.be.false;
    });

    it('should render modal when showSelectContextModal=true', () => {
      wrapper.setState({'showSelectContextModal': true});
      expect(wrapper.find('Modal')).to.have.length(1);
      expect(wrapper.find(SelectContextForm)).to.have.length(1);
    });

    it('should render SelectContextForm with context initialValue', () => {
      wrapper.setState({'showSelectContextModal': true});
      expect(wrapper.find(SelectContextForm).prop('initialValues')).to.eql({context: '"my-space"."my.folder"'});
    });
  });

  describe('#insertAtRanges()', () => {
    it('default ranges should be selections', () => {
      instance.insertAtRanges('foo');
      expect(editor.executeEdits).to.have.been.calledWith('dremio', [{ identifier: 'dremio-inject', range: fakeRange1, text: 'foo' }, { identifier: 'dremio-inject', range: fakeRange2, text: 'foo' }]);
      expect(editor.pushUndoStop).to.have.been.called;
      expect(sqlEditor.focus).to.have.been.called;
    });

    it('passing ranges', () => {
      instance.insertAtRanges('foo', [fakeRange2, fakeRange1]);
      expect(editor.executeEdits).to.have.been.calledWith('dremio', [{ identifier: 'dremio-inject', range: fakeRange2, text: 'foo' }, { identifier: 'dremio-inject', range: fakeRange1, text: 'foo' }]);
      expect(editor.pushUndoStop).to.have.been.called;
      expect(sqlEditor.focus).to.have.been.called;
    });
  });

  describe('#insert${TYPE}()', () => {
    const ranges = [fakeRange2, fakeRange1];
    beforeEach(() => {
      sinon.stub(instance, 'insertAtRanges');
    });

    it('#insertFullPath()', () => {
      instance.insertFullPath(Immutable.fromJS(['@a', 'b']), ranges);
      expect(instance.insertAtRanges).to.be.calledWith('"@a".b', ranges);
    });

    it('#insertFieldName()', () => {
      instance.insertFieldName('1a', ranges);
      expect(instance.insertAtRanges).to.be.calledWith('"1a"', ranges);
    });

    it('#insertFunction()', () => {
      instance.insertFunction('PI', null, ranges);
      expect(instance.insertAtRanges).to.be.calledWith('PI', ranges);

      instance.insertFunction('PI', '', ranges);
      expect(instance.insertAtRanges).to.be.calledWith('PI', ranges);

      // testing advanced cases requires access to monaco classes, which we don't have a way of loading in unit tests for now
    });
  });

  describe('#handleDrop()', () => {
    beforeEach(() => {
      sinon.stub(instance, 'insertFunction');
      sinon.stub(instance, 'insertFieldName');
      sinon.stub(instance, 'insertFullPath');
    });
    it('with function', () => {
      const data = { id: 'TO_CHAR', args: '({expression}, [literal string] {format})' };
      instance.handleDrop(data);
      expect(instance.insertFunction).to.have.been.calledWith(data.id, data.args);
    });
    it('with field name', () => {
      const data = { id: 'theName' };
      instance.handleDrop(data);
      expect(instance.insertFieldName).to.have.been.calledWith(data.id);
    });
    it('with full path', () => {
      const data = { id: Immutable.fromJS(['@a', 'b'])};
      instance.handleDrop(data);
      expect(instance.insertFullPath).to.have.been.calledWith(data.id);
    });
  });
});
