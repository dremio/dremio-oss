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

import SelectContextForm from 'pages/ExplorePage/components/forms/SelectContextForm';
import { constructFullPath } from 'utils/pathUtils';
import codeMirrorUtils from 'utils/CodeMirrorUtils';

import SqlAutoComplete from './SqlAutoComplete';

describe('SqlAutoComplete', () => {
  let commonProps;
  let context;
  let wrapper;
  let instance;
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
      changeQueryContext: sinon.spy()
    };
    context = {
      router: {push: sinon.spy()},
      routeParams: {tableId: 'table'},
      location: {query: {version: 'version'}}
    };

    wrapper = shallow(<SqlAutoComplete {...commonProps}/>, {context});
    instance = wrapper.instance();
  });

  it('renders <div>', () => {
    expect(wrapper.type()).to.eql('div');
    expect(wrapper.prop('className')).to.eql('sql-autocomplete');
    expect(wrapper.find('CodeMirror')).to.have.length(1);
    expect(wrapper.find('Modal')).to.have.length(0);
  });

  describe('edit query context', () => {
    it('should render edit context button', () => {
      expect(wrapper.find('.context').text()).to.eql('Context: my-space.my.folder');

      wrapper.setProps({context: null});
      expect(wrapper.find('.context').text()).to.eql('Context: <none>');
    });

    it('should set showSelectContextModal when edit is clicked', () => {
      wrapper.instance().handleClickEditContext();
      expect(wrapper.state('showSelectContextModal')).to.be.true;
    });

    it('should unset showSelectContextModal on hide', () => {
      wrapper.instance().hideSelectContextModal();
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

  describe('#insertFullPathAtPosition()', () => {
    beforeEach(() => {
      sinon.stub(codeMirrorUtils, 'insertTextAtPos');
      instance.editor = {
        getCursor: sinon.stub().returns(123),
        doc: {
          getValue: sinon.stub().returns('theValue'),
          setValue: sinon.spy()
        }
      };
    });
    afterEach(() => {
      codeMirrorUtils.insertTextAtPos.restore();
    });
    it('should escape param if it is a string, or constructFullPath if it is a list', () => {
      instance.insertFullPathAtPosition('needs-escaping', 0);
      expect(codeMirrorUtils.insertTextAtPos).to.be.calledWith(instance.editor, '"needs-escaping"', 0);

      const fullPath = ['a', 'b', 'c'];
      instance.insertFullPathAtPosition(fullPath, 0);
      expect(codeMirrorUtils.insertTextAtPos).to.be.calledWith(instance.editor, constructFullPath(fullPath), 0);
    });

    it('should set state.code to editor.doc.getValue', () => {
      instance.insertFullPathAtPosition('needs-escaping', 0);
      expect(instance.state.code).to.eql('theValue');
    });

    it('should call onChange with editor.doc.getValue', () => {
      instance.insertFullPathAtPosition('needs-escaping', 0);
      expect(commonProps.onChange).to.be.calledWith('theValue');
    });

    describe('#insertFullPathAtDrop()', () => {
      it('should call insertFullPathAtPosition with this.posForDrop', () => {
        sinon.spy(instance, 'insertFullPathAtPosition');
        instance.posForDrop = 987;
        instance.insertFullPathAtDrop('name');
        expect(instance.insertFullPathAtPosition).to.be.calledWith('name', 987);
      });
    });

    describe('#insertFullPathAtCursor()', () => {
      it('should call insertFullPathAtPosition with editor.getCursor()', () => {
        sinon.spy(instance, 'insertFullPathAtPosition');
        instance.insertFullPathAtCursor('name');
        expect(instance.insertFullPathAtPosition).to.be.calledWith('name', instance.editor.getCursor());
      });
    });
  });


});
