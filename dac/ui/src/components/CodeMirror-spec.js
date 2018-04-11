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

import CodeMirror from './CodeMirror';

describe('CodeMirror', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      codeMirror: sinon.stub().returns({
        on: sinon.spy(),
        setValue: sinon.spy(),
        setOption: sinon.spy(),
        focus: sinon.spy(),
        execCommand: sinon.spy(),
        toTextArea: sinon.spy()
      })
    };
    commonProps = {
      ...minimalProps,
      defaultValue: 'the default value',
      options: {
        mode: 'text/x-sql',
        theme: 'mdn-like',
        lineWrapping: true
      },
      onChange: sinon.spy(),
      onDragover: sinon.spy()
    };

    wrapper = shallow(<CodeMirror {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<CodeMirror {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render div', () => {
    expect(wrapper.find('div')).to.have.length(1);
  });

  describe('#componentDidMount', () => {
    it('should init codeMirror, register event handlers', () => {
      instance.componentDidMount();
      expect(commonProps.codeMirror).to.be.called;
      expect(instance.editor.on).to.be.calledTwice;

      expect(instance.editor.on.args[0][0]).to.equal('change');
      expect(instance.editor.on.args[0][1]).to.equal(instance.handleChange);

      expect(instance.editor.on.args[1][0]).to.equal('dragover');
      expect(instance.editor.on.args[1][1]).to.equal(commonProps.onDragover);
    });

    it('should set default value only if it !== undefined', () => {
      sinon.spy(instance, 'resetValue');
      instance.componentDidMount();
      expect(instance.resetValue).to.be.called;
      instance.resetValue.reset();

      wrapper = shallow(<CodeMirror {...commonProps} defaultValue={undefined}/>);
      instance = wrapper.instance();
      sinon.spy(instance, 'resetValue');
      instance.componentDidMount();
      expect(instance.resetValue).to.not.be.called;

      wrapper = shallow(<CodeMirror {...commonProps} defaultValue={''}/>);
      instance = wrapper.instance();
      sinon.spy(instance, 'resetValue');
      instance.componentDidMount();
      expect(instance.resetValue).to.be.called;
    });
  });

  describe('#componentDidUpdate', () => {
    it('should resetValue only if defaultValue has changed', () => {
      instance.componentDidMount();
      sinon.spy(instance, 'resetValue');

      instance.componentDidUpdate(commonProps);
      expect(instance.resetValue).to.not.be.called;

      instance.componentDidUpdate({...commonProps, defaultValue: 'different value'});
      expect(instance.resetValue).to.be.called;

      wrapper.setProps({defaultValue: undefined});
      instance.componentDidUpdate({...commonProps, defaultValue: 'different value'});
      expect(instance.resetValue).to.be.calledTwice;

      wrapper.setProps({defaultValue: ''});
      instance.componentDidUpdate({...commonProps, defaultValue: 'different value'});
      expect(instance.resetValue).to.be.calledThrice;
    });
  });

  describe('#handleChange', () => {
    it('should call props.onChange only if !reseting', () => {
      instance.reseting = true;
      instance.handleChange();
      expect(commonProps.onChange).to.not.be.called;
      instance.reseting = false;
      instance.handleChange();
      expect(commonProps.onChange).to.be.called;
    });
  });

  describe('#resetValue()', () => {
    it('should setValue', () => {
      instance.editor = commonProps.codeMirror();
      instance.resetValue();
      expect(instance.editor.setValue).to.be.calledWith(commonProps.defaultValue);
      expect(commonProps.onChange).to.not.be.called;
      expect(instance.reseting).to.be.false;
    });

    it('should default defaultValue to empty string', () => {
      instance.editor = commonProps.codeMirror();
      wrapper.setProps({defaultValue: undefined});
      instance.resetValue();
      expect(instance.editor.setValue).to.be.calledWith('');
    });
  });
});
