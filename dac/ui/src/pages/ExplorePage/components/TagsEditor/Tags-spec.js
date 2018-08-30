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
import { shallow, mount } from 'enzyme';
import { List } from 'immutable';
import keyCodes from '@app/constants/Keys.json';
import { TagsView } from './Tags';
import {
  deleteButton
} from './Tag.less';

const generateTags = numberOfTags => List(Array(numberOfTags).fill().map((e, index) => `tag ${index + 1}`));
const simulateKeyDown = (wrapper, keyCode) => wrapper.simulate('keydown', {
  keyCode,
  preventDefault: () => {}
});
const getInput = wrapper => wrapper.find('input');
const setValueToInput = (wrapper, value) => {
  const input = getInput(wrapper);

  input.simulate('change', { target: { value } });
};
const getAllTags = wrapper => wrapper.find('Tag');
const clickTag = (wrapper, index) => {
  getAllTags(wrapper).at(index).simulate('click');
};
const clickBeforeTag = (wrapper, index) => {
  wrapper.find('.cursorPlaceholder').at(index).simulate('click');
};

describe('Tags', () => {
  let commonProps;
  const tagsCount = 5;

  beforeEach(() => {
    commonProps = {
      tags: generateTags(tagsCount),
      onAddTag: () => {},
      onRemoveTag: () => {}
    };
  });

  it('Right number of items is rendered', () => {

    const wrapper = shallow(<TagsView {...commonProps} />);

    expect(getAllTags(wrapper).length).to.eq(tagsCount);
  });

  const addTagTest = keyCodeName => {
    if (!keyCodes.hasOwnProperty(keyCodeName)) {
      throw new Error(`keyCodeName (${keyCodeName}) must be presented in keyCodes`);
    }

    it(`Add tag is called, when ${keyCodeName} is pressed. Tag selection is reset.`, () => {
      const addHandler = sinon.spy();
      const wrapper = shallow(<TagsView {...commonProps} onAddTag={addHandler} />);
      const instance = wrapper.instance();
      const tagName = 'test tag name with space';

      instance.setState({ selectedTagIndex: 0 }); // select the first tag

      setValueToInput(wrapper, tagName);

      expect(instance.state.value).to.eq(tagName); // check that value is set to a state

      expect(addHandler).to.be.not.called;

      simulateKeyDown(wrapper, keyCodes[keyCodeName]);

      expect(addHandler).to.be.calledWith(tagName);
      expect(instance.state.selectedTagIndex).to.eq(-1); // tag selection is reset
    });
  };

  ['ENTER', 'TAB'].forEach(addTagTest);

  it('Tab does not adds a tag if input is empty', () => {
    const addHandler = sinon.spy();
    const wrapper = shallow(<TagsView {...commonProps} onAddTag={addHandler} />);

    setValueToInput(wrapper, ''); // nothin is entered

    expect(addHandler).to.be.not.called;

    simulateKeyDown(wrapper, keyCodes.TAB);

    expect(addHandler).to.be.not.called;
  });

  it('tag is selected on click', () => {
    const wrapper = mount(<TagsView {...commonProps} />);
    const index = 2;
    clickTag(wrapper, index);

    const state = wrapper.instance().state;
    expect(state.tagCursorPosition).to.eq(index); // respective tag is selected
    expect(state.selectedTagIndex).to.eq(index); // and a cursor is put before selected tag
  });

  describe('cursor move', () => { // here we have to use mount, as we need access to dom element to handle focus

    const homeTest = keyName => it(`Cursor is moved to a first tag, when ${keyName} is pressed. Selection should be reset`, () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      instance.selectTag(tagsCount - 1, true); // select last tag and move a cursor to it

      simulateKeyDown(wrapper, keyCodes[keyName]);

      expect(instance.state.tagCursorPosition).to.eq(0);
    });

    // array values should respect keyCodes field names
    ['HOME', 'UP'].forEach(homeTest);

    const endTest = keyName => it(`Cursor is moved to a last character, when ${keyName} is pressed. Selection should be reset`, () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      const str = 'a tag name';
      setValueToInput(wrapper, str);

      simulateKeyDown(wrapper, keyCodes[keyName]);

      expect(instance.state.tagCursorPosition).to.eq(-1); // cursor should be located in input, so tagCursorPosition should be reset to -1

      const inputWrapper = getInput(wrapper);

      expect(inputWrapper.node.selectionStart).to.eq(str.length); // check that a cursor in END position
    });

    // array values should respect keyCodes field names
    ['END', 'DOWN'].forEach(endTest);

    it('Cursor is moved to a right position, when an user clicks between tags', () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      const index = 3;

      clickBeforeTag(wrapper, index);

      expect(instance.state.tagCursorPosition).to.eq(index);
    });

    it('left arrow is pressed', () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      const index = 3;

      clickBeforeTag(wrapper, index); // set initial cursor position

      simulateKeyDown(wrapper, keyCodes.LEFT);

      expect(instance.state.tagCursorPosition).to.eq(index - 1);
    });

    it('cursor is moved to tags section, if left arrow is pressed and cursor in the begining of the input section', () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      getInput(wrapper).simulate('focus'); // input is empty and focus in its begining
      expect(instance.state.tagCursorPosition).to.eq(-1); // means input is selected

      simulateKeyDown(getInput(wrapper), keyCodes.LEFT);
      expect(instance.state.tagCursorPosition).to.eq(commonProps.tags.size - 1); // means a cursor is moved to the tags section right before last tag;
    });

    it('right arrow is pressed', () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      const index = 3;

      clickBeforeTag(wrapper, index); // set initial cursor position

      simulateKeyDown(wrapper, keyCodes.RIGHT);

      expect(instance.state.tagCursorPosition).to.eq(index + 1);
    });
  });

  describe('tags deletion', () => {
    let wrapper;
    let removeHandler;
    let instance;

    beforeEach(() => {
      removeHandler = sinon.spy();
      wrapper = mount(<TagsView {...commonProps} onRemoveTag={removeHandler} />);
      instance = wrapper.instance();
    });

    const deleteTest = isBACKSPACE => it(`tag is selected first before deletion using ${isBACKSPACE ? 'BACKSPACE' : 'DELETE'} button`, () => {
      const position = 2;
      const indexToRemove = isBACKSPACE ? position - 1 : position;
      const keyCode = keyCodes[isBACKSPACE ? 'BACKSPACE' : 'DELETE'];
      clickBeforeTag(wrapper, position); // move cursor to a position before 3rd tag (< tagsCount)

      simulateKeyDown(wrapper, keyCode);
      expect(removeHandler.notCalled).to.eq(true); // DELETE handler is not called
      expect(instance.state.tagCursorPosition).to.eq(position); // position is not changed
      expect(instance.state.selectedTagIndex).to.eq(indexToRemove); // next or previsouse tag was selected depending on the key
      expect(getAllTags(wrapper).length).to.eq(tagsCount); // tag count was not changed

      simulateKeyDown(wrapper, keyCode);
      expect(removeHandler.calledWith(commonProps.tags.get(indexToRemove))).to.eq(true); // DELETE handler called for a next to the cursor tag
      expect(instance.state.tagCursorPosition).to.eq(indexToRemove);
      expect(instance.state.selectedTagIndex).to.eq(-1); // no selected tags
    });

    deleteTest(true);
    deleteTest(false);

    it('tag is removed on x click', () => {
      const index = 3;
      getAllTags(wrapper).at(index).find(`.${deleteButton}`).simulate('click');

      expect(removeHandler.calledWith(commonProps.tags.get(index))).to.eq(true); // DELETE handler called for a next to the cursor tag
      expect(instance.state.tagCursorPosition).to.eq(index);
    });
  });
});
