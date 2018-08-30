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
import { List } from 'immutable';
import classNames from 'classnames';
import keyCodes from '@app/constants/Keys.json';
import {
  container,
  input as inputCls,
  tagElement as tagElementCls,
  cursor,
  tagWrapper,
  cursorPlaceholder
} from './Tags.less';
import { Tag } from './Tag';

// component consist of 2 main sections:
// 1) Tags section. The section where tags are displayed
// 2) Input section. The section where a text input is located. User types a new tag here.
// I will references on this section through the code.
export class TagsView extends Component {
  static propTypes = {
    placeholder: PropTypes.string,
    tags: PropTypes.instanceOf(List).isRequired,

    onAddTag: PropTypes.func.isRequired, // (tagName: string)
    onRemoveTag: PropTypes.func.isRequired, // (tagName: string)
    onChange: PropTypes.func // function () {} is called, when tag is added or removed
  }

  state = {
    value: '', // value of input
    tagCursorPosition: -1, // a zero-base index of a tag, before which a cursor is located in tags section. -1 means that cursor in an input.
    selectedTagIndex: -1 // a zero-base index of a selected tag. -1 means there are no selected tag.
  }

  handleTextChange = (e) => {
    const value = e.target.value;
    this.setState({
      value
    });
  }

  addTag = (newTag) => {
    const {
      onAddTag
    } = this.props;

    if (!newTag.trim()) return;

    onAddTag(newTag);
    this.setState({
      value: '', // string was transformed to a tags
      selectedTagIndex: -1 // deselects selected tag, if there is any. Product manager asked this
    });
    this.onChange();
  }

  onKeyDown = (e) => {
    const {
      tags
    } = this.props;
    const {
      selectedTagIndex,
      tagCursorPosition,
      value
    } = this.state;
    const areTagsFocused = this.areTagsFocused();
    let preventDefault = false; // indicates if we should cancel a default behaviour for current key

    switch (e.keyCode) {
    case keyCodes.ENTER:
      this.addTag(value);
      break;
    case keyCodes.TAB:
      if (value) {
        preventDefault = true;
        this.addTag(value);
      }
      break;
    case keyCodes.HOME:
    case keyCodes.UP:
      this.moveCursorToTags(0);
      break;
    case keyCodes.END:
    case keyCodes.DOWN:
      this.setInputPosition(this.input.value.length);
      break;
    case keyCodes.LEFT:
      if (areTagsFocused) {
        this.moveCursorToTags(-1, { isRelative: true });
      } else if (this.input.selectionStart === 0) {
        this.moveCursorToTags(tags.size - 1);
      }
      break;
    case keyCodes.RIGHT:
      if (areTagsFocused) {
        this.moveCursorToTags(1, { isRelative: true });
      }
      //else do nothing. Default input behaviour is ok
      break;
    case keyCodes.BACKSPACE:
      if (areTagsFocused || this.input.selectionStart === 0) {
        if (selectedTagIndex >= 0) {
          this.removeTag(selectedTagIndex);
        } else {
          const pos = areTagsFocused ? tagCursorPosition : tags.size;
          if (pos > 0) {
            this.selectTag(pos - 1);
          }
        }
      }
      break;
    case keyCodes.DELETE:
      if (areTagsFocused) {
        if (selectedTagIndex >= 0) {
          this.removeTag(selectedTagIndex);
        } else {
          this.selectTag(tagCursorPosition);
        }
      }
      break;
    default:
      break;
    }

    if (preventDefault) {
      e.preventDefault();
    }
  }

  areTagsFocused() {
    return this.state.tagCursorPosition >= 0;
  }

  // this method manages a cursor position inside tags section
  // positionOrOffset - a cursor position that should be set or offset relative to current cursor position
  moveCursorToTags(positionOrOffset, {
      isRelative = false,
      resetTagSelection = true
    } = {}) {
    let focusInput = false;
    this.setState((
      /* state */ {
        tagCursorPosition
      },
      /* props */ {
        tags
      }) => {
      const cnt = tags.size;
      const cursorPosition = isRelative ? tagCursorPosition + positionOrOffset : positionOrOffset; // a cursor position to be set

      focusInput = cnt === 0 || cnt <= cursorPosition; // indidcates if an input should be selected after cursor position move. This may happen if position > number of tags. Or if there are no tags at all.

      const newState = {
        tagCursorPosition: focusInput ? -1 : Math.max(cursorPosition, 0)
      };

      if (resetTagSelection) {
        newState.selectedTagIndex = -1;
      }

      return newState;
    }, () => {
      if (focusInput) {
        // has to do it via timeout, as if do this synchroniously, then cursor is set into second position. Default input behaviour cause this
        setTimeout(() => {
          this.setInputPosition(0);
        }, 0);
      } else {
        this.input.blur();
        this.tagsContainer.focus();
      }
    });
  }

  selectTag = (index, moveCursor) => {
    this.setState({
      selectedTagIndex: index
    });
    if (moveCursor) {
      this.moveCursorToTags(index, { resetTagSelection: false });
    }
  }

  removeTag = (index) => {
    const {
      onRemoveTag,
      tags
    } = this.props;
    onRemoveTag(tags.get(index));
    this.moveCursorToTags(index);

    this.onChange();
  }

  onChange = () => {
    const {
      onChange
    } = this.props;

    if (onChange) {
      onChange();
    }
  }

  setInputPosition(position) {
    this.input.focus();
    this.input.selectionStart = this.input.selectionEnd = position;
  }

  focusInput = () => {
    this.setState({
      tagCursorPosition: -1
    });
  }

  onInputRef = (element) => {
    this.input = element;
  }

  onTagsRef = (element) => {
    this.tagsContainer = element;
  }

  render() {
    const {
      tags,
      placeholder
    } = this.props;
    const {
      value,
      selectedTagIndex,
      tagCursorPosition
    } = this.state;

    const tagElements = [];

    tags.forEach((t, i) => {
      // I have to wrap this element to a span, as for multiline case tags has to go on new line with cursor placeholder.
      // This will keep left padding consistent.
      tagElements.push(<span key={`w_${i}`} className={tagWrapper}>
        <div key={`cursor_${i}`}
          onClick={() => this.moveCursorToTags(i)}
          className={classNames(cursorPlaceholder, i === tagCursorPosition && cursor)}
          ></div>
        <Tag key={`tag_${i}`}
          className={tagElementCls}
          text={t}
          isSelected={i === selectedTagIndex}
          deleteHandler={() => this.removeTag(i)}
          onClick={() => this.selectTag(i, true)}
          />
      </span>);
    });

    return (
      <div className={container} tabIndex={0} onKeyDown={this.onKeyDown} ref={this.onTagsRef}>
        {tagElements}
        <input type='text'
          tabIndex={0}
          placeholder={placeholder || la('Add tag')}
          ref={this.onInputRef}
          className={inputCls}
          value={value}
          onChange={this.handleTextChange}
          maxLength={128} // restriction according to spec
          onFocus={this.focusInput} />
      </div>
    );
  }
}
