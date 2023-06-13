/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { useRef, useState } from "react";
//@ts-ignore
import ImmutablePropTypes from "react-immutable-proptypes";
import classNames from "clsx";
import keyCodes from "@app/constants/Keys.json";
import * as classes from "./Labels.module.less";
import { Tag } from "@app/pages/ExplorePage/components/TagsEditor/Tag";
import { IconButton, TagList } from "dremio-ui-lib";
import { intl } from "@app/utils/intl";
import { useClickOutside } from "@mantine/hooks";

interface TagsViewProps {
  placeholder?: string;
  tags: ImmutablePropTypes<string>;
  className?: string;
  //if this handler is not provided, then input for new tags will not be displayed
  onAddTag?: (tagName: string) => void;
  // If this handler is not provided, then x button for tags will not be displayed
  onRemoveTag?: (tagName: string) => Promise<any>;
  onTagClick: (tagName: string) => void;
  // is called, when tag is added or removed
  onChange: () => void;
  isEditAllowed?: boolean;
}

const TagsView = ({
  tags,
  className,
  onAddTag,
  onRemoveTag,
  onTagClick: onTagClickProp,
  onChange: onChangeProp,
  isEditAllowed,
}: TagsViewProps) => {
  const [value, setValue] = useState<string>("");
  const [selectedTagIndex, setSelectedTagIndex] = useState<number>(-1);
  const [hasError, setHasError] = useState<boolean>(false);
  const [editEnabled, setEditEnabled] = useState<boolean>(false);
  const inputRef = useRef();
  const tagsContainerRef = useRef();
  const ref = useClickOutside(() => {
    setEditEnabled(false);
    setHasError(false);
  });

  const handleTextChange = (e: {
    stopPropagation: () => void;
    preventDefault: () => void;
    target: { value: any };
  }) => {
    e.stopPropagation();
    e.preventDefault();
    const val = e.target.value;
    setValue(val);
  };

  const addTag = (newTag: string) => {
    if (!newTag.trim()) return;

    const isDuplicateTag = tags.find(
      (tag: string) => tag.toLowerCase() === newTag.toLowerCase()
    );
    if (!isDuplicateTag) {
      if (onAddTag) onAddTag(newTag);
      onChange();
    }
    setValue("");
    setHasError(Boolean(isDuplicateTag));
  };

  const showInputField = () => !!onAddTag;

  const areTagsFocusedFunc = () => {
    return selectedTagIndex >= 0;
  };

  const onKeyDown = (e: { keyCode: any; preventDefault: () => void }) => {
    const areTagsFocused = areTagsFocusedFunc();
    let preventDefault = false; // indicates if we should cancel a default behaviour for current key

    switch (e.keyCode) {
      case keyCodes.ENTER:
        addTag(value);
        break;
      case keyCodes.TAB:
        if (value) {
          preventDefault = true;
          addTag(value);
        }
        break;
      case keyCodes.HOME:
      case keyCodes.UP:
        moveCursorToTags(0);
        break;
      case keyCodes.END:
      case keyCodes.DOWN:
        if (showInputField()) {
          setInputPosition(inputRef.current.value.length);
        } else {
          moveCursorToTags(tags.size - 1);
        }
        break;
      case keyCodes.LEFT:
        if (areTagsFocused) {
          moveCursorToTags(-1, { isRelative: true });
        } else if (showInputField() && inputRef.current.selectionStart === 0) {
          moveCursorToTags(tags.size - 1);
        }
        break;
      case keyCodes.RIGHT:
        if (areTagsFocused) {
          moveCursorToTags(1, { isRelative: true });
        }
        //else do nothing. Default input behaviour is ok
        break;
      case keyCodes.BACKSPACE:
        if (areTagsFocused) {
          removeTag(selectedTagIndex);
          // cursor in the begining of the input and nothing is selected
        } else if (
          showInputField() &&
          inputRef.current.selectionStart === 0 &&
          inputRef.current.selectionEnd === 0
        ) {
          moveCursorToTags(tags.size - 1);
        }
        break;
      case keyCodes.DELETE:
        if (areTagsFocused) {
          removeTag(selectedTagIndex, true);
        }
        break;
      default:
        break;
    }

    if (preventDefault) {
      e.preventDefault();
    }
  };

  // this method manages a cursor position inside tags section
  // positionOrOffset - a cursor position that should be set or offset relative to current cursor position
  const moveCursorToTags = async (
    positionOrOffset: number,
    { isRelative = false } = {}
  ) => {
    let focusInput = false;
    const cnt = tags.size;
    const cursorPosition = isRelative
      ? selectedTagIndex + positionOrOffset
      : positionOrOffset; // a cursor position to be set

    focusInput = showInputField() && (cnt === 0 || cnt <= cursorPosition); // indidcates if an input should be selected after cursor position move. This may happen if position > number of tags. Or if there are no tags at all.
    setSelectedTagIndex(
      focusInput ? -1 : Math.min(Math.max(cursorPosition, 0), cnt - 1)
    );
    if (focusInput) {
      // has to do it via timeout, as if do this synchroniously, then cursor is set into second position. Default input behaviour cause this
      setTimeout(() => {
        setInputPosition(0);
      }, 0);
    } else {
      if (showInputField()) {
        inputRef.current?.blur();
      }
      tagsContainerRef.current?.focus();
    }
  };

  const removeTag = async (
    index: number,
    selectNext?: boolean,
    e?: { stopPropagation: () => void; preventDefault: () => void } | undefined
  ) => {
    e?.stopPropagation();
    e?.preventDefault();
    if (onRemoveTag) await onRemoveTag(tags.get(index));
    moveCursorToTags(selectNext ? index : index - 1);
    setHasError(false);
    onChange();
  };

  const onChange = () => {
    if (onChangeProp) {
      onChangeProp();
    }
  };

  const setInputPosition = (position: number) => {
    if (!showInputField()) return;
    inputRef.current.focus();
    inputRef.current.selectionStart = inputRef.current.selectionEnd = position;
  };

  const focusInput = () => {
    setSelectedTagIndex(-1);
  };

  const onInputRef = (element: any) => {
    inputRef.current = element;
  };

  const onTagsRef = (element: any) => {
    tagsContainerRef.current = element;
  };

  const onTagClick = (tag: string) => {
    if (onTagClickProp) {
      onTagClickProp(tag);
    }
  };

  const onEditClick = (e: {
    stopPropagation: () => void;
    preventDefault: () => void;
  }) => {
    e.stopPropagation();
    e.preventDefault();
    setEditEnabled(true);
    setTimeout(() => {
      inputRef.current.focus();
    }, 300);
  };

  const tagElements: JSX.Element[] = [];

  tags.forEach((t: string, i: number) => {
    tagElements.push(
      <span key={`w_${i}`} className={classes["tagWrapper"]}>
        <Tag
          key={`tag_${i}`}
          className={classes["tagElement"]}
          text={t}
          daqa={`tag-pill-${t}`}
          deleteHandler={
            onRemoveTag && editEnabled
              ? (e: any) => removeTag(i, null, e)
              : null
          }
          onClick={() => onTagClick(t)}
        />
      </span>
    );
  });

  return (
    <div
      className={!editEnabled && tags.size === 0 ? "flex --alignCenter" : ""}
    >
      {(!isEditAllowed || !editEnabled) && tags.size > 0 ? (
        <TagList tags={tags} className={classes["tagSearch"]} />
      ) : (
        !editEnabled && (
          <div className={classes["noTags"]}>
            {intl.formatMessage({ id: "Wiki.NoLabel" })}
          </div>
        )
      )}

      <div
        className={classNames(
          editEnabled ? classes["container"] : "",
          className
        )}
        tabIndex={0}
        onKeyDown={onKeyDown}
        ref={onTagsRef}
        data-qa="tagsContainer"
      >
        <div ref={ref}>
          {isEditAllowed && editEnabled && tagElements}
          {isEditAllowed && !editEnabled && (
            <IconButton
              tooltip="Common.Edit"
              onClick={onEditClick}
              className={classNames("gutter-left", classes["editTag"])}
            >
              <dremio-icon
                name="interface/edit"
                class={classes["editTagsIcon"]}
              />
            </IconButton>
          )}
          {showInputField() && editEnabled && (
            <input
              type="text"
              tabIndex={0}
              ref={onInputRef}
              className={classes["input"]}
              value={value}
              onChange={handleTextChange}
              maxLength={128} // restriction according to spec
              onFocus={focusInput}
            />
          )}
        </div>
      </div>

      {showInputField() && hasError && (
        <p className={classes["uniqueTagsError"]}>
          {intl.formatMessage({ id: "Catalog.Tags.UniqueTags" })}
        </p>
      )}
    </div>
  );
};

export default TagsView;
