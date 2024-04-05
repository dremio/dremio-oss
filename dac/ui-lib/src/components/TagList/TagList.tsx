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
import React, { useState } from "react";
import { default as Tag } from "./Tag";
import { default as Tooltip } from "../Tooltip";

import "./TagList.scss";

type TagListViewTypes = {
  tags: any;
  visibleTagsNumber?: any;
  onTagClick?: any;
  className: string;
  style?: any;
  onMainContainerRef?: any;
};

const TagListView = ({
  tags,
  visibleTagsNumber,
  className,
  style,
  onMainContainerRef,
}: TagListViewTypes) => {
  const [showTooltip, setShowTooltip] = useState(false);

  const onHoverAwsTags = () => {
    setShowTooltip(true);
  };

  const handleRequestClose = () => {
    setShowTooltip(false);
  };

  const showModal = tags.size > visibleTagsNumber;
  const visible = showModal ? tags.slice(0, visibleTagsNumber) : tags;
  const modalTags = showModal ? tags.slice(visibleTagsNumber) : null;
  const modalTagsTooltip = modalTags?.toJS()?.join(",\n");
  const tagPopover = (
    <div className={"tagPopover"} onClick={handleRequestClose}>
      <div className={"tagEntry"}>{modalTagsTooltip}</div>
    </div>
  );

  return (
    <div ref={onMainContainerRef} className={className} style={style}>
      {visible.map((tag) => (
        <Tag key={tag} text={tag} />
      ))}
      {modalTags && (
        <Tooltip open={showTooltip} title={tagPopover} interactive>
          <div className="link awsTagExtra">
            <div
              onMouseEnter={onHoverAwsTags}
              onMouseLeave={handleRequestClose}
            >
              &nbsp;+{modalTags.size}
            </div>
          </div>
        </Tooltip>
      )}
    </div>
  );
};

// supports calculation of visible tags count depending on component width.
type TagListTypes = {
  tags: any;
  onTagClick?: any;
  className: string;
  style?: any;
  maxWidth?: number;
};
const TagList = (props: TagListTypes) => {
  const [el, setEl] = useState({ clientWidth: null });

  const onRef = (el) => {
    setEl(el);
  };

  const countVisibleTags = () => {
    const { tags, maxWidth } = props;
    //These constants are used only in this method and are mostly based on tagClass css
    const MAX_TAG_WIDTH = 100,
      DEFAULT_TAGS_WIDTH_PX = 800,
      PX_PER_CHAR = 7,
      TAG_PADDING_PX = 22,
      MIN_TAG_WIDTH_PX = 35; // width for '...' button

    let remainingWidth = maxWidth || el.clientWidth || DEFAULT_TAGS_WIDTH_PX;

    const totalCount = tags.size;
    let i;

    for (i = 0; i < totalCount; i++) {
      const tag = tags.get(i);
      const currentTagWidth = Math.min(
        tag.length * PX_PER_CHAR + TAG_PADDING_PX,
        MAX_TAG_WIDTH,
      );

      if (currentTagWidth > remainingWidth) {
        // no more space
        if (remainingWidth >= MIN_TAG_WIDTH_PX) {
          //there is enough space to show '...'
          return i;
        }
        // not enough space for '...' button, so do not include current tag in result. I assume that
        // current tag has width more than '...' button
        return i === 0 ? 0 : i - 1;
      }

      remainingWidth -= currentTagWidth;
    }

    // all tags could be displayed
    return totalCount;
  };

  return (
    <TagListView
      onMainContainerRef={onRef}
      {...props}
      visibleTagsNumber={countVisibleTags()}
    />
  );
};

export default TagList;
