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

import { useRef } from "react";
import TagsModal from "pages/HomePage/components/modals/TagsModal/TagsModal";
import { Tag } from "#oss/pages/ExplorePage/components/TagsEditor/Tag";
import { useOverflowIndex } from "dremio-ui-lib/components";
import clsx from "clsx";

import classes from "./TagList.less";

export const TagList = (props) => {
  const { className, style, tags, onTagClick } = props;
  const parentRef = useRef(null);
  const [overflowIndex, overflowedElement] = useOverflowIndex(parentRef);
  return (
    <div
      className={clsx(className, classes["tag-list"])}
      style={style}
      ref={parentRef}
    >
      {tags.map((tag, i) => {
        return (
          <Tag
            onClick={onTagClick ? () => onTagClick(tag) : null}
            className={`${classes["tag"]} tag-list__tag`}
            text={tag}
            title
            key={tag}
            style={{
              ...(i >= overflowIndex ? { visibility: "hidden" } : {}),
            }}
          />
        );
      })}
      {overflowIndex && (
        <span
          style={{
            position: "absolute",
            left: overflowedElement.offsetLeft,
          }}
        >
          <TagsModal
            key="tagsModal"
            tags={tags.slice(overflowIndex).toJS()}
            onTagClick={onTagClick}
            mainTagClass={classes["tag"]}
          />
        </span>
      )}
    </div>
  );
};
