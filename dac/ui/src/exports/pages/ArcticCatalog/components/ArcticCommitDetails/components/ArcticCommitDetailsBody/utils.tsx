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

import { Tag } from "@app/services/nessie/client/index";
import { Reference } from "@app/types/nessie";
// @ts-ignore
import { IconButton } from "dremio-ui-lib";

import * as classes from "./ArcticCommitDetailsBody.module.less";

export const getLabeledTags = (
  tags: ({ type: "TAG" } & Tag)[],
  handleDeleteTag: (ref: Reference) => void
) => {
  return tags.map((tag) => (
    <span key={tag.name} className={classes["tag-name"]}>
      <dremio-icon name="vcs/tag" class={classes["tag-name__icon"]} />
      <span className={classes["tag-name__name"]}>{tag.name}</span>
      <IconButton
        tooltip="ArcticCatalog.Tags.DeleteTag"
        className={classes["tag-name__icon"]}
        onClick={() => handleDeleteTag(tag)}
      >
        <dremio-icon
          name="interface/close-small"
          class={classes["tag-name__icon"]}
        />
      </IconButton>
    </span>
  ));
};
