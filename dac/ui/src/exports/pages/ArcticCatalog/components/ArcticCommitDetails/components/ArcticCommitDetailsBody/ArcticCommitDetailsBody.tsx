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

import { useState } from "react";
import { FormattedMessage } from "react-intl";
// @ts-ignore
import { IconButton } from "dremio-ui-lib";
import { Avatar } from "dremio-ui-lib/dist-esm";
import { Reference } from "@app/types/nessie";
import { LogEntry, Tag } from "@app/services/nessie/client/index";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { getLabeledTags } from "./utils";
import NewTagDialog from "@app/pages/NessieHomePage/components/NewTagDialog/NewTagDialog";
import { convertISOStringWithTooltip } from "@app/pages/NessieHomePage/components/RepoView/components/RepoViewBody/components/RepoViewBranchList/utils";

import * as classes from "./ArcticCommitDetailsBody.module.less";

type ArcticCommitDetailsBodyProps = {
  references: any[];
  commitId: string;
  commitData?: LogEntry;
  refetch: (props: Record<string, unknown>) => void;
};

function ArcticCommitDetailsBody({
  references,
  commitId,
  commitData,
  refetch,
}: ArcticCommitDetailsBodyProps) {
  const [dialogState, setDialogState] = useState(false);
  const {
    state: { reference },
  } = useNessieContext();

  const openDialog = () => {
    setDialogState(true);
  };

  const closeDialog = () => {
    setDialogState(false);
  };

  const tags: ({ type: "TAG" } & Tag)[] = references;
  const commitMeta = commitData?.commitMeta;

  return (
    <>
      <div className={classes["commit-details-wrapper"]}>
        <div className={classes["commit-details-body"]}>
          <span className={classes["commit-details-body__section"]}>
            <FormattedMessage id="Common.Author" />
            <div className={classes["commit-details-body__section--subtext"]}>
              <Avatar
                initials={nameToInitials(commitMeta?.author ?? "")}
                className={classes["user-avatar"]}
              />
              {commitMeta?.author}
            </div>
          </span>
          <span className={classes["commit-details-body__section"]}>
            <FormattedMessage id="ArcticCatalog.Commits.Details.CommitTime" />
            <div className={classes["commit-details-body__section--subtext"]}>
              {convertISOStringWithTooltip(
                commitMeta?.commitTime?.toString() ?? "",
                { isRelative: true }
              )}
            </div>
          </span>
          <span
            className={`${classes["commit-details-body__section"]} ${classes["tags-section"]}`}
          >
            <FormattedMessage id="ArcticCatalog.Tags" />
            <div
              className={`${classes["commit-details-body__section--subtext"]} ${classes["icon-wrapper"]}`}
            >
              {tags.length ? (
                getLabeledTags(tags)
              ) : (
                <span className={classes["no-tag-placeholder"]}>
                  <FormattedMessage id="ArcticCatalog.Commits.Details.NoTags" />
                </span>
              )}
              <IconButton
                tooltip="ArcticCatalog.Tags.Dialog.AddTag"
                disabled={reference === null}
                onClick={openDialog}
                className={classes["icon-button"]}
              >
                <dremio-icon name="interface/add-small" />
              </IconButton>
            </div>
          </span>
        </div>
        <div>
          <span className={classes["commit-details-body__section"]}>
            <FormattedMessage id="Common.Description" />
            <div className={classes["commit-details-body__section--subtext"]}>
              {commitMeta?.message}
            </div>
          </span>
        </div>
      </div>
      <NewTagDialog
        open={dialogState}
        forkFrom={{ ...reference, hash: commitId } as Reference}
        closeDialog={closeDialog}
        refetch={() => refetch({ hash: commitId })}
      />
    </>
  );
}

export default ArcticCommitDetailsBody;
