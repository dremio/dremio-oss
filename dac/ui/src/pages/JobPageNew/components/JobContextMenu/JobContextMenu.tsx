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

// @ts-ignore
import CopyToClipboard from "react-copy-to-clipboard";
import { useCallback, useEffect } from "react";
import MenuItemLink from "#oss/components/Menus/MenuItemLink";
import Menu from "components/Menus/Menu";
import MenuItem from "components/Menus/MenuItem";
import { addNotification } from "#oss/actions/notification";
import { useDispatch } from "react-redux";
import { MSG_CLEAR_DELAY_SEC } from "#oss/constants/Constants";
import { useIntl } from "react-intl";
import copy from "copy-to-clipboard";
import * as jobPaths from "dremio-ui-common/paths/jobs.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";

import * as classes from "./JobContextMenu.module.less";

type JobContextMenuProps = {
  context: any;
  onViewDetails: (rowData: any) => void;
  closeMenu: () => void;
};

const JobContextMenu = ({
  context,
  onViewDetails,
  closeMenu,
}: JobContextMenuProps) => {
  const dispatch = useDispatch();
  const intl = useIntl();
  const jobId = context?.rowData?.data?.job?.value;
  const copyShortcut =
    navigator.userAgent.indexOf("Mac OS X") !== -1 ? "⌘C" : "CTRL + C";

  const handleCopy = useCallback(() => {
    const message = (
      <span>
        Copied <i>{jobId}</i>.
      </span>
    );
    dispatch(addNotification(message, "success", MSG_CLEAR_DELAY_SEC));
    closeMenu();
  }, [closeMenu, dispatch, jobId]);

  const handleKeyPress = useCallback(
    (e: any) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "c") {
        copy(jobId);
        handleCopy();
      }
      if (e.key === "Escape") {
        closeMenu();
      }
    },
    [jobId, handleCopy, closeMenu],
  );

  useEffect(() => {
    document.addEventListener("keydown", handleKeyPress);
    return () => {
      document.removeEventListener("keydown", handleKeyPress);
    };
  }, [handleKeyPress]);

  const projectId = getSonarContext().getSelectedProjectId?.();

  return (
    <Menu style={{ width: 215 }}>
      <MenuItem
        onClick={() => {
          onViewDetails(context);
          closeMenu();
        }}
      >
        {intl.formatMessage({ id: "Job.ContextMenu.ViewDetails" })}
      </MenuItem>
      <MenuItemLink
        key="newTab"
        newWindow
        href={jobPaths.job.link({ jobId, projectId })}
        closeMenu={closeMenu}
        text={intl.formatMessage({ id: "Job.ContextMenu.NewTab" })}
      />
      <CopyToClipboard text={jobId} onCopy={handleCopy}>
        <MenuItem className={classes["keyboard-shortcut"]}>
          <>
            {intl.formatMessage({ id: "Job.ContextMenu.CopyId" })}
            <span className={classes["keyboard-shortcut__cmd"]}>
              {copyShortcut}
            </span>
          </>
        </MenuItem>
      </CopyToClipboard>
    </Menu>
  );
};

export default JobContextMenu;
