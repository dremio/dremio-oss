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
import { connect } from "react-redux";
import { useIntl } from "react-intl";
import { Link } from "react-router";
import { useRef } from "react";

import { getLocation } from "@app/selectors/routing";
import config from "@app/utils/config";

import SideNavHelpExtra from "@inject/components/SideNav/SideNavHelpExtra";
import SideNavHelpCopyright from "@inject/components/SideNav/SideNavHelpCopyright";
import { useTutorialController } from "dremio-ui-common/walkthrough/TutorialController";
import { useIsOrgCreator } from "@inject/utils/orgUtils";

import * as classes from "./HelpMenu.module.less";

type HelpMenuProps = {
  close: () => void;
  location: any;
  organizationLanding: boolean;
};

const HelpMenu = ({ close, location, organizationLanding }: HelpMenuProps) => {
  const intl = useIntl();
  const menuRef = useRef<HTMLUListElement | null>(null);
  const externalLink = (
    <span className={"externalLinkIcon dremioIcon-External-link"}></span>
  );
  const { isTutorialHidden, hideTutorial } = useTutorialController();
  const isOrgCreator = useIsOrgCreator?.();
  const showTutorial = organizationLanding && isOrgCreator && isTutorialHidden;
  const onBlur = (e: any) => {
    if (!menuRef.current?.contains(e.relatedTarget)) {
      close();
    }
  };

  return (
    <ul ref={menuRef} className={classes["help-menu"]}>
      <li tabIndex={-1}>
        <a
          target="_blank"
          rel="noreferrer"
          href={intl.formatMessage({ id: "SideNav.HelpDocUrl" })}
          className={classes["help-menu-item"]}
          onClick={() => close()}
        >
          {intl.formatMessage({ id: "SideNav.HelpDoc" })}
          {externalLink}
        </a>
      </li>
      {config.displayTutorialsLink && (
        <li tabIndex={-1}>
          <a
            target="_blank"
            rel="noreferrer"
            href={intl.formatMessage({ id: "SideNav.TutorialsUrl" })}
            className={classes["help-menu-item"]}
            onClick={() => close()}
          >
            {intl.formatMessage({ id: "SideNav.Tutorials" })}
            {externalLink}
          </a>
        </li>
      )}
      <li tabIndex={-1}>
        <a
          target="_blank"
          rel="noreferrer"
          href={intl.formatMessage({ id: "SideNav.CommunityUrl" })}
          className={classes["help-menu-item"]}
          onClick={() => close()}
        >
          {intl.formatMessage({ id: "SideNav.CommunitySite" })}
          {externalLink}
        </a>
      </li>
      {/* @ts-ignore */}
      {SideNavHelpExtra && (
        <SideNavHelpExtra
          close={close}
          className={classes["help-menu-item"]}
          onBlur={SideNavHelpCopyright() && !showTutorial ? onBlur : undefined}
        />
      )}
      {showTutorial && (
        <li
          tabIndex={0}
          onClick={() => {
            hideTutorial(false);
            close();
          }}
          onKeyPress={(e) => {
            if (e.code === "Enter" || e.code === "Space") {
              hideTutorial(false);
              close();
            }
          }}
          className={classes["help-menu-item"]}
          {...(SideNavHelpCopyright() && { onBlur: onBlur })}
        >
          {intl.formatMessage({ id: "SideNav.GetStartedSonarTutorial" })}
        </li>
      )}
      {/* This will render the support dialogue in OSS/ENT and Copyright for DCS*/}
      {SideNavHelpCopyright() ? (
        <SideNavHelpCopyright />
      ) : (
        <li>
          <Link
            to={{ ...location, state: { modal: "AboutModal" } }}
            onClick={() => close()}
            onBlur={onBlur}
            className={classes["help-menu-item"]}
          >
            {intl.formatMessage({ id: "App.AboutHeading" })}
          </Link>
        </li>
      )}
    </ul>
  );
};

const mapStateToProps = (state: any) => ({
  location: getLocation(state),
});

export default connect(mapStateToProps, {})(HelpMenu);
