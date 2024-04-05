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

import { getLocation } from "@app/selectors/routing";
import config from "@app/utils/config";

import Menu from "components/Menus/Menu";
import MenuItemLink from "components/Menus/MenuItemLink";
import MenuItem from "components/Menus/MenuItem";
import SideNavHelpExtra from "@inject/components/SideNav/SideNavHelpExtra";
import SideNavHelpCopyright from "@inject/components/SideNav/SideNavHelpCopyright";
import { menuListStyle } from "@app/components/SideNav/SideNavConstants";
import { FeatureSwitch } from "@app/exports/components/FeatureSwitch/FeatureSwitch";
import { PRODUCT_TUTORIALS } from "@inject/featureFlags/flags/PRODUCT_TUTORIALS";
import { useTutorialController } from "dremio-ui-common/walkthrough/TutorialController";
import { useIsOrgCreator } from "@inject/utils/orgUtils";

type HelpMenuProps = {
  closeMenu: () => void;
  location: any;
  organizationLanding: boolean;
};

const HelpMenu = ({
  closeMenu,
  location,
  organizationLanding,
}: HelpMenuProps) => {
  const intl = useIntl();
  const externalLink = (
    <span className={"externalLinkIcon dremioIcon-External-link"}></span>
  );
  const { isTutorialHidden, hideTutorial } = useTutorialController();
  const isOrgCreator = useIsOrgCreator?.();

  return (
    <Menu style={menuListStyle}>
      <MenuItemLink
        href={intl.formatMessage({ id: "SideNav.HelpDocUrl" })}
        external
        newWindow
        closeMenu={closeMenu}
        text={intl.formatMessage({ id: "SideNav.HelpDoc" })}
        rightIcon={externalLink}
      />
      {config.displayTutorialsLink && (
        <MenuItemLink
          href={intl.formatMessage({ id: "SideNav.TutorialsUrl" })}
          external
          newWindow
          closeMenu={closeMenu}
          text={intl.formatMessage({ id: "SideNav.Tutorials" })}
          rightIcon={externalLink}
        />
      )}
      <MenuItemLink
        href={intl.formatMessage({ id: "SideNav.CommunityUrl" })}
        external
        newWindow
        closeMenu={closeMenu}
        text={intl.formatMessage({ id: "SideNav.CommunitySite" })}
        rightIcon={externalLink}
      />
      {/* @ts-ignore */}
      {SideNavHelpExtra && <SideNavHelpExtra closeMenu={closeMenu} />}
      {organizationLanding && isOrgCreator && isTutorialHidden && (
        <FeatureSwitch
          flag={PRODUCT_TUTORIALS}
          renderEnabled={() => (
            <MenuItem
              onClick={() => {
                hideTutorial(false);
                closeMenu();
              }}
            >
              {intl.formatMessage({ id: "SideNav.GetStartedSonarTutorial" })}
            </MenuItem>
          )}
        />
      )}
      {/* This will render the support dialogue in OSS/ENT and Copyright for DCS*/}
      {SideNavHelpCopyright() ? (
        <SideNavHelpCopyright />
      ) : (
        <MenuItemLink
          href={{ ...location, state: { modal: "AboutModal" } }}
          closeMenu={closeMenu}
          text={intl.formatMessage({ id: "App.AboutHeading" })}
        />
      )}
    </Menu>
  );
};

const mapStateToProps = (state: any) => ({
  location: getLocation(state),
});

export default connect(mapStateToProps, {})(HelpMenu);
