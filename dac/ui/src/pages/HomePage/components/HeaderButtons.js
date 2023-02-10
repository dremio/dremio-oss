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
import { Component } from "react";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { injectIntl } from "react-intl";
import { IconButton } from "dremio-ui-lib";

import DropdownMenu from "@app/components/Menus/DropdownMenu";
import { ENTITY_TYPES } from "@app/constants/Constants";

import HeaderButtonsMixin from "dyn-load/pages/HomePage/components/HeaderButtonsMixin";
import { RestrictedArea } from "@app/components/Auth/RestrictedArea";
import localStorageUtils from "utils/storageUtils/localStorageUtils";
import HeaderButtonAddActions from "./HeaderButtonAddActions.tsx";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import * as classes from "./HeaderButtonAddActions.module.less";

@HeaderButtonsMixin
export class HeaderButtons extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired,
  };

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    toggleVisibility: PropTypes.func.isRequired,
    rootEntityType: PropTypes.oneOf(Object.values(ENTITY_TYPES)),
    user: PropTypes.string,
    rightTreeVisible: PropTypes.bool,
    intl: PropTypes.object.isRequired,
    canUploadFile: PropTypes.bool,
    isVersionedSource: PropTypes.bool,
  };

  static defaultProps = {
    entity: Immutable.Map(),
  };

  getButtonsForEntityType(entityType) {
    switch (entityType) {
      case ENTITY_TYPES.space:
        return this.getSpaceSettingsButtons();
      case ENTITY_TYPES.source:
        return this.getSourceSettingsButtons().concat(this.getSourceButtons());
      default:
        return [];
    }
  }

  getSourceButtons() {
    const { entity, isVersionedSource } = this.props;
    const buttons = [];

    if (entity.get("isPhysicalDataset")) {
      buttons.push({
        qa: "query-folder",
        iconType: "navigation-bar/sql-runner",
        to: wrapBackendLink(entity.getIn(["links", "query"])),
      });
    } else if (
      !isVersionedSource &&
      entity.get("fileSystemFolder") &&
      (entity.getIn("permissions", "canEditFormatSettings") === true ||
        localStorageUtils.isUserAnAdmin())
    ) {
      buttons.push({
        qa: "convert-folder",
        iconType: "interface/format-folder",
        to: {
          ...this.context.location,
          state: {
            modal: "DatasetSettingsModal",
            tab: "format",
            entityType: entity.get("entityType"),
            entityId: entity.get("id"),
            query: { then: "query" },
            isHomePage: true,
          },
        },
      });
    }
    return buttons;
  }

  getIconAltText(iconType) {
    const messages = {
      "interface/format-folder": "Folder.FolderFormat",
      "navigation-bar/sql-runner": "Job.Query",
      "interface/settings": "Common.Settings",
    };
    const iconMessageId = messages[iconType];
    return iconMessageId
      ? this.props.intl.formatMessage({ id: iconMessageId })
      : "Type";
  }

  renderButton = (item, index) => {
    const { qa, to, iconType, style, iconStyle, authRule } = item;
    const iconAlt = this.getIconAltText(iconType);

    let link = (
      <IconButton
        as={LinkWithRef}
        className="button-white"
        data-qa={`${qa}-button`}
        to={to ? to : "."}
        key={`${iconType}-${index}`}
        style={style}
        tooltip={iconAlt}
      >
        <dremio-icon name={iconType} alt={iconAlt} style={iconStyle} />
      </IconButton>
    );

    if (authRule) {
      link = (
        <RestrictedArea key={`${iconType}-${index}-${index}`} rule={authRule}>
          {link}
        </RestrictedArea>
      );
    }

    return link;
  };

  canAddInHomeSpace(rootEntityType) {
    const { canUploadFile: cloudCanUploadFile } = this.props;

    const isHome = rootEntityType === ENTITY_TYPES.home;

    // software permission for uploading a file is stored in localstorage,
    // while the permission on cloud is stored in Redux
    const canUploadFile =
      localStorageUtils.getUserPermissions()?.canUploadFile ||
      cloudCanUploadFile;

    return isHome && canUploadFile;
  }

  shouldShowAddButton(rootEntityType, entity) {
    const { isVersionedSource } = this.props;
    const showAddButton =
      [ENTITY_TYPES.home, ENTITY_TYPES.space].includes(rootEntityType) ||
      isVersionedSource;

    const hasPermissionToAdd =
      localStorageUtils.isUserAnAdmin() ||
      entity.getIn(["permissions", "canEdit"]) ||
      entity.getIn(["permissions", "canCreateChildren"]) ||
      entity.getIn(["permissions", "canAlter"]);

    return hasPermissionToAdd && showAddButton;
  }

  render() {
    const { rootEntityType, entity, isVersionedSource } = this.props;

    const buttonsForCurrentPage = this.getButtonsForEntityType(rootEntityType);

    const shouldShowAddButton = this.shouldShowAddButton(
      rootEntityType,
      entity
    );

    const canAddInHomeSpace = this.canAddInHomeSpace(rootEntityType);

    const hasAddMenu = shouldShowAddButton || canAddInHomeSpace;

    return (
      <>
        {hasAddMenu && (
          <DropdownMenu
            customItemRenderer={
              <IconButton
                aria-label="Add"
                className={classes["headerButtons__plusIcon"]}
              >
                <dremio-icon name="interface/circle-plus"></dremio-icon>
              </IconButton>
            }
            hideArrow
            closeOnSelect
            menu={
              <HeaderButtonAddActions
                context={this.context}
                allowFileUpload={rootEntityType === ENTITY_TYPES.home}
                allowTable={isVersionedSource}
                canUploadFile={canAddInHomeSpace}
              />
            }
          />
        )}
        {buttonsForCurrentPage.map(this.renderButton)}
      </>
    );
  }
}
HeaderButtons = injectIntl(HeaderButtons);
