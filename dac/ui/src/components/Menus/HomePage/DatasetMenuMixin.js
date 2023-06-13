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
import Menu from "components/Menus/Menu";
import MenuItem from "components/Menus/MenuItem";
import DividerHr from "components/Menus/DividerHr";
import MenuItemLink from "components/Menus/MenuItemLink";

import AnalyzeMenuItem from "components/Menus/HomePage/AnalyzeMenuItem";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import { abilities } from "utils/datasetUtils";
import { shouldUseNewDatasetNavigation } from "@app/utils/datasetNavigationUtils";
import { constructFullPath } from "@app/utils/pathUtils";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";

export default function (input) {
  Object.assign(input.prototype, {
    // eslint-disable-line no-restricted-properties
    getGraphLink() {
      return null;
    },

    newMenuDropdown() {
      const { entity, closeMenu, entityType, summaryDataset, openWikiDrawer } =
        this.props;

      const versionContext = getVersionContextFromId(entity.get("id"));
      const { type: refType, value: refValue } = versionContext ?? {};

      const { canRemoveFormat, canEdit, canMove, canDelete, isPhysical } =
        abilities(entity, entityType);

      const resourceId = entity.getIn(["fullPathList", 0]);
      const newFullPath = constructFullPath(entity.get("fullPathList"));

      return (
        <Menu>
          <MenuItemLink
            href={{
              pathname: sqlPaths.sqlEditor.link(),
              search: `?context="${encodeURIComponent(
                resourceId
              )}"&queryPath=${newFullPath}`,
            }}
            text={la("Query")}
            closeMenu={closeMenu}
          />
          {canEdit && (
            <MenuItemLink
              href={wrapBackendLink(
                `${entity.getIn(["links", "edit"])}${
                  refType && refValue
                    ? `&refType=${refType}&refValue=${refValue}`
                    : ""
                }`
              )}
              text={la("Edit")}
            />
          )}
          {isPhysical && (
            <MenuItemLink
              href={wrapBackendLink(entity.getIn(["links", "query"]))}
              text={la("Go to Table")}
              closeMenu={closeMenu}
            />
          )}
          {summaryDataset && (
            <MenuItem
              onClick={() => {
                closeMenu();
                openWikiDrawer(summaryDataset);
              }}
            >
              {la("Open Details Panel")}
            </MenuItem>
          )}
          <AnalyzeMenuItem entity={entity} closeMenu={closeMenu} />
          <DividerHr />
          {canDelete &&
            ((entityType !== "file" && (
              <MenuItemLink
                closeMenu={closeMenu}
                href={this.getRemoveLocation()}
                text={la("Remove")}
              />
            )) ||
              (entityType === "file" && (
                <MenuItem onClick={this.handleRemoveFile}>
                  {la("Remove")}
                </MenuItem>
              )))}
          {canMove && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getRenameLocation()}
              text={la("Rename")}
            />
          )}
          {canMove && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getMoveLocation()}
              text={la("Move")}
            />
          )}
          <MenuItem onClick={this.copyPath}>{la("Copy Path")}</MenuItem>
          <DividerHr />
          <MenuItemLink
            closeMenu={closeMenu}
            href={this.getSettingsLocation()}
            text={la("Settings")}
          />
          {canRemoveFormat && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getRemoveFormatLocation()}
              text={la("Remove Format")}
            />
          )}
        </Menu>
      );
    },

    oldMenuDropdown() {
      const { entity, closeMenu, entityType } = this.props;

      const { canRemoveFormat, canEdit, canMove, canDelete } = abilities(
        entity,
        entityType
      );

      return (
        <Menu>
          {
            <MenuItemLink
              href={wrapBackendLink(entity.getIn(["links", "query"]))}
              text={la("Query")}
              closeMenu={closeMenu}
            />
          }

          {
            // feature has a bug see DX-7054
            /*
          entityType === 'folder' && <MenuItemLink
            text={la('Browse Contents')}
            href={entity.getIn(['links', 'self'])}
            closeMenu={closeMenu} />
          */
          }

          {canEdit && (
            <MenuItemLink
              href={wrapBackendLink(entity.getIn(["links", "edit"]))}
              text={la("Edit")}
            />
          )}

          {
            <MenuItemLink
              href={this.getMenuItemUrl("wiki")}
              text={la("Details")}
            />
          }

          {
            // EE has data graph menu item here
          }

          {<AnalyzeMenuItem entity={entity} closeMenu={closeMenu} />}

          <DividerHr />

          {canDelete &&
            ((entityType !== "file" && (
              <MenuItemLink
                closeMenu={closeMenu}
                href={this.getRemoveLocation()}
                text={la("Remove")}
              />
            )) ||
              (entityType === "file" && (
                <MenuItem onClick={this.handleRemoveFile}>
                  {la("Remove")}
                </MenuItem>
              )))}

          {canMove && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getRenameLocation()}
              text={la("Rename")}
            />
          )}

          {canMove && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getMoveLocation()}
              text={la("Move")}
            />
          )}

          <MenuItem onClick={this.copyPath}>{la("Copy Path")}</MenuItem>

          <DividerHr />

          <MenuItemLink
            closeMenu={closeMenu}
            href={this.getSettingsLocation()}
            text={la("Settings")}
          />

          {canRemoveFormat && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getRemoveFormatLocation()}
              text={la("Remove Format")}
            />
          )}
        </Menu>
      );
    },

    render() {
      return shouldUseNewDatasetNavigation()
        ? this.newMenuDropdown()
        : this.oldMenuDropdown();
    },
  });
}
