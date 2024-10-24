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
import { shouldUseNewDatasetNavigation } from "#oss/utils/datasetNavigationUtils";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

export default function (input) {
  Object.assign(input.prototype, {
    getGraphLink() {
      return null;
    },

    newMenuDropdown() {
      const { t } = getIntlContext();
      const { entity, closeMenu, entityType, openWikiDrawer } = this.props;

      const versionContext = getVersionContextFromId(entity.get("id"));
      const { type: refType, value: refValue } = versionContext ?? {};

      const { canRemoveFormat, canEdit, canMove, canDelete, isPhysical } =
        abilities(entity, entityType);

      const resourceId = entity.getIn(["fullPathList", 0]);
      const newFullPath = JSON.stringify(entity.get("fullPathList").toJS());

      return (
        <Menu>
          <MenuItemLink
            href={{
              pathname: sqlPaths.sqlEditor.link(),
              search: `?context="${encodeURIComponent(
                resourceId,
              )}"&queryPath=${encodeURIComponent(newFullPath)}`,
            }}
            text={t("Dataset.Actions.Query")}
            closeMenu={closeMenu}
          />
          {canEdit && (
            <MenuItemLink
              href={wrapBackendLink(
                `${entity.getIn(["links", "edit"])}${
                  refType && refValue
                    ? `&refType=${refType}&refValue=${refValue}`
                    : ""
                }`,
              )}
              text={t("Common.Actions.Edit")}
            />
          )}
          {isPhysical && (
            <MenuItemLink
              href={wrapBackendLink(
                `${entity.getIn(["links", "query"])}${
                  refType && refValue && resourceId
                    ? `?refType=${refType}&refValue=${refValue}&sourceName=${resourceId}`
                    : ""
                }`,
              )}
              text={t("Dataset.Actions.GoTo.Table")}
              closeMenu={closeMenu}
            />
          )}
          <MenuItem
            onClick={() => {
              closeMenu();
              openWikiDrawer(entity);
            }}
            disabled={!entity}
          >
            {t("Dataset.Actions.OpenDetails")}
          </MenuItem>
          <AnalyzeMenuItem entity={entity} closeMenu={closeMenu} />
          <DividerHr />
          {canMove && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getRenameLocation()}
              text={t("Common.Actions.Rename")}
            />
          )}
          {canMove && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getMoveLocation()}
              text={t("Common.Actions.Move")}
            />
          )}
          <MenuItem onClick={this.copyPath}>
            {t("Common.Actions.CopyPath")}
          </MenuItem>
          <MenuItemLink
            closeMenu={closeMenu}
            href={this.getSettingsLocation()}
            text={t("Common.Settings")}
          />
          {(canRemoveFormat || canDelete) && <DividerHr />}
          {canRemoveFormat && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getRemoveFormatLocation()}
              text={t("Dataset.Actions.RemoveFormat")}
            />
          )}
          {canDelete && (
            <>
              {(entityType !== "file" && (
                <MenuItemLink
                  className="danger"
                  closeMenu={closeMenu}
                  href={this.getRemoveLocation()}
                  text={t("Common.Actions.Delete")}
                />
              )) ||
                (entityType === "file" && (
                  <MenuItem onClick={this.handleRemoveFile} className="danger">
                    {t("Common.Actions.Delete")}
                  </MenuItem>
                ))}
            </>
          )}
        </Menu>
      );
    },

    oldMenuDropdown() {
      const { t } = getIntlContext();
      const { entity, closeMenu, entityType } = this.props;

      const resourceId = entity.getIn(["fullPathList", 0]);
      const newFullPath = JSON.stringify(entity.get("fullPathList").toJS());

      const { canRemoveFormat, canEdit, canMove, canDelete } = abilities(
        entity,
        entityType,
      );

      return (
        <Menu>
          {
            <MenuItemLink
              href={{
                pathname: sqlPaths.sqlEditor.link(),
                search: `?context="${encodeURIComponent(
                  resourceId,
                )}"&queryPath=${encodeURIComponent(newFullPath)}`,
              }}
              text={t("Dataset.Actions.Query")}
              closeMenu={closeMenu}
            />
          }

          {
            // feature has a bug see DX-7054
            /*
          entityType === 'folder' && <MenuItemLink
            text={laDeprecated('Browse Contents')}
            href={entity.getIn(['links', 'self'])}
            closeMenu={closeMenu} />
          */
          }

          {canEdit && (
            <MenuItemLink
              href={wrapBackendLink(entity.getIn(["links", "edit"]))}
              text={t("Common.Actions.Edit")}
            />
          )}

          {
            <MenuItemLink
              href={this.getMenuItemUrl("wiki")}
              text={t("Common.Actions.Details")}
            />
          }

          {
            // EE has data graph menu item here
          }

          {<AnalyzeMenuItem entity={entity} closeMenu={closeMenu} />}

          <DividerHr />

          {canMove && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getRenameLocation()}
              text={t("Common.Actions.Rename")}
            />
          )}

          {canMove && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getMoveLocation()}
              text={t("Common.Actions.Move")}
            />
          )}

          <MenuItem onClick={this.copyPath}>
            {t("Common.Actions.CopyPath")}
          </MenuItem>

          <MenuItemLink
            closeMenu={closeMenu}
            href={this.getSettingsLocation()}
            text={t("Common.Settings")}
          />

          {(canRemoveFormat || canDelete) && <DividerHr />}

          {canRemoveFormat && (
            <MenuItemLink
              closeMenu={closeMenu}
              href={this.getRemoveFormatLocation()}
              text={t("Dataset.Actions.RemoveFormat")}
            />
          )}

          {canDelete && (
            <>
              {(entityType !== "file" && (
                <MenuItemLink
                  className="danger"
                  closeMenu={closeMenu}
                  href={this.getRemoveLocation()}
                  text={t("Common.Actions.Delete")}
                />
              )) ||
                (entityType === "file" && (
                  <MenuItem onClick={this.handleRemoveFile} className="danger">
                    {t("Common.Actions.Delete")}
                  </MenuItem>
                ))}
            </>
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
