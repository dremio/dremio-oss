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
import MenuItemLink from "components/Menus/MenuItemLink";
import DividerHr from "components/Menus/DividerHr";
import { EntityLinkProvider } from "@app/pages/HomePage/components/EntityLink";
import { FormattedMessage } from "react-intl";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

export default function (input) {
  Object.assign(input.prototype, {
    // eslint-disable-line no-restricted-properties
    render() {
      const { t } = getIntlContext();
      const { spaceId, closeMenu } = this.props;
      const { location } = this.context;
      return (
        <Menu>
          {
            <EntityLinkProvider entityId={spaceId}>
              {(link) => (
                <MenuItemLink
                  href={link}
                  text={<FormattedMessage id="Dataset.Browse" />}
                  closeMenu={closeMenu}
                />
              )}
            </EntityLinkProvider>
          }
          {
            <MenuItemLink
              href={{
                ...location,
                state: { modal: "SpaceModal", entityId: spaceId },
              }}
              text={<FormattedMessage id="Common.Settings" />}
              closeMenu={closeMenu}
            />
          }
          {<DividerHr />}
          {
            <MenuItem onClick={this.handleRemoveSpace} className="danger">
              {<span>{t("Common.Actions.Delete")}</span>}
            </MenuItem>
          }
        </Menu>
      );
    },
  });
}
