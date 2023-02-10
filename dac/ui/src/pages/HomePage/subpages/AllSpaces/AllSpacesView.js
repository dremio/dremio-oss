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
import { PureComponent, Fragment } from "react";
import { connect } from "react-redux";
import moment from "@app/utils/dayjs";
import PropTypes from "prop-types";
import { injectIntl } from "react-intl";
import clsx from "clsx";
import { IconButton } from "dremio-ui-lib";
import * as classes from "@app/uiTheme/radium/replacingRadiumPseudoClasses.module.less";

import EntityLink from "@app/pages/HomePage/components/EntityLink";
import { RestrictedArea } from "@app/components/Auth/RestrictedArea";
import { manageSpaceRule } from "@app/utils/authUtils";
import { EntityIcon } from "@app/pages/HomePage/components/EntityIcon";
import { EntityName } from "@app/pages/HomePage/components/EntityName";

import * as allSpacesAndAllSources from "uiTheme/radium/allSpacesAndAllSources";

import { getSpaces, getNameFromRootEntity } from "selectors/home";

import LinkButton from "components/Buttons/LinkButton";
import ResourcePin from "components/ResourcePin";
import AllSpacesMenu from "components/Menus/HomePage/AllSpacesMenu";

import SettingsBtn from "components/Buttons/SettingsBtn";
import { SortDirection } from "@app/components/Table/TableUtils";

import BrowseTable from "../../components/BrowseTable";
import { tableStyles } from "../../tableStyles";
import { getSettingsLocation } from "components/Menus/HomePage/AllSpacesMenu";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";

const mapStateToProps = (state) => ({
  spaces: getSpaces(state),
});

const btnTypes = {
  settings: "settings"
};

export class AllSpacesView extends PureComponent {
  static propTypes = {
    spaces: PropTypes.object,
    intl: PropTypes.object.isRequired,
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
  };

  getTableData = () => {
    const [name, created, action] = this.getTableColumns();
    return this.props.spaces
      .toList()
      .sort((a, b) => b.get("isActivePin") - a.get("isActivePin"))
      .map((item) => {
        const entityId = item.get("id");
        const itemName = getNameFromRootEntity(item);
        return {
          rowClassName: itemName,
          data: {
            [name.key]: {
              node: () => (
                <div style={allSpacesAndAllSources.listItem}>
                  <EntityIcon entityId={entityId} />
                  <EntityLink entityId={entityId}>
                    <EntityName entityId={entityId} />
                  </EntityLink>
                  <ResourcePin entityId={entityId} />
                </div>
              ),
              value(sortDirection = null) {
                // todo: DRY
                if (!sortDirection) return itemName;
                const activePrefix =
                  sortDirection === SortDirection.ASC ? "a" : "z";
                const inactivePrefix =
                  sortDirection === SortDirection.ASC ? "z" : "a";
                return (
                  (item.get("isActivePin") ? activePrefix : inactivePrefix) +
                  itemName
                );
              },
            },
            [created.key]: {
              node: () =>
                item.get("createdAt")
                  ? moment(item.get("createdAt")).format("MM/DD/YYYY")
                  : "—",
              value: () =>
                item.get("createdAt") ? new Date(item.get("createdAt")) : "",
            },
            [action.key]: {
              node: () => this.getActionCell(item)
            },
          },
        };
      });
  };

  getActionCell(item) {
    return <ActionWrap>{this.getActionCellButtons(item)}</ActionWrap>
  }

  getActionCellButtons(item) {
    const allBtns = [{
      label: this.getInlineIcon("interface/settings"),
      tooltip: "Common.Settings",
      link: getSettingsLocation(this.context.location, item.get("id")),
      type: btnTypes.settings
    }]
    return [
      ...allBtns
        // return rendered link buttons
        .map((btnType, index) => (
          <IconButton
            as={LinkWithRef}
            to={btnType.link}
            tooltip={btnType.tooltip}
            key={item.get("id") + index}
            className="main-settings-btn min-btn"
            data-qa={btnType.type}
          >
            {btnType.label}
          </IconButton>
        )),
      this.getSettingsBtnByType(
        <AllSpacesMenu
          spaceId={item.get("id")}
        />,
        item
      ),
    ];
  }

  handleSettingsClose(settingsWrap) {
    $(settingsWrap).parents("tr").removeClass("hovered");
  }

  handleSettingsOpen(settingsWrap) {
    $(settingsWrap).parents("tr").addClass("hovered");
  }

  getSettingsBtnByType(menu, item) {
    return (
      <SettingsBtn
        handleSettingsClose={this.handleSettingsClose.bind(this)}
        handleSettingsOpen={this.handleSettingsOpen.bind(this)}
        dataQa={item.get("name")}
        menu={menu}
        classStr="main-settings-btn min-btn catalog-btn"
        key={`${item.get("name")}-${item.get("id")}`}
        tooltip="Common.More"
        hideArrowIcon
      >
        {this.getInlineIcon("interface/more")}
      </SettingsBtn>
    );
  }

  getInlineIcon(icon) {
    return <dremio-icon name={icon} data-qa={icon} />;
  }

  getTableColumns() {
    const { intl } = this.props;
    return [
      {
        key: "name",
        label: intl.formatMessage({ id: "Common.Name" }),
        flexGrow: 1,
      },
      { 
        key: "created", 
        label: intl.formatMessage({ id: "Common.Created" })
      },
      {
        key: "action",
        label: "",
        style: tableStyles.actionColumn,
        disableSort: true,
        width: 60,
      },
    ];
  }

  render() {
    const { spaces, intl } = this.props;
    const numberOfSpaces = spaces ? spaces.size : 0;
    return (
      <Fragment>
        <BrowseTable
          title={`${intl.formatMessage({
            id: "Space.AllSpaces",
          })} (${numberOfSpaces})`}
          buttons={
            <RestrictedArea rule={manageSpaceRule}>
              <LinkButton
                buttonStyle="primary"
                className={clsx(classes["primaryButtonPsuedoClasses"])}
                to={{
                  ...this.context.location,
                  state: { modal: "SpaceModal" },
                }}
                style={allSpacesAndAllSources.addButton}
              >
                {this.props.intl.formatMessage({ id: "Space.AddSpace" })}
              </LinkButton>
            </RestrictedArea>
          }
          tableData={this.getTableData()}
          columns={this.getTableColumns()}
          disableZebraStripes={true}
          rowHeight={40}
        />
      </Fragment>
    );
  }
}
AllSpacesView = injectIntl(AllSpacesView);

function ActionWrap({ children }) {
  return <span className="action-wrap">{children}</span>;
}
ActionWrap.propTypes = {
  children: PropTypes.node,
};

export default connect(mapStateToProps)(AllSpacesView);
