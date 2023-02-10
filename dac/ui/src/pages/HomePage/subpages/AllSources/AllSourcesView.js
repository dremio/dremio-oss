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
import { PureComponent } from "react";
import moment from "@app/utils/dayjs";
import PropTypes from "prop-types";
import { injectIntl } from "react-intl";
import clsx from "clsx";
import * as classes from "@app/uiTheme/radium/replacingRadiumPseudoClasses.module.less";

import EntityLink from "@app/pages/HomePage/components/EntityLink";
import EllipsedText from "components/EllipsedText";

import LinkButton from "components/Buttons/LinkButton";
import ResourcePin from "components/ResourcePin";
import AllSourcesMenu from "components/Menus/HomePage/AllSourcesMenu";
import FontIcon from "components/Icon/FontIcon";

import SettingsBtn from "components/Buttons/SettingsBtn";
import { SortDirection } from "@app/components/Table/TableUtils";
import { getIconStatusDatabase } from "utils/iconUtils";

import * as allSpacesAndAllSources from "uiTheme/radium/allSpacesAndAllSources";
import BrowseTable from "../../components/BrowseTable";
import { tableStyles } from "../../tableStyles";
import { getSettingsLocation } from "components/Menus/HomePage/AllSourcesMenu";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
import { IconButton } from "dremio-ui-lib";

const btnTypes = {
  settings: "settings"
};

class AllSourcesView extends PureComponent {
  static propTypes = {
    sources: PropTypes.object,
    intl: PropTypes.object.isRequired,
    title: PropTypes.string.isRequired,
    isExternalSource: PropTypes.bool,
    isDataPlaneSource: PropTypes.bool,
    isObjectStorageSource: PropTypes.bool,
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
    loggedInUser: PropTypes.object.isRequired,
  };

  getTableData() {
    const [name, datasets, created, action] = this.getTableColumns();
    return this.props.sources
      .toList()
      .sort((a, b) => b.get("isActivePin") - a.get("isActivePin"))
      .map((item) => {
        const icon = (
          <FontIcon
            type={getIconStatusDatabase(
              item.getIn(["state", "status"]),
              item.get("type")
            )}
          />
        );
        return {
          // todo: loc
          rowClassName: item.get("name"),
          data: {
            [name.key]: {
              node: () => (
                <div style={allSpacesAndAllSources.listItem}>
                  {icon}
                  <EntityLink entityId={item.get("id")}>
                    <EllipsedText text={item.get("name")} />
                  </EntityLink>
                  <ResourcePin entityId={item.get("id")} />
                </div>
              ),
              value(sortDirection = null) {
                // todo: DRY
                if (!sortDirection) return item.get("name");
                const activePrefix =
                  sortDirection === SortDirection.ASC ? "a" : "z";
                const inactivePrefix =
                  sortDirection === SortDirection.ASC ? "z" : "a";
                return (
                  (item.get("isActivePin") ? activePrefix : inactivePrefix) +
                  item.get("name")
                );
              },
            },
            [datasets.key]: {
              node: () => item.get("numberOfDatasets"),
            },
            [created.key]: {
              node: () => moment(item.get("ctime")).format("MM/DD/YYYY"),
              value: new Date(item.get("ctime")),
            },
            [action.key]: {
              node: () => this.getActionCell(item)
            },
          },
        };
      });
  }

  getActionCell(item) {
    return <ActionWrap>{this.getActionCellButtons(item)}</ActionWrap>
  }

  getActionCellButtons(item) {
    const allBtns = [{
      label: this.getInlineIcon("interface/settings"),
      tooltip: "Common.Settings",
      link: getSettingsLocation(this.context.location, item),
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
        <AllSourcesMenu
          item={item}
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
      { key: "datasets", label: intl.formatMessage({ id: "Common.Datasets" }) },
      { key: "created", label: intl.formatMessage({ id: "Common.Created" }) },
      {
        key: "action",
        label: "",
        style: tableStyles.actionColumn,
        disableSort: true,
        width: 60,
      },
    ];
  }

  renderAddButton() {
    const { isExternalSource, isDataPlaneSource, isObjectStorageSource } =
      this.props;

    /*eslint no-nested-ternary: "off"*/
    const headerId = isExternalSource
      ? "Source.AddDatabaseSource"
      : isDataPlaneSource
      ? "Source.AddDataPlane"
      : isObjectStorageSource
      ? "Source.Add.Object.Storage"
      : "Source.Add.Metastore";

    return (
      this.context.loggedInUser.admin && (
        <LinkButton
          buttonStyle="primary"
          className={clsx(classes["primaryButtonPsuedoClasses"])}
          to={{
            ...this.context.location,
            state: {
              modal: "AddSourceModal",
              isExternalSource,
              isDataPlaneSource,
            },
          }}
          style={allSpacesAndAllSources.addButton}
        >
          {this.props.intl.formatMessage({ id: headerId })}
        </LinkButton>
      )
    );
  }

  render() {
    const { sources, title } = this.props;
    const totalItems = sources.size;
    return (
      <BrowseTable
        title={`${title} (${totalItems})`}
        tableData={this.getTableData()}
        columns={this.getTableColumns()}
        buttons={this.renderAddButton()}
        disableZebraStripes={true}
        rowHeight={40}
      />
    );
  }
}
export default injectIntl(AllSourcesView);

function ActionWrap({ children }) {
  return <span className="action-wrap">{children}</span>;
}
ActionWrap.propTypes = {
  children: PropTypes.node,
};
