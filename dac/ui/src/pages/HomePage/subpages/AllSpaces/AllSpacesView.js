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

import EntityLink from "@app/pages/HomePage/components/EntityLink";
import SpacesLoader from "@app/pages/HomePage/components/SpacesLoader";
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
import { SortDirection } from "components/VirtualizedTableViewer";

import BrowseTable from "../../components/BrowseTable";
import { tableStyles } from "../../tableStyles";

const mapStateToProps = (state) => ({
  spaces: getSpaces(state),
});

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
              node: () => (
                <span className="action-wrap">
                  <SettingsBtn
                    routeParams={this.context.location.query}
                    menu={<AllSpacesMenu spaceId={item.get("id")} />}
                    dataQa={itemName}
                  />
                </span>
              ),
            },
          },
        };
      });
  };

  getTableColumns() {
    const { intl } = this.props;
    return [
      {
        key: "name",
        label: intl.formatMessage({ id: "Common.Name" }),
        flexGrow: 1,
      },
      { key: "created", label: intl.formatMessage({ id: "Common.Created" }) },
      {
        key: "action",
        label: intl.formatMessage({ id: "Common.Action" }),
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
        <SpacesLoader />
        <BrowseTable
          title={`${intl.formatMessage({
            id: "Space.AllSpaces",
          })} (${numberOfSpaces})`}
          buttons={
            <RestrictedArea rule={manageSpaceRule}>
              <LinkButton
                buttonStyle="primary"
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
        />
      </Fragment>
    );
  }
}
AllSpacesView = injectIntl(AllSpacesView);

export default connect(mapStateToProps)(AllSpacesView);
