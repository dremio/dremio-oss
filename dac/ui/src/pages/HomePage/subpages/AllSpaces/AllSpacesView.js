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
import { connect } from "react-redux";
import PropTypes from "prop-types";
import { browserHistory } from "react-router";
import Immutable from "immutable";

import moment from "@app/utils/dayjs";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
import EntityLink from "@app/pages/HomePage/components/EntityLink";
import AllSpacesMenu from "components/Menus/HomePage/AllSpacesMenu";
import SettingsBtn from "components/Buttons/SettingsBtn";
import BreadCrumbs from "@app/components/BreadCrumbs";
import CatalogListingView from "../../components/CatalogListingView/CatalogListingView";
import EllipsedText from "@app/components/EllipsedText";
import { RestrictedArea } from "@app/components/Auth/RestrictedArea";
import { PureEntityIcon } from "@app/pages/HomePage/components/EntityIcon";
import { IconButton } from "dremio-ui-lib/components";
import { Button } from "dremio-ui-lib/components";
import { EntityName } from "@app/pages/HomePage/components/EntityName";

import {
  catalogListingColumns,
  CATALOG_LISTING_COLUMNS,
} from "dremio-ui-common/sonar/components/CatalogListingTable/catalogListingColumns.js";
import { manageSpaceRule } from "@app/utils/authUtils";
import { getSettingsLocation } from "components/Menus/HomePage/AllSpacesMenu";
import { getSpaces } from "selectors/home";
import { ENTITY_TYPES } from "@app/constants/Constants";
import { intl } from "@app/utils/intl";
import { getCatalogData } from "@app/utils/catalog-listing-utils";
import { renderResourcePin } from "@inject/utils/catalog-listing-utils";
import CatalogDetailsPanel from "../../components/CatalogDetailsPanel/CatalogDetailsPanel";

const mapStateToProps = (state) => ({
  spaces: getSpaces(state),
});

const btnTypes = {
  settings: "settings",
};

const SPACES_SORTING_MAP = {
  [CATALOG_LISTING_COLUMNS.name]: "name",
  [CATALOG_LISTING_COLUMNS.created]: "createdAt",
};

export class AllSpacesView extends PureComponent {
  static propTypes = {
    spaces: PropTypes.object,
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
  };

  state = {
    sort: null,
    filter: "",
    datasetDetails: null,
  };

  getRow(item) {
    const entityId = item.get("id");
    return {
      id: item.get("id"),
      data: {
        name: (
          <div style={{ gap: 4, display: "flex", alignItems: "center" }}>
            <PureEntityIcon entityType={ENTITY_TYPES.space} />
            <EntityLink entityId={entityId}>
              <EntityName entityId={entityId} />
            </EntityLink>
            {renderResourcePin(entityId)}
          </div>
        ),
        created: moment(new Date(item.get("createdAt"))).format("MM/DD/YYYY"),
        actions: this.getActionCell(item),
      },
    };
  }

  getActionCell(item) {
    return <ActionWrap>{this.getActionCellButtons(item)}</ActionWrap>;
  }

  getActionCellButtons(item) {
    const allBtns = [
      {
        label: this.getInlineIcon("interface/settings"),
        tooltip: intl.formatMessage({ id: "Common.Settings" }),
        link: getSettingsLocation(this.context.location, item.get("id")),
        type: btnTypes.settings,
      },
    ];
    return [
      <IconButton
        key={item.get("id")}
        tooltip="Open details panel"
        onClick={(e) => {
          e.preventDefault();
          this.openDetailsPanel(item, true);
        }}
        tooltipPortal
        tooltipPlacement="top"
        className="main-settings-btn min-btn catalog-btn"
      >
        <dremio-icon name="interface/meta" />
      </IconButton>,
      ...allBtns
        // return rendered link buttons
        .map((btnType, index) => (
          <IconButton
            as={LinkWithRef}
            to={btnType.link}
            tooltip={btnType.tooltip}
            tooltipPortal
            key={item.get("id") + index}
            className="main-settings-btn min-btn"
            data-qa={btnType.type}
          >
            {btnType.label}
          </IconButton>
        )),
      this.getSettingsBtnByType(
        <AllSpacesMenu spaceId={item.get("id")} />,
        item,
      ),
    ];
  }

  openDetailsPanel = (dataset) => {
    this.setState({
      datasetDetails: dataset,
    });
  };

  closeDetailsPanel = () => {
    this.setState({
      datasetDetails: null,
    });
  };

  getSettingsBtnByType(menu, item) {
    return (
      <SettingsBtn
        dataQa={item.get("name")}
        menu={menu}
        classStr="main-settings-btn min-btn catalog-btn"
        key={`${item.get("name")}-${item.get("id")}`}
        tooltip={intl.formatMessage({ id: "Common.More" })}
        hideArrowIcon
      >
        {this.getInlineIcon("interface/more")}
      </SettingsBtn>
    );
  }

  getInlineIcon(icon) {
    return <dremio-icon name={icon} data-qa={icon} />;
  }

  onColumnsSorted = (sortedColumns) => {
    this.setState({ sort: sortedColumns });
  };

  renderAddButton() {
    return (
      <RestrictedArea rule={manageSpaceRule}>
        <Button
          variant="tertiary"
          onClick={() =>
            browserHistory.push({
              ...this.context.location,
              state: { modal: "SpaceModal" },
            })
          }
          style={{ minWidth: "fit-content" }}
        >
          <dremio-icon name="interface/add-small" class="add-source-icon" />
          {intl.formatMessage({ id: "Space.AddSpace" })}
        </Button>
      </RestrictedArea>
    );
  }

  render() {
    const { spaces } = this.props;
    const { datasetDetails } = this.state;
    const { pathname } = this.context.location;
    const columns = catalogListingColumns({
      isViewAll: true,
      isVersioned: true,
    });
    const numberOfSpaces = spaces ? spaces.size : 0;
    const sortedSpaces = getCatalogData(
      spaces.map((space) => space.set("name", space.getIn(["path", 0]))),
      this.state.sort,
      SPACES_SORTING_MAP,
      this.state.filter,
    );

    return (
      <>
        <CatalogListingView
          getRow={(i) => {
            const item = sortedSpaces.get(i);
            return this.getRow(item);
          }}
          columns={columns}
          rowCount={sortedSpaces.size}
          onColumnsSorted={this.onColumnsSorted}
          title={
            <h3 className="flex items-center" style={{ height: 32 }}>
              <EllipsedText>
                <BreadCrumbs
                  fullPath={Immutable.fromJS([
                    `${intl.formatMessage({
                      id: "Space.AllSpaces",
                    })} (${numberOfSpaces})`,
                  ])}
                  pathname={pathname}
                />
              </EllipsedText>
            </h3>
          }
          rightHeaderButtons={this.renderAddButton()}
          className="all-spaces-view"
          onFilter={(filter) => this.setState({ filter: filter })}
          showButtonDivider
        />
        {datasetDetails && (
          <CatalogDetailsPanel
            panelItem={datasetDetails}
            handleDatasetDetailsCollapse={this.closeDetailsPanel}
          />
        )}
      </>
    );
  }
}

function ActionWrap({ children }) {
  return <span className="action-wrap">{children}</span>;
}
ActionWrap.propTypes = {
  children: PropTypes.node,
};

export default connect(mapStateToProps)(AllSpacesView);
