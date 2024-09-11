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
import PropTypes from "prop-types";
import Immutable from "immutable";
import { browserHistory } from "react-router";

import moment from "@app/utils/dayjs";
import { intl } from "@app/utils/intl";

import EntityLink from "@app/pages/HomePage/components/EntityLink";
import EllipsedText from "components/EllipsedText";
import AllSourcesMenu from "components/Menus/HomePage/AllSourcesMenu";
import SettingsBtn from "components/Buttons/SettingsBtn";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
import CatalogListingView from "../../components/CatalogListingView/CatalogListingView";
import BreadCrumbs from "@app/components/BreadCrumbs";

import { IconButton } from "dremio-ui-lib/components";
import { getIconStatusDatabase } from "utils/iconUtils";
import { isNotSoftware } from "dyn-load/utils/versionUtils";
import { getSettingsLocation } from "components/Menus/HomePage/AllSourcesMenu";
import { Button } from "dremio-ui-lib/components";
import {
  catalogListingColumns,
  CATALOG_LISTING_COLUMNS,
} from "dremio-ui-common/sonar/components/CatalogListingTable/catalogListingColumns.js";
import { getCatalogData } from "@app/utils/catalog-listing-utils";
import {
  renderResourcePin,
  renderSourceDetailsIcon,
} from "@inject/utils/catalog-listing-utils";
import CatalogDetailsPanel from "../../components/CatalogDetailsPanel/CatalogDetailsPanel";

const btnTypes = {
  settings: "settings",
};

const SOURCES_SORTING_MAP = {
  [CATALOG_LISTING_COLUMNS.name]: "name",
  [CATALOG_LISTING_COLUMNS.datasets]: "numberOfDatasets",
  [CATALOG_LISTING_COLUMNS.created]: "ctime",
};

class AllSourcesView extends PureComponent {
  static propTypes = {
    sources: PropTypes.object,
    title: PropTypes.string.isRequired,
    isExternalSource: PropTypes.bool,
    isDataPlaneSource: PropTypes.bool,
    isObjectStorageSource: PropTypes.bool,
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
    loggedInUser: PropTypes.object.isRequired,
  };

  state = {
    sort: null,
    filter: "",
    datasetDetails: null,
  };

  getRow(item) {
    return {
      id: item.get("id"),
      data: {
        name: (
          <div style={{ gap: 4, display: "flex", alignItems: "center" }}>
            <dremio-icon
              name={getIconStatusDatabase(
                item.getIn(["state", "status"]),
                item.get("type"),
              )}
            ></dremio-icon>
            <EntityLink
              entityId={item.get("id")}
              style={{
                overflow: "hidden",
                fontWeight: 500,
              }}
            >
              <EllipsedText text={item.get("name")} />
            </EntityLink>
            {renderResourcePin(item.get("id"))}
          </div>
        ),
        datasets: item.get("numberOfDatasets"),
        created: moment(new Date(item.get("ctime"))).format("MM/DD/YYYY"),
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
        link: getSettingsLocation(this.context.location, item),
        type: btnTypes.settings,
      },
    ];
    return [
      renderSourceDetailsIcon(
        this.context.location,
        item,
        this.renderDatasetDetailsIcon,
      ),
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
      this.getSettingsBtnByType(<AllSourcesMenu item={item} />, item),
    ];
  }

  renderDatasetDetailsIcon = (item) => {
    return (
      <IconButton
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
      </IconButton>
    );
  };
  getSettingsBtnByType(menu, item) {
    return (
      <SettingsBtn
        dataQa={item.get("name")}
        menu={menu}
        classStr="main-settings-btn min-btn catalog-btn"
        key={`${item.get("name")}-${item.get("id")}`}
        hideArrowIcon
        disablePortal
      >
        {this.getInlineIcon("interface/more")}
      </SettingsBtn>
    );
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

  getInlineIcon(icon) {
    return <dremio-icon name={icon} data-qa={icon} />;
  }

  onColumnsSorted = (sortedColumns) => {
    this.setState({ sort: sortedColumns });
  };

  renderAddButton() {
    const { isExternalSource, isDataPlaneSource, isObjectStorageSource } =
      this.props;

    /*eslint no-nested-ternary: "off"*/
    const headerId = isExternalSource
      ? "Source.AddDatabaseSource"
      : isDataPlaneSource
        ? isNotSoftware()
          ? "Source.AddArcticCatalog"
          : "Source.AddNessieCatalog"
        : isObjectStorageSource
          ? "Source.Add.Object.Storage"
          : "Source.Add.Metastore";

    return (
      this.context.loggedInUser.admin && (
        <Button
          onClick={() =>
            browserHistory.push({
              ...this.context.location,
              state: {
                modal: "AddSourceModal",
                isExternalSource,
                isDataPlaneSource,
              },
            })
          }
          variant="tertiary"
          style={{ minWidth: "fit-content" }}
        >
          <dremio-icon name="interface/add-small" class="add-source-icon" />
          {intl.formatMessage({ id: headerId })}
        </Button>
      )
    );
  }

  render() {
    const { sources, title, isDataPlaneSource } = this.props;
    const { datasetDetails } = this.state;
    const totalItems = sources.size;
    const { pathname } = this.context.location;
    const columns = catalogListingColumns({
      isViewAll: true,
      isVersioned: isDataPlaneSource,
    });
    const sortedSources = getCatalogData(
      sources,
      this.state.sort,
      SOURCES_SORTING_MAP,
      this.state.filter,
    );

    return (
      <>
        <CatalogListingView
          getRow={(i) => {
            const item = sortedSources.get(i);
            return this.getRow(item);
          }}
          columns={columns}
          rowCount={sortedSources.size}
          onColumnsSorted={this.onColumnsSorted}
          title={
            <h3 className="flex items-center" style={{ height: 32 }}>
              <EllipsedText>
                <BreadCrumbs
                  fullPath={Immutable.fromJS([`${title} (${totalItems})`])}
                  pathname={pathname}
                />
              </EllipsedText>
            </h3>
          }
          rightHeaderButtons={this.renderAddButton()}
          className="all-sources-view"
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
export default AllSourcesView;

function ActionWrap({ children }) {
  return <span className="action-wrap">{children}</span>;
}
ActionWrap.propTypes = {
  children: PropTypes.node,
};
