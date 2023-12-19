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
import PropTypes from "prop-types";
import Immutable from "immutable";
import { debounce } from "lodash/function";
import Mousetrap from "mousetrap";
import invariant from "invariant";
import { injectIntl } from "react-intl";

import EllipsedText from "components/EllipsedText";

import { SearchField } from "components/Fields";
import StatefulTableViewer from "components/StatefulTableViewer";
import { SortDirection } from "@app/components/Table/TableUtils";
import {
  Row,
  GridColumn,
  SidebarColumn,
} from "@app/pages/HomePage/components/Columns";

import { isVersionedSource } from "@app/utils/sourceUtils";
import EmptyStateContainer from "./EmptyStateContainer";
import { constructFullPath } from "utils/pathUtils";
import { tableStyles } from "../tableStyles";
import { IconButton } from "dremio-ui-lib/components";
import { getIconType } from "@app/components/DatasetSummary/datasetSummaryUtils";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { PureEntityIcon } from "./EntityIcon";
import { ENTITY_TYPES } from "@app/constants/Constants";
import { getEntityTypeFromObject } from "@app/utils/entity-utils";
import { getExtraSummaryPanelIcon } from "dyn-load/utils/summary-utils";

import {
  activeWikiButton,
  searchField,
  searchFieldDivider,
  panelIcon,
} from "./BrowseTable.less";

@injectIntl
export class BrowseTable extends Component {
  static propTypes = {
    rightSidebar: PropTypes.node,
    rightSidebarExpanded: PropTypes.bool,
    toggleSidebar: PropTypes.func,
    tableData: PropTypes.instanceOf(Immutable.List),
    columns: PropTypes.array,
    title: PropTypes.node,
    buttons: PropTypes.node,
    children: PropTypes.node,
    filterKey: PropTypes.string.isRequired,
    intl: PropTypes.object.isRequired,
    renderExternalLink: PropTypes.func,
    renderTitleExtraContent: PropTypes.func,
    className: PropTypes.string,
    item: PropTypes.object,
    rightSidebarIcon: PropTypes.bool,
    // extra props passed along to underlying Table impl
  };

  static defaultProps = {
    filterKey: "name",

    // pass thru to Table
    defaultSortBy: "name",
    defaultSortDirection: SortDirection.ASC,
  };

  state = {
    filter: "",
  };

  mainContainerNode = null;

  constructor(props) {
    super(props);

    this.handleFilterChange = debounce(this.handleFilterChange, 200);
  }

  componentDidMount() {
    Mousetrap.bind(["command+f", "ctrl+f"], () => {
      this.searchField.focus();
      return false;
    });
  }

  componentWillUnmount() {
    Mousetrap.unbind(["command+f", "ctrl+f"]);
  }

  handleFilterChange = (filter) => {
    this.setState({
      filter,
    });
  };

  filteredTableData = () => {
    const { filterKey, tableData } = this.props;
    const { filter } = this.state;

    return !filter
      ? tableData
      : tableData.filter((item) => {
          let value = item.data[filterKey].value;
          value =
            typeof value !== "function"
              ? value
              : value.call(item.data[filterKey].value);
          return value.toLowerCase().includes(filter.trim().toLowerCase());
        });
  };

  onMainContainerRef = (mainEl) => {
    this.mainContainerNode = mainEl;
  };

  getMainContainer = () => this.mainContainerNode;

  render() {
    const {
      title,
      buttons,
      tableData,
      rightSidebar,
      rightSidebarExpanded,
      intl,
      toggleSidebar,
      renderExternalLink,
      renderTitleExtraContent,
      item,
      rightSidebarIcon,
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      panelItem,
      ...passAlongProps
    } = this.props;
    const versionContext = getVersionContextFromId(
      panelItem?.get("entityId") || panelItem?.get("id")
    );
    const panelName =
      panelItem?.get("name") || panelItem?.toJS()?.fullPath?.pop();
    const panelIsSource = panelItem?.get("entityType") === ENTITY_TYPES.source;

    invariant(
      !title || typeof title === "string" || title.props.fullPath,
      "BrowseTable title must be string or BreadCrumbs."
    );

    const resetScrollTop = Boolean(
      window.navigator.userAgent.toLowerCase().includes("firefox") &&
        this.state.filter
    ); //it's needed for DX-7140

    if (tableData.size) {
      passAlongProps.noDataText = intl.formatMessage(
        { id: "Search.BrowseTable" },
        { filter: this.state.filter }
      );
    }

    const showButtonDivider =
      buttons != null || (!rightSidebarExpanded && rightSidebar);

    return (
      <div className="main-info" ref={this.onMainContainerRef}>
        {this.props.children}
        <div className="list-content">
          <div className="row">
            <div>
              <Row className="browse-table-viewer-header">
                <GridColumn
                  style={{
                    justifyContent: "space-between",
                    alignItems: "center",
                  }}
                >
                  <span
                    style={styles.heading}
                    className="list-content__title-wrap"
                  >
                    <h3>
                      <EllipsedText
                        text={
                          !title || typeof title === "string"
                            ? title
                            : title &&
                              title.props &&
                              LRE +
                                constructFullPath(
                                  title.props.fullPath.toJS(),
                                  true
                                )
                        }
                      >
                        {title}
                      </EllipsedText>
                    </h3>
                    {renderTitleExtraContent && (
                      <span className="browse-table__titleExtra">
                        {renderTitleExtraContent()}
                      </span>
                    )}
                  </span>
                  <div style={{ display: "flex", alignItems: "center" }}>
                    {renderExternalLink && (
                      <span className="browse-table__externalLink">
                        {renderExternalLink()}
                      </span>
                    )}
                    <SearchField
                      ref={(searchField) => (this.searchField = searchField)}
                      onChange={this.handleFilterChange}
                      style={tableStyles.searchField}
                      closeIconTheme={
                        showButtonDivider && tableStyles.closeIcon
                      }
                      placeholder={intl.formatMessage({
                        id: "Dataset.FilterEllipsis",
                      })}
                      disabled={tableData.size < 1}
                      showCloseIcon
                      showIcon
                      inputClassName="mousetrap"
                      dataQa="browse-table-filter"
                      className={`${searchField} ${
                        showButtonDivider && searchFieldDivider
                      }`}
                    />
                    {buttons}
                    {!rightSidebarExpanded && rightSidebarIcon && (
                      <IconButton
                        tooltip={intl.formatMessage({
                          id: "Wiki.OpenDetails",
                        })}
                        onClick={toggleSidebar}
                        tooltipPortal
                        tooltipPlacement="top"
                        className={panelIcon}
                      >
                        <dremio-icon name="interface/meta" />
                      </IconButton>
                    )}
                  </div>
                </GridColumn>
                {rightSidebarExpanded && rightSidebar && (
                  <SidebarColumn
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      padding: "0 10px",
                    }}
                  >
                    <div className="flex items-center gap-1 text-ellipsis">
                      {panelIsSource ? (
                        <PureEntityIcon
                          disableHoverListener
                          entityType={panelItem.get("entityType")}
                          sourceStatus={panelItem?.getIn(
                            ["state", "status"],
                            null
                          )}
                          sourceType={panelItem?.get("type")}
                          style={{ width: 26, height: 26 }}
                        />
                      ) : (
                        <dremio-icon
                          name={`entities/${getIconType(
                            getEntityTypeFromObject(panelItem),
                            !!versionContext
                          )}`}
                          key={panelName} // <use> href doesn't always update
                          style={{ width: 26, height: 26 }}
                        />
                      )}
                      <p className="text-ellipsis">{panelName}</p>
                      {getExtraSummaryPanelIcon(panelItem)}
                    </div>
                    <div className={activeWikiButton}>
                      <IconButton
                        tooltip="Close details panel"
                        onClick={toggleSidebar}
                        style={{ width: 24, height: 24 }}
                      >
                        <dremio-icon name="interface/close-big" />
                      </IconButton>
                    </div>
                  </SidebarColumn>
                )}
              </Row>
            </div>
            <Row>
              <GridColumn className="table-wrap">
                <StatefulTableViewer
                  virtualized
                  className="table"
                  tableData={this.filteredTableData()}
                  resetScrollTop={resetScrollTop}
                  style={{ width: "100%" }}
                  {...(isVersionedSource(item?.get("type")) && {
                    renderEmpty: () => {
                      return (
                        <div className="table-wrap__empty">
                          <EmptyStateContainer
                            icon="interface/empty-content"
                            title={intl.formatMessage({
                              id: "Arctic.Source.Empty.Title",
                            })}
                          >
                            {intl.formatMessage({
                              id: "Arctic.Source.Empty.Hint",
                            })}
                          </EmptyStateContainer>
                        </div>
                      );
                    },
                  })}
                  {...passAlongProps}
                />
              </GridColumn>
              {rightSidebarExpanded && rightSidebar && (
                <SidebarColumn style={{ position: "relative" }}>
                  {rightSidebar}
                </SidebarColumn>
              )}
            </Row>
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  // todo: RTL support
  heading: {
    flexShrink: 1,
    minWidth: 0,
  },
};
const LRE = "\u202A"; // ... but make sure the text is treated as LTR by the text engine (e.g. render '@dremio', not 'dremio@')
// note: wrapping in <div> with direction:ltr doesn't produce "..."

export default BrowseTable;
