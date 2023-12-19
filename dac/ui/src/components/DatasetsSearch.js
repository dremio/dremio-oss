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
import Immutable from "immutable";
import PropTypes from "prop-types";
import { Link } from "react-router";
import ViewStateWrapper from "components/ViewStateWrapper";
import { IconButton } from "dremio-ui-lib/components";
import { injectIntl } from "react-intl";
import { getIconDataTypeFromDatasetType } from "utils/iconUtils";
import { TagList } from "dremio-ui-lib";
import { shouldUseNewDatasetNavigation } from "@app/utils/datasetNavigationUtils";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
import { bodySmall } from "uiTheme/radium/typography";

import { PALE_NAVY } from "uiTheme/radium/colors";
import DatasetItemLabel from "./Dataset/DatasetItemLabel";
import DatasetsSearchViewActions from "dyn-load/components/DatasetsSearchViewActions";
import QueryDataset from "@app/components/QueryDataset/QueryDataset";
import "./DatasetsSearch.less";
import {
  DATASET_TYPES_TO_ICON_TYPES,
  VIRTUAL_DATASET,
} from "@app/constants/datasetTypes";

const emptyList = new Immutable.List();
const TAG_LIST_MAX_WIDTH = 160;

class DatasetsSearch extends PureComponent {
  static propTypes = {
    searchData: PropTypes.instanceOf(Immutable.List).isRequired,
    globalSearch: PropTypes.bool,
    handleSearchHide: PropTypes.func.isRequired,
    inputValue: PropTypes.string,
    searchViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired,
  };

  onClickDataSetItem = () => {
    this.props.handleSearchHide();
  };

  getDatasetsList(searchData, inputValue) {
    const { globalSearch } = this.props;

    return searchData.map((value, key) => {
      const resourceId = value.getIn(["fullPath", 0]);
      const newFullPath = JSON.stringify(value.get("fullPath").toJS());
      const name = value.getIn(["fullPath", -1]);
      const tags = value.get("tags", emptyList);

      const href = {
        pathname: sqlPaths.sqlEditor.link(),
        search: `?context="${encodeURIComponent(
          resourceId
        )}"&queryPath=${encodeURIComponent(newFullPath)}`,
      };

      const toLink = shouldUseNewDatasetNavigation()
        ? href
        : wrapBackendLink(value.getIn(["links", "self"]));

      const datasetItem = (
        <div
          key={key}
          style={{ ...styles.datasetItem, ...bodySmall }}
          data-qa={`ds-search-row-${name}`}
          className="search-result-row"
        >
          <div style={styles.datasetData}>
            <DatasetItemLabel
              isSearchItem
              name={name}
              showFullPath
              inputValue={inputValue}
              fullPath={value.get("displayFullPath")}
              typeIcon={getIconDataTypeFromDatasetType(
                value.get("datasetType")
              )}
              placement="right"
              hideOverlayActionButtons
            />
          </div>
          <div className="search-result-row__right">
            {tags.size > 0 && (
              <TagList
                tags={tags}
                className="tagSearch"
                maxWidth={TAG_LIST_MAX_WIDTH}
              />
            )}
            {this.getActionButtons(value)}
          </div>
        </div>
      );

      return globalSearch ? (
        <Link
          key={key}
          className="dataset"
          style={{ textDecoration: "none" }}
          to={toLink}
          onClick={this.onClickDataSetItem}
        >
          {datasetItem}
        </Link>
      ) : (
        datasetItem
      );
    });
  }

  getActionButtons(dataset) {
    const {
      intl: { formatMessage },
    } = this.props;
    const datasetIconType = getIconDataTypeFromDatasetType(
      dataset.get("datasetType")
    );

    return (
      <span className="main-settings-btn min-btn" style={styles.actionButtons}>
        <DatasetsSearchViewActions dataset={dataset} />
        {datasetIconType !== DATASET_TYPES_TO_ICON_TYPES[VIRTUAL_DATASET] && (
          <IconButton
            as={Link}
            tooltip={formatMessage({ id: "Go.To.Table" })}
            to={wrapBackendLink(dataset.getIn(["links", "self"]))}
            data-qa="edit"
            onClick={(e) => e.stopPropagation()}
          >
            <dremio-icon
              name="navigation-bar/go-to-dataset"
              style={styles.icon}
            ></dremio-icon>
          </IconButton>
        )}
        <QueryDataset
          fullPath={dataset.get("fullPath")}
          resourceId={dataset.get("fullPath").get(0)}
        />
      </span>
    );
  }

  render() {
    const { searchData, inputValue, searchViewState } = this.props;
    const searchBlock =
      searchData && searchData.size && searchData.size > 0 ? (
        <div>{this.getDatasetsList(searchData, inputValue)}</div>
      ) : (
        <div style={styles.notFound}>
          {laDeprecated("No views or tables found")}
        </div>
      );
    return (
      <section className="datasets-search" style={styles.main}>
        <div className="dataset-wrapper" style={styles.datasetWrapper}>
          <ViewStateWrapper viewState={searchViewState}>
            {searchBlock}
          </ViewStateWrapper>
        </div>
      </section>
    );
  }
}

const styles = {
  main: {
    background: "#fff",
    zIndex: 999,
    color: "#000",
    boxShadow: "-1px 1px 1px #ccc",
  },
  datasetItem: {
    padding: "10px",
    borderBottom: "1px solid rgba(0,0,0,.1)",
    height: 45,
    width: "100%",
    display: "flex",
    alignItems: "center",
  },
  datasetData: {
    margin: "0 0 0 5px",
    minWidth: 300,
    display: "flex",
  },
  header: {
    height: 38,
    width: "100%",
    background: PALE_NAVY,
    display: "flex",
    alignItems: "center",
    padding: "0 10px",
  },
  datasetWrapper: {
    maxHeight: 360,
    overflow: "auto",
  },
  icon: {
    width: 24,
    height: 24,
  },
  closeIcon: {
    margin: "0 0 0 auto",
    height: 24,
    cursor: "pointer",
  },
  actionButtons: {
    margin: "0",
  },
  parentDatasetsHolder: {
    display: "flex",
  },
  parentDatasets: {
    display: "flex",
    alignItems: "center",
    margin: "0 10px 0 0",
  },
  notFound: {
    fontSize: 14,
    padding: "16px",
  },
};
export default injectIntl(DatasetsSearch);
