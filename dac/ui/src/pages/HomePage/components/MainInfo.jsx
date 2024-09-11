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
import { connect } from "react-redux";
import { Link } from "react-router";
import Immutable from "immutable";
import PropTypes from "prop-types";
import DocumentTitle from "react-document-title";
import { injectIntl } from "react-intl";
import urlParse from "url-parse";

import MainInfoMixin from "@inject/pages/HomePage/components/MainInfoMixin";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
import { loadWiki } from "@app/actions/home";
import DatasetMenu from "components/Menus/HomePage/DatasetMenu";
import FolderMenu from "components/Menus/HomePage/FolderMenu";

import BreadCrumbs, { formatFullPath } from "components/BreadCrumbs";
import { getRootEntityType } from "utils/pathUtils";
import SettingsBtn from "components/Buttons/SettingsBtn";
import { ENTITY_TYPES } from "@app/constants/Constants";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import { IconButton } from "dremio-ui-lib/components";

import { NESSIE, ARCTIC } from "@app/constants/sourceTypes";
import { NEW_DATASET_NAVIGATION } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { HeaderButtons } from "@inject/pages/HomePage/components/HeaderButtons";
import MainInfoItemNameAndTag from "./MainInfoItemNameAndTag";
import SourceBranchPicker from "./SourceBranchPicker/SourceBranchPicker";
import { getSortedSources } from "@app/selectors/home";
import { getSourceByName } from "@app/utils/nessieUtils";
import ProjectHistoryButton from "@app/exports/pages/VersionedHomePage/components/ProjectHistoryButton/ProjectHistoryButton";
import { selectState } from "@app/selectors/nessie/nessie";
import { constructVersionedEntityUrl } from "@app/exports/pages/VersionedHomePage/versioned-page-utils";
import { isVersionedSource as checkIsVersionedSource } from "@app/utils/sourceUtils";
import { fetchSupportFlagsDispatch } from "@inject/actions/supportFlags";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import { compose } from "redux";
import { withCatalogARSFlag } from "@inject/utils/arsUtils";
import CatalogListingView from "./CatalogListingView/CatalogListingView";
import EllipsedText from "@app/components/EllipsedText";
import {
  catalogListingColumns,
  CATALOG_LISTING_COLUMNS,
} from "dremio-ui-common/sonar/components/CatalogListingTable/catalogListingColumns.js";
import { getCatalogData } from "@app/utils/catalog-listing-utils";
import { intl } from "@app/utils/intl";
import CatalogDetailsPanel from "./CatalogDetailsPanel/CatalogDetailsPanel";
import Message from "@app/components/Message";

import { panelIcon } from "./BrowseTable.less";

const folderPath = "/folder/";

const shortcutBtnTypes = {
  edit: "edit",
  goToTable: "goToTable",
  settings: "settings",
  query: "query",
};

const CATALOG_SORTING_MAP = {
  [CATALOG_LISTING_COLUMNS.name]: "name",
  [CATALOG_LISTING_COLUMNS.jobs]: "jobCount",
};

const loadingSkeletonRows = Array(10).fill(null);

const getEntityId = (props) => {
  return props && props.entity ? props.entity.get("id") : null;
};

@MainInfoMixin
export class MainInfoView extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired,
  };

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    entityType: PropTypes.oneOf(Object.values(ENTITY_TYPES)),
    viewState: PropTypes.instanceOf(Immutable.Map),
    updateRightTreeVisibility: PropTypes.func,
    rightTreeVisible: PropTypes.bool,
    isInProgress: PropTypes.bool,
    intl: PropTypes.object.isRequired,
    fetchWiki: PropTypes.func, // (entityId) => Promise
    source: PropTypes.instanceOf(Immutable.Map),
    canUploadFile: PropTypes.bool,
    isVersionedSource: PropTypes.bool,
    rootEntityType: PropTypes.string,
    nessieState: PropTypes.object,
    dispatchFetchSupportFlags: PropTypes.func,

    // HOC
    isArsEnabled: PropTypes.bool,
    isArsLoading: PropTypes.bool,
  };

  static defaultProps = {
    viewState: Immutable.Map(),
  };

  state = {
    isDetailsPanelShown: localStorageUtils.getWikiVisibleState(),
    datasetDetails: null,
    sort: null,
    filter: "",
  };

  componentDidMount() {
    this.fetchWiki();
    this.fetchSupportFlags();
  }

  componentDidUpdate(prevProps) {
    this.fetchWiki(prevProps);

    if (getEntityId(prevProps) !== getEntityId(this.props)) {
      this.setState({ filter: "", sort: null });
    }
  }

  fetchWiki(prevProps) {
    const oldId = getEntityId(prevProps);
    const newId = getEntityId(this.props);
    if (newId && oldId !== newId && this.shouldShowDetailsPanelIcon()) {
      this.props.fetchWiki(newId);
      this.setState({ datasetDetails: null });
    }
  }

  fetchSupportFlags() {
    const { dispatchFetchSupportFlags } = this.props;

    dispatchFetchSupportFlags?.("ui.upload.allow");
    dispatchFetchSupportFlags?.("client.tools.tableau");
    dispatchFetchSupportFlags?.("client.tools.powerbi");
    dispatchFetchSupportFlags?.(NEW_DATASET_NAVIGATION);
  }

  getActionCell(item, idx) {
    return <ActionWrap>{this.getActionCellButtons(item, idx)}</ActionWrap>;
  }

  getActionCellButtons(item, idx) {
    const dataType = item.get("fileType") || "dataset";
    switch (dataType) {
      case "folder":
        return this.getFolderActionButtons(item, idx);
      case "file":
        return this.getFileActionButtons(item, item.get("permissions"), idx);
      case "dataset":
        return this.getShortcutButtons(item, dataType, idx);
      case "physicalDatasets":
        return this.getShortcutButtons(item, "physicalDataset", idx); // entities collection uses type name without last 's'
      // looks like 'raw', 'database', and 'table' are legacy entity types that are obsolete.
      default:
        throw new Error("unknown dataType");
    }
  }

  getFolderActionButtons(folder, idx) {
    const { isVersionedSource, sourceType, nessieState = {} } = this.props;
    const isFileSystemFolder = !!folder.get("fileSystemFolder");
    const isQueryAble = folder.get("queryable");
    const permissions = folder.get("permissions")
      ? folder.get("permissions").toJS()
      : null;
    const isAdmin = localStorageUtils.isUserAnAdmin();

    if (isFileSystemFolder && isQueryAble) {
      return this.getShortcutButtons(folder, "folder", idx);
    } else if (
      this.checkToRenderConvertFolderButton(isFileSystemFolder, permissions)
    ) {
      return [
        this.getDetailsPanelBtn(folder),
        this.renderConvertButton(folder, {
          icon: <dremio-icon name="interface/format-folder" />,
          tooltip: intl.formatMessage({ id: "Folder.FolderFormat" }),
          to: {
            ...this.context.location,
            state: {
              modal: "DatasetSettingsModal",
              tab: "format",
              entityName: folder.get("fullPathList").last(),
              entityType: folder.get("entityType"),
              type: folder.get("entityType"),
              entityId: folder.get("id"),
              query: { then: "query" },
              isHomePage: true,
            },
          },
        }),
        this.getSettingsBtnByType(
          <FolderMenu folder={folder} isVersionedSource={isVersionedSource} />,
          folder,
          idx,
        ),
      ];
    } else if (
      isAdmin ||
      (permissions &&
        (permissions.canAlter ||
          permissions.canRead ||
          permissions.canEditAccessControlList ||
          permissions.canDelete))
    ) {
      return [
        this.getDetailsPanelBtn(folder),
        this.getSettingsBtnByType(
          <FolderMenu
            folder={folder}
            isVersionedSource={isVersionedSource}
            sourceType={sourceType}
            nessieState={nessieState}
          />,
          folder,
          idx,
        ),
      ];
    } else {
      return;
    }
  }

  getFileActionButtons(file, permissions, idx) {
    const isAdmin = localStorageUtils.isUserAnAdmin();
    const isQueryAble = file.get("queryable");
    if (isQueryAble) {
      return this.getShortcutButtons(file, "file", idx);
    }
    // DX-12874 not queryable files should have only promote button
    if (this.checkToRenderConvertFileButton(isAdmin, permissions)) {
      return [
        this.renderConvertButton(file, {
          icon: <dremio-icon name="interface/format-file" />,
          tooltip: intl.formatMessage({ id: "File.FileFormat" }),
          to: {
            ...this.context.location,
            state: {
              modal: "DatasetSettingsModal",
              tab: "format",
              entityName: file.get("fullPathList").last(),
              entityType: file.get("entityType"),
              type: file.get("entityType"),
              entityId: file.get("id"),
              queryable: file.get("queryable"),
              fullPath: file.get("filePath"),
              query: { then: "query" },
              isHomePage: true,
            },
          },
        }),
      ];
    } else {
      return;
    }
  }

  // this method is targeted for dataset like entities: PDS, VDS and queriable files
  getShortcutButtons(item, entityType, idx) {
    const allBtns = this.getShortcutButtonsData(
      item,
      entityType,
      shortcutBtnTypes,
    );
    return [
      this.getDetailsPanelBtn(item),
      ...allBtns
        // select buttons to be shown
        .filter((btn) => btn.isShown)
        // return rendered link buttons
        .map((btnType, index) => (
          <IconButton
            as={LinkWithRef}
            to={btnType.link}
            tooltip={intl.formatMessage({ id: btnType.tooltip })}
            key={item.get("id") + index}
            className="main-settings-btn min-btn"
            data-qa={btnType.type}
            tooltipPortal
          >
            {btnType.label}
          </IconButton>
        )),
      this.getSettingsBtnByType(
        <DatasetMenu
          entity={item}
          entityType={entityType}
          openWikiDrawer={(dataset) => this.openDetailsPanel(dataset, true)}
        />,
        item,
        idx,
      ),
    ];
  }

  getInlineIcon(icon) {
    return <dremio-icon name={icon} data-qa={icon} />;
  }

  getWikiButtonLink(item) {
    const url = wrapBackendLink(item.getIn(["links", "query"]));
    const parseUrl = urlParse(url);
    return `${parseUrl.pathname}/wiki${parseUrl.query}`;
  }

  getDetailsPanelBtn(item) {
    const showButton = this.shouldShowDetailsPanelIcon(item);
    return (
      showButton && (
        <IconButton
          tooltip="Open details panel"
          onClick={(e) => {
            e.preventDefault();
            this.openDetailsPanel(item, true);
          }}
          className="main-settings-btn min-btn catalog-btn"
          tooltipPortal
        >
          <dremio-icon name="interface/meta" />
        </IconButton>
      )
    );
  }

  getSettingsBtnByType(menu, item, idx) {
    return (
      <SettingsBtn
        dataQa={item.get("name")}
        menu={menu}
        classStr="main-settings-btn min-btn catalog-btn"
        key={`${item.get("name")}-${item.get("id")}`}
        hideArrowIcon
      >
        {this.getInlineIcon("interface/more")}
      </SettingsBtn>
    );
  }

  getRow(item, i) {
    if (!item)
      return {
        id: `${i}`,
      };

    const jobsCount =
      item.get("jobCount") || item.getIn(["extendedConfig", "jobCount"]) || 0;
    return {
      id: item.get("id"),
      className: item.get("id"),
      data: {
        name: (
          <MainInfoItemNameAndTag
            item={item}
            openDetailsPanel={this.openDetailsPanel}
          />
        ),
        jobs: (
          <Link to={wrapBackendLink(item.getIn(["links", "jobs"]))}>
            {jobsCount}
          </Link>
        ),
        actions: this.getActionCell(item, i),
      },
    };
  }

  getTableData() {
    const contents = this.props.entity && this.props.entity.get("contents");
    let rows = Immutable.List();
    if (contents && !contents.isEmpty()) {
      const appendRow = (dataset) => {
        // DX-10700 there could be the cases, when we reset a entity cache (delete all entities of the certain type), but there are references on these intities in the state;
        // For example, we clearing folders, when we navigating to another folder, but sources could have a reference by id on these folder. As folder would not be found, here we would have undefined
        //skip such cases
        if (!dataset) return;
        rows = rows.push(dataset);
      };
      contents.get("datasets").forEach(appendRow);
      contents.get("folders").forEach(appendRow);
      contents.get("files").forEach(appendRow);
      contents.get("physicalDatasets").forEach(appendRow);
    }

    return rows;
  }

  isReadonly(spaceList) {
    const { pathname } = this.context.location;
    if (spaceList !== undefined) {
      return !!spaceList.find((item) => {
        if (item.href === pathname) {
          return item.readonly;
        }
      });
    }
    return false;
  }

  toggleDetailsPanel = () => {
    const { entity } = this.props;
    let newValue;
    this.setState(
      (prevState) => ({
        isDetailsPanelShown: (newValue = !prevState.isDetailsPanelShown),
        datasetDetails: Immutable.fromJS({
          ...entity.toJS(),
          fullPath: entity.get("fullPath") || entity.get("fullPathList"),
        }),
      }),
      () => {
        localStorageUtils.setWikiVisibleState(newValue);
      },
    );
  };

  toggleRightTree = () => {
    this.props.updateRightTreeVisibility(!this.props.rightTreeVisible);
  };

  isNessie = () => {
    const { source, entity } = this.props;
    return entity && source && source.get("type") === NESSIE;
  };

  isArctic = () => {
    const { source, entity } = this.props;
    return entity && source && source.get("type") === ARCTIC;
  };

  isNeitherNessieOrArctic = () => {
    return !this.isNessie() && !this.isArctic();
  };

  constructVersionSourceLink = () => {
    const { source, nessieState = {} } = this.props;
    const { hash, reference } = nessieState;
    const { pathname } = this.context.location;
    const versionBase = this.isArctic() ? "arctic" : "nessie";
    let namespace = encodeURIComponent(reference?.name) || "";
    if (pathname.includes(folderPath)) {
      namespace = pathname.substring(
        pathname.indexOf(folderPath) + folderPath.length,
        pathname.length,
      );
      if (reference) {
        namespace = `${encodeURIComponent(reference?.name)}/${namespace}`;
      }
    }
    return constructVersionedEntityUrl({
      type: "source",
      baseUrl: `/sources/${versionBase}/${source.get("name")}`,
      tab: "commits",
      namespace: namespace,
      hash: hash ? `?hash=${hash}` : "",
    });
  };

  renderExternalLink = () => {
    if (this.isNeitherNessieOrArctic()) return null;
    else {
      return <ProjectHistoryButton to={this.constructVersionSourceLink()} />;
    }
  };

  renderTitleExtraContent = () => {
    const { source } = this.props;
    if (this.isNeitherNessieOrArctic()) return null;
    return <SourceBranchPicker source={source.toJS()} />;
  };

  renderDatasetDetailsIcon = () => {
    const { isDetailsPanelShown } = this.state;
    if (!this.shouldShowDetailsPanelIcon() || isDetailsPanelShown) return null;

    return (
      <IconButton
        tooltip={intl.formatMessage({
          id: "Wiki.OpenDetails",
        })}
        onClick={this.toggleDetailsPanel}
        tooltipPlacement="top"
        className={panelIcon}
      >
        <dremio-icon name="interface/meta" />
      </IconButton>
    );
  };

  openDetailsPanel = async (dataset) => {
    const { datasetDetails } = this.state;
    const currentDatasetId = dataset.get("id") || dataset?.get("entityId");
    if (currentDatasetId === datasetDetails?.get("entityId")) {
      return;
    }

    this.setState({
      isDetailsPanelShown: true,
      datasetDetails: dataset,
    });
  };

  handleUpdatePanelDetails = (dataset) => {
    if (dataset.get("error")) {
      this.setState({
        datasetDetails: this.state.datasetDetails.merge(dataset),
      });
    } else if (
      dataset?.get("entityId") !== this.state.datasetDetails?.get("entityId")
    ) {
      this.setState({ datasetDetails: dataset });
    }
  };

  openDatasetInNewTab = () => {
    const { datasetDetails = Immutable.fromJS({}) } = this.state;

    const selfLink = datasetDetails.getIn(["links", "query"]);
    const editLink = datasetDetails.getIn(["links", "edit"]);
    const canAlter = datasetDetails.getIn(["permissions", "canAlter"]);
    const toLink = canAlter && editLink ? editLink : selfLink;
    const urldetails = new URL(window.location.origin + toLink);
    const pathname = urldetails.pathname + "/wiki" + urldetails.search;
    window.open(wrapBackendLink(pathname), "_blank");
  };

  onColumnsSorted = (sortedColumns) => {
    this.setState({ sort: sortedColumns });
  };

  render() {
    const {
      canUploadFile,
      entity,
      viewState,
      isVersionedSource,
      rootEntityType,
    } = this.props;
    const { datasetDetails, isDetailsPanelShown } = this.state;
    const panelItem = datasetDetails
      ? datasetDetails
      : this.shouldShowDetailsPanelIcon()
        ? entity
        : null;
    const { pathname } = this.context.location;
    const tableData = this.getTableData();
    const sortedData =
      viewState.get("isInProgress") && tableData.size === 0
        ? Immutable.fromJS(loadingSkeletonRows)
        : getCatalogData(
            tableData,
            this.state.sort,
            CATALOG_SORTING_MAP,
            this.state.filter,
          );
    const columns = catalogListingColumns({
      isViewAll: false,
      isVersioned: !this.isNeitherNessieOrArctic(),
    });
    const error = viewState.getIn(["error", "message", "errorMessage"]);

    const buttons = entity && (
      <>
        <HeaderButtons
          entity={entity}
          rootEntityType={rootEntityType}
          rightTreeVisible={this.props.rightTreeVisible}
          toggleVisibility={this.toggleRightTree}
          canUploadFile={canUploadFile}
          isVersionedSource={isVersionedSource}
        />
        {this.renderDatasetDetailsIcon()}
      </>
    );

    return (
      <>
        <DocumentTitle
          title={
            (entity && formatFullPath(entity.get("fullPathList")).join(".")) ||
            ""
          }
        />
        {error ? (
          <Message isDismissable={false} messageType="error" message={error} />
        ) : (
          <CatalogListingView
            key={pathname}
            getRow={(i) => {
              const item = sortedData.get(i);
              return this.getRow(item, i);
            }}
            columns={columns}
            rowCount={sortedData.size}
            onColumnsSorted={this.onColumnsSorted}
            title={
              <h3
                style={{
                  minWidth: !this.renderTitleExtraContent() ? 80 : 150,
                  height: 32,
                }}
              >
                <EllipsedText>
                  {entity && (
                    <BreadCrumbs
                      fullPath={entity.get("fullPathList")}
                      pathname={pathname}
                      showCopyButton
                      includeQuotes
                      extraContent={this.renderTitleExtraContent()}
                    />
                  )}
                </EllipsedText>
              </h3>
            }
            rightHeaderButtons={buttons}
            leftHeaderButtons={this.renderExternalLink()}
            onFilter={(filter) => this.setState({ filter: filter })}
            showButtonDivider={
              buttons != null || (!isDetailsPanelShown && panelItem)
            }
            leftHeaderStyles={{
              minWidth: !this.renderTitleExtraContent() ? 80 : 150,
            }}
          />
        )}

        {panelItem && isDetailsPanelShown && (
          <CatalogDetailsPanel
            panelItem={panelItem}
            handleDatasetDetailsCollapse={this.toggleDetailsPanel}
            handlePanelDetails={this.handleUpdatePanelDetails}
          />
        )}
      </>
    );
  }
}
MainInfoView = injectIntl(MainInfoView);

function ActionWrap({ children }) {
  return <span className="action-wrap">{children}</span>;
}
ActionWrap.propTypes = {
  children: PropTypes.node,
};

function mapStateToProps(state, props) {
  const rootEntityType = getRootEntityType(
    wrapBackendLink(props.entity?.getIn(["links", "self"])),
  );

  let isVersionedSource = false;
  let sourceType;
  const entityType = props.entity?.get("entityType");
  const sources = getSortedSources(state);
  if (
    sources &&
    rootEntityType === ENTITY_TYPES.source &&
    entityType === ENTITY_TYPES.folder
  ) {
    const entityJS = props.entity.toJS();
    const parentSource = getSourceByName(
      entityJS.fullPathList[0],
      sources,
    )?.toJS();
    sourceType = parentSource?.type;
    isVersionedSource = checkIsVersionedSource(parentSource?.type);
  } else if (entityType === ENTITY_TYPES.source) {
    sourceType = props.entity.get("type");
    isVersionedSource = checkIsVersionedSource(props.entity.get("type"));
  }
  const nessieState = selectState(state.nessie, props.source?.get("name"));

  return {
    rootEntityType,
    isVersionedSource,
    sourceType,
    canUploadFile: state.privileges?.project?.canUploadFile,
    nessieState,
  };
}

export default compose(
  withCatalogARSFlag,
  connect(mapStateToProps, (dispatch) => ({
    fetchWiki: loadWiki(dispatch),
    dispatchFetchSupportFlags: fetchSupportFlagsDispatch?.(dispatch),
  })),
)(MainInfoView);
