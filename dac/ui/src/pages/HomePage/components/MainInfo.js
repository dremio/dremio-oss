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
import { IconButton } from "dremio-ui-lib";
import { TagsAlert } from "@app/pages/HomePage/components/TagsAlert";

import { NESSIE, ARCTIC } from "@app/constants/sourceTypes";
import { NEW_DATASET_NAVIGATION } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { tableStyles } from "../tableStyles";
import BrowseTable from "./BrowseTable";
import { HeaderButtons } from "./HeaderButtons";
import MainInfoItemNameAndTag from "./MainInfoItemNameAndTag";
import SourceBranchPicker from "./SourceBranchPicker/SourceBranchPicker";
import { getSortedSources } from "@app/selectors/home";
import { getSourceByName } from "@app/utils/nessieUtils";
import ProjectHistoryButton from "@app/exports/pages/ArcticCatalog/components/ProjectHistoryButton";
import { selectState } from "@app/selectors/nessie/nessie";
import { constructArcticUrl } from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import { isVersionedSource as checkIsVersionedSource } from "@app/utils/sourceUtils";
import { fetchSupportFlagsDispatch } from "@inject/actions/supportFlags";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import { compose } from "redux";
import { withCatalogARSFlag } from "@inject/utils/arsUtils";
import { Wiki } from "@app/pages/ExplorePage/components/Wiki/Wiki";
import { isEntityWikiEditAllowed } from "dyn-load/utils/entity-utils";

const folderPath = "/folder/";

const shortcutBtnTypes = {
  edit: "edit",
  goToTable: "goToTable",
  settings: "settings",
  query: "query",
};

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
  };

  componentDidMount() {
    this.fetchWiki();
    this.fetchSupportFlags();
  }

  componentDidUpdate(prevProps) {
    this.fetchWiki(prevProps);
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

  getActionCell(item) {
    return <ActionWrap>{this.getActionCellButtons(item)}</ActionWrap>;
  }

  getActionCellButtons(item) {
    const dataType = item.get("fileType") || "dataset";
    switch (dataType) {
      case "folder":
        return this.getFolderActionButtons(item);
      case "file":
        return this.getFileActionButtons(item, item.get("permissions"));
      case "dataset":
        return this.getShortcutButtons(item, dataType);
      case "physicalDatasets":
        return this.getShortcutButtons(item, "physicalDataset"); // entities collection uses type name without last 's'
      // looks like 'raw', 'database', and 'table' are legacy entity types that are obsolete.
      default:
        throw new Error("unknown dataType");
    }
  }

  getFolderActionButtons(folder) {
    const { isVersionedSource } = this.props;
    const isFileSystemFolder = !!folder.get("fileSystemFolder");
    const isQueryAble = folder.get("queryable");
    const permissions = folder.get("permissions")
      ? folder.get("permissions").toJS()
      : null;
    const isAdmin = localStorageUtils.isUserAnAdmin();

    if (isFileSystemFolder && isQueryAble) {
      return this.getShortcutButtons(folder, "folder");
    } else if (
      this.checkToRenderConvertFolderButton(isFileSystemFolder, permissions)
    ) {
      return [
        this.getDetailsPanelBtn(folder),
        this.renderConvertButton(folder, {
          icon: <dremio-icon name="interface/format-folder" />,
          tooltip: "Folder.FolderFormat",
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
          <FolderMenu folder={folder} isVersionedSource={isVersionedSource} />,
          folder,
        ),
      ];
    } else {
      return;
    }
  }

  getFileActionButtons(file, permissions) {
    const isAdmin = localStorageUtils.isUserAnAdmin();
    const isQueryAble = file.get("queryable");
    if (isQueryAble) {
      return this.getShortcutButtons(file, "file");
    }
    // DX-12874 not queryable files should have only promote button
    if (this.checkToRenderConvertFileButton(isAdmin, permissions)) {
      return [
        this.renderConvertButton(file, {
          icon: <dremio-icon name="interface/format-file" />,
          tooltip: "File.FileFormat",
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
  getShortcutButtons(item, entityType) {
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
            tooltip={btnType.tooltip}
            key={item.get("id") + index}
            className="main-settings-btn min-btn"
            data-qa={btnType.type}
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
          tooltipPortal
          tooltipPlacement="top"
          className="main-settings-btn min-btn catalog-btn"
        >
          <dremio-icon name="interface/meta" />
        </IconButton>
      )
    );
  }

  getSettingsBtnByType(menu, item) {
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

  getRow(item) {
    const [name, jobs, action] = this.getTableColumns();
    const jobsCount =
      item.get("jobCount") || item.getIn(["extendedConfig", "jobCount"]) || 0;
    return {
      data: {
        [name.key]: {
          node: () => (
            <MainInfoItemNameAndTag
              item={item}
              openDetailsPanel={this.openDetailsPanel}
            />
          ),
          value: item.get("name"),
        },
        [jobs.key]: {
          node: () => (
            <Link to={wrapBackendLink(item.getIn(["links", "jobs"]))}>
              {jobsCount}
            </Link>
          ),
          value: jobsCount,
        },
        [action.key]: {
          node: () => this.getActionCell(item),
        },
      },
    };
  }

  getTableColumns() {
    const {
      intl: { formatMessage },
      entity,
    } = this.props;

    const showJobsColumn = entity && this.isNeitherNessieOrArctic();

    return [
      {
        key: "name",
        label: formatMessage({ id: "Common.Name" }),
        infoContent: <TagsAlert />,
        flexGrow: 1,
      },
      {
        key: "jobs",
        label: formatMessage({ id: "Job.Jobs" }),
        style: showJobsColumn ? tableStyles.digitColumn : { display: "none" },
        columnAlignment: "alignRight",
        headerStyle: {
          justifyContent: "flex-end",
          display: showJobsColumn ? "flex" : "none",
        },
        isFixedWidth: true,
        width: 66,
      },
      {
        key: "action",
        label: " ",
        style: tableStyles.actionColumn,
        isFixedWidth: true,
        width: 140,
        className: "row-buttons",
        headerClassName: "row-buttons",
        disableSort: true,
      },
    ];
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
        rows = rows.push(this.getRow(dataset));
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
    return constructArcticUrl({
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
      return (
        <Link
          to={this.constructVersionSourceLink()}
          style={{ textDecoration: "none" }}
        >
          <ProjectHistoryButton />
        </Link>
      );
    }
  };

  renderTitleExtraContent = () => {
    const { source } = this.props;
    if (this.isNeitherNessieOrArctic()) return null;
    return <SourceBranchPicker source={source.toJS()} />;
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

    const buttons = entity && (
      <HeaderButtons
        entity={entity}
        rootEntityType={rootEntityType}
        rightTreeVisible={this.props.rightTreeVisible}
        toggleVisibility={this.toggleRightTree}
        canUploadFile={canUploadFile}
        isVersionedSource={isVersionedSource}
      />
    );

    return (
      <>
        <BrowseTable
          item={entity}
          panelItem={panelItem}
          title={
            entity && (
              <BreadCrumbs
                fullPath={entity.get("fullPathList")}
                pathname={pathname}
                showCopyButton
                includeQuotes
                extraContent={this.renderTitleExtraContent()}
              />
            )
          }
          buttons={buttons}
          key={pathname} /* trick to clear out the searchbox on navigation */
          columns={this.getTableColumns()}
          rightSidebar={
            panelItem ? (
              <Wiki
                entityId={panelItem?.get("entityId") || panelItem?.get("id")}
                isEditAllowed={isEntityWikiEditAllowed(panelItem)}
                className="bottomContent"
                dataset={panelItem}
                handlePanelDetails={this.handleUpdatePanelDetails}
                isPanel
              />
            ) : null
          }
          rightSidebarExpanded={isDetailsPanelShown}
          rightSidebarIcon={this.shouldShowDetailsPanelIcon()}
          toggleSidebar={this.toggleDetailsPanel}
          tableData={this.getTableData()}
          viewState={viewState}
          renderExternalLink={this.renderExternalLink}
          disableZebraStripes
          rowHeight={40}
        >
          <DocumentTitle
            title={
              (entity &&
                formatFullPath(entity.get("fullPathList")).join(".")) ||
              ""
            }
          />
        </BrowseTable>
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

    isVersionedSource = checkIsVersionedSource(parentSource?.type);
  } else if (entityType === ENTITY_TYPES.source) {
    isVersionedSource = checkIsVersionedSource(props.entity.get("type"));
  }

  const nessieState = selectState(state.nessie, props.source?.get("name"));

  return {
    rootEntityType,
    isVersionedSource,
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
