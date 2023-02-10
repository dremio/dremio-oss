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
import { sortedIndex } from "lodash/array";
import { customAlphabet } from "nanoid/non-secure";
import transformModelMapper from "utils/mappers/ExplorePage/Transform/transformModelMapper";
import simpleTransformMappers from "utils/mappers/ExplorePage/simpleTransformMappers";
import mapConvertDataType from "utils/mappers/ExplorePage/convertDataType";
import dataStoreUtils from "utils/dataStoreUtils";
import { copyTextToClipboard } from "@app/utils/clipboard/clipboardUtils";
import {
  BOOLEAN,
  DATE,
  DATETIME,
  DECIMAL,
  FLOAT,
  INTEGER,
  LIST,
  MAP,
  STRUCT,
  TEXT,
  TIME,
} from "@app/constants/DataTypes";
import { APIV2Call } from "@app/core/APICall";
import { JOB_STATUS } from "@app/pages/ExplorePage/components/ExploreTable/constants";
import { PHYSICAL_DATASET_TYPES } from "@app/constants/datasetTypes";
import { getRefQueryParamsFromDataset } from "../nessieUtils";
import { newQuery } from "@app/exports/paths";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";

const DROP_WIDTH = 150;
const ARROW_WIDTH = 20;
const SHADOW_WIDTH = 1;
const WIDTH_WITH_PADDING = 6;
const HEIGHT_WITH_LINEHEIGHT = 9;
const HALF_LINEHEIGHT = HEIGHT_WITH_LINEHEIGHT / 2;
const ROWS_LIMIT = 150;
const TRANSFORM_TYPE_WITHOUT_DATA = [
  "EXTRACT_ELEMENT",
  "EXTRACT_ELEMENTS",
  "EXTRACT_TEXT",
  "REPLACE_TEXT",
  "SPLIT",
  "KEEP_ONLY",
  "EXCLUDE",
  "REPLACE",
];

const rand = customAlphabet("1234567890", 13);

const detailTypeToTransformType = {
  EXTRACT_ELEMENT: "extract",
  EXTRACT_ELEMENTS: "extract",
  EXTRACT_TEXT: "extract",
  REPLACE_TEXT: "replace",
  REPLACE: "replace",
  KEEP_ONLY: "keeponly",
  SPLIT: "split",
  EXCLUDE: "exclude",
};

const notNeedSelectionMethod = ["Values", "Range", "Custom Condition", "Exact"];

const transformationMapperHash = {
  ASC: simpleTransformMappers.mapSortItem,
  DESC: simpleTransformMappers.mapSortItem,
  UNNEST: simpleTransformMappers.mapUnnest,
  RENAME: simpleTransformMappers.mapRename,
  MULTIPLE: simpleTransformMappers.mapMultySortItem,
  CONVERT_CASE: simpleTransformMappers.mapCovertCase,
  TRIM_WHITE_SPACES: simpleTransformMappers.mapTrim,
  transform: transformModelMapper.transformApplyOrPreviewMapper,
  CALCULATED_FIELD: simpleTransformMappers.mapCalculated,
  DROP: simpleTransformMappers.mapDrop,
  CONVERT_DATA_TYPE: mapConvertDataType,
  SPLIT_BY_DATA_TYPE: simpleTransformMappers.mapCleanData,
  SINGLE_DATA_TYPE: simpleTransformMappers.mapCleanData,
  GROUP_BY: simpleTransformMappers.mapGroupBy,
  JOIN: simpleTransformMappers.mapJoin,
};

class ExploreUtils {
  getTransformState(location) {
    const state = location.state || {};
    return Immutable.fromJS({
      transformType: state.transformType,
      columnName: state.columnName,
      columnType: state.columnType,
      method:
        state.method ||
        this.getInitialTransformMethod(
          state.columnType,
          state.transformType,
          state.selection
        ),
      selection: state.selection || {},
    });
  }

  getLocationToGoToTransformWizard({
    detailType,
    column,
    toType,
    location,
    joinTab,
    props,
  }) {
    const { pathname: oldPathname, query: oldQuery } = location;
    const pathname = `${oldPathname}/details`;
    const query = {
      ...oldQuery,
      type: detailType,
    };
    const state = {
      columnName: column.name,
      columnType: column.type,
      toType,
      previewVersion: "",
      props,
    };
    if (joinTab) {
      query.joinTab = joinTab;
    }
    if (TRANSFORM_TYPE_WITHOUT_DATA.indexOf(detailType) !== -1) {
      query.type = "transform";
      state.transformType = detailTypeToTransformType[detailType] || detailType;
      state.method = this.getInitialTransformMethod(
        state.columnType,
        state.transformType,
        state.selection
      );
    }
    return { pathname, state, query };
  }

  getInitialTransformMethod(columnType, transformType, selection) {
    const hasSelection = selection && selection.cellText !== "";
    switch (columnType) {
      case TEXT:
        switch (transformType) {
          case "replace":
          case "keeponly":
          case "exclude":
            return hasSelection ? "Pattern" : "Values";
          default:
            return "Pattern";
        }
      case DATETIME:
      case DATE:
      case TIME:
      case INTEGER:
      case FLOAT:
      case DECIMAL:
        switch (transformType) {
          case "replace":
          case "keeponly":
          case "exclude":
            return hasSelection ? "Exact" : "Range";
          default:
            break;
        }
        break;
      case BOOLEAN:
        return "Values";
      default:
        break;
    }

    const firstMethodForColumnType = dataStoreUtils
      .getSubtypeForTransformTab()
      .find((subtitle) => {
        return subtitle.types.find((type) => type === columnType);
      });
    if (firstMethodForColumnType) {
      return firstMethodForColumnType.name;
    }
  }

  transformHasSelection(transform) {
    return (
      Boolean(transform.getIn(["selection", "cellText"])) ||
      Boolean(transform.getIn(["selection", "mapPathList"]))
    );
  }

  /**
   * Trims empty cells out of selection. Returns null if selection spans multiple cells.
   * @param selection
   * @returns selection with modified range
   */
  updateSelectionToSingleCell(selection) {
    if (!selection || selection.rangeCount !== 1) {
      return null;
    }
    const selRange = selection.getRangeAt(0);
    let startCell = this.getTableCellElement(selRange.startContainer);
    let endCell = this.getTableCellElement(selRange.endContainer);

    // already single cell?
    if (startCell === endCell) {
      return selection;
    }

    // try removing outside cells
    let rangeStart = selRange.startOffset;
    if (startCell && startCell.textContent.length === rangeStart) {
      startCell = startCell.nextSibling;
      rangeStart = 0;
    }
    let rangeEnd = selRange.endOffset;
    if (endCell && rangeEnd === 0) {
      endCell = endCell.previousSibling;
      if (endCell) {
        rangeEnd = endCell.textContent.length;
      }
    }

    if (startCell !== endCell) {
      return null;
    }

    const textNode = this.getTableCellTextNode(startCell);
    this.replaceSelectionRange(selection, textNode, rangeStart, rangeEnd);

    return selection;
  }

  getTableCellElement(node) {
    const result =
      node.nodeType === window.Node.TEXT_NODE ? node.parentNode : node;
    return result?.closest(".public_fixedDataTableCell_main");
  }

  replaceSelectionRange(selection, node, start, end) {
    try {
      selection.removeAllRanges();
      const range = document.createRange();
      range.setStart(node, start);
      range.setEnd(node, end);
      selection.addRange(range);
    } catch (e) {
      return;
    }
  }

  getTableCellTextNode(cell) {
    return $(".cell-wrap", cell).contents()[0];
  }

  getSelectionData(selection) {
    if (!selection || selection.rangeCount !== 1) {
      return null;
    }

    const selectionText = selection.toString(); //DX-11088 we would like to enable spliting data by space. So we should not trim selection value.
    const oRange = selection.getRangeAt(0);
    if (oRange.endContainer === oRange.startContainer) {
      return {
        selection,
        text: selectionText,
        oRange,
        oRect: oRange.getBoundingClientRect(),
        startOffset: oRange.startOffset,
        endOffset: oRange.endOffset,
      };
    }

    return {
      selection,
      text: selectionText,
      oRange,
      oRect: oRange.getBoundingClientRect(),
      startOffset: oRange.startOffset,
      endOffset: oRange.endOffset,
    };
  }

  calculateListSelectionOffsets(startOffset, endOffset, text) {
    const items = JSON.parse(text);
    if (!items || !items.length) {
      return null;
    }

    const itemsText = items.map(JSON.stringify);
    // calcluate indices of first and last chars of an item
    const itemExtents = itemsText.slice(1).reduce(
      (prev, itemText) => {
        const start = prev[prev.length - 1][1] + 2; // + 1 for comma, +1 to advance to next item.
        return prev.concat([[start, start + itemText.length - 1]]);
      },
      [[1, 1 + itemsText[0].length - 1]]
    );

    const startItemIndex = sortedIndex(
      itemExtents.map((x) => x[1]),
      startOffset
    );
    const endItemIndex = sortedIndex(
      itemExtents.map((x) => x[0]),
      endOffset
    ); // This end index is exclusive

    if (
      startItemIndex === items.length ||
      endItemIndex === 0 ||
      startItemIndex === endItemIndex
    ) {
      return null;
    }

    return {
      itemsText,
      startIndex: startItemIndex,
      endIndex: endItemIndex,
      startOffset: itemExtents[startItemIndex][0],
      endOffset: itemExtents[endItemIndex - 1][1] + 1, // +1 to change inclusive to exclusive
    };
  }

  _findItemsForList(startOffset, endOffset, text, sel) {
    const newOffsets = this.calculateListSelectionOffsets(
      startOffset,
      endOffset,
      text
    );
    if (newOffsets === null) {
      return null;
    }
    const {
      itemsText,
      startIndex,
      endIndex,
      startOffset: newStartOffset,
      endOffset: newEndOffset,
    } = newOffsets;

    this.replaceSelectionRange(
      sel,
      sel.anchorNode,
      newStartOffset,
      newEndOffset
    );
    const selectedText = text.slice(newStartOffset, newEndOffset);
    return {
      oRect: sel.getRangeAt(0).getBoundingClientRect(),
      selectedText,
      startIndex,
      endIndex,
      listOfItems: itemsText,
    };
  }

  getSelectionForList(columnText, columnName, sel) {
    const { startOffset, endOffset, selection } = sel;
    const itemInfo = this._findItemsForList(
      startOffset,
      endOffset,
      columnText,
      selection
    );
    if (!itemInfo) {
      return null;
    }
    const { selectedText, listOfItems, startIndex, endIndex, oRect } = itemInfo;
    return {
      listOfItems,
      model: {
        columnName,
        cellText: columnText,
        startIndex,
        endIndex, // exclusive
        listLength: listOfItems.length,
      },
      position: {
        display: true,
        left: oRect.left + oRect.width,
        top: oRect.top + oRect.height,
        textWrap: {
          width: "auto",
          height: oRect.height + HEIGHT_WITH_LINEHEIGHT,
          left: oRect.left - SHADOW_WIDTH,
          top: oRect.top - HALF_LINEHEIGHT,
          text: selectedText.toString().trim(),
        },
      },
    };
  }

  getSelection(columnText, columnName, selection) {
    const { oRect, text, startOffset, endOffset } = selection;

    return {
      model: {
        columnName,
        cellText: columnText,
        length: text.length,
        offset: startOffset > endOffset ? endOffset : startOffset,
        fullCellSelection: columnText === text.trim(),
      },
      position: {
        display: true,
        left:
          oRect.left + oRect.width - DROP_WIDTH + ARROW_WIDTH + SHADOW_WIDTH,
        top: oRect.top + oRect.height,
        textWrap: {
          width: oRect.width + WIDTH_WITH_PADDING,
          height: oRect.height + HEIGHT_WITH_LINEHEIGHT,
          left: oRect.left - SHADOW_WIDTH,
          top: oRect.top - HALF_LINEHEIGHT,
          text: text.trim(),
        },
      },
    };
  }

  selectAll(elem) {
    const position = elem.getBoundingClientRect();
    this.selectElementContents(elem);
    return {
      display: true,
      left:
        position.left +
        position.width -
        DROP_WIDTH +
        ARROW_WIDTH +
        SHADOW_WIDTH,
      top: position.top + position.height,
      textWrap: {
        width: position.width + WIDTH_WITH_PADDING,
        height: position.height + HEIGHT_WITH_LINEHEIGHT,
        left: position.left - SHADOW_WIDTH,
        top: position.top - HALF_LINEHEIGHT,
        text: elem.textContent.trim(),
      },
    };
  }

  selectElementContents(elem) {
    const range = document.createRange();
    range.selectNodeContents(elem);
    const sel = window.getSelection();
    sel.removeAllRanges();
    sel.addRange(range);
  }

  getHrefForUntitledDatasetConfig = (
    dottedFullPath,
    newVersion,
    doNotWaitJobCompletion
  ) => {
    const apiCall = new APIV2Call().paths("/datasets/new_untitled").params({
      parentDataset: dottedFullPath,
      newVersion,
      limit: doNotWaitJobCompletion ? 0 : ROWS_LIMIT,
    });

    return apiCall.toPath();
  };

  getAPICallForUntitledDatasetConfig = (
    dottedFullPath,
    newVersion,
    doNotWaitJobCompletion
  ) => {
    const apiCall = new APIV2Call().paths("/datasets/new_untitled").params({
      parentDataset: dottedFullPath,
      newVersion,
      limit: doNotWaitJobCompletion ? 0 : ROWS_LIMIT,
    });

    return apiCall;
  };

  getHrefForDatasetConfig = (resourcePath) =>
    `${resourcePath}?view=explore&limit=50`;

  getDatasetMetadataLink = (datasetPath, sessionId, version) => {
    const apiCall = new APIV2Call()
      .paths(`dataset/${datasetPath.join(".")}/version/${version}/preview`)
      .params({
        view: "explore",
        limit: 0,
        triggerJob: false,
      });

    // TODO: check if getRefQueryParamsFromDataset is needed here

    if (sessionId) {
      apiCall.params({ sessionId });
    }

    return apiCall.toPath();
  };

  getPreviewLink = (dataset, tipVersion, sessionId, willLoadTable) => {
    const apiCall = new APIV2Call()
      .paths(`${dataset.getIn(["apiLinks", "self"])}/preview`)
      .params({
        view: "explore",
        limit: 0,
      });

    if (tipVersion) {
      apiCall.params({ tipVersion });
    } else {
      apiCall.params(getRefQueryParamsFromDataset(dataset.get("fullPath")));
    }

    if (sessionId) {
      apiCall.params({ sessionId });
    }

    if (!willLoadTable) {
      apiCall.params({ triggerJob: false });
    }

    return apiCall.toPath();
  };

  getReviewLink = (dataset, tipVersion) => {
    const apiCall = new APIV2Call()
      .paths(`${dataset.getIn(["apiLinks", "self"])}/review`)
      .params({
        jobId: dataset.get("jobId"),
        view: "explore",
        limit: 0,
      });

    if (tipVersion) {
      apiCall.params({ tipVersion });
    }

    return apiCall.toPath();
  };

  getHrefForTransform({ resourceId, tableId }, location) {
    // TODO this should be moved into dataset decorator
    const { version } = location.query;
    const fullPath =
      location.query.mode === "edit"
        ? `${encodeURIComponent(`"${resourceId}"`)}.${encodeURIComponent(
            tableId
          )}`
        : "tmp.UNTITLED";
    const resourcePath = `dataset/${fullPath}`;

    const apiCall = new APIV2Call().paths(`${resourcePath}/version/${version}`);

    return apiCall.toPath();
  }

  getNewDatasetVersion = () => "000" + rand();

  getFullPath = ({ resourceId, tableId, mode }) =>
    mode === "edit" ? `${resourceId}.${tableId}` : "tmp.UNTITLED";

  getVersionedResoursePath = ({ resourceId, tableId, version, mode }) => {
    const fullPath = this.getFullPath({ resourceId, tableId, mode });
    const resourcePath = `/dataset/${fullPath}`;
    return `${resourcePath}/version/${version}`;
  };

  getPreviewTransformationLink(dataset, newVersion) {
    const end = `transformAndPreview?newVersion=${encodeURIComponent(
      newVersion
    )}&limit=0`;
    return `${dataset.getIn(["apiLinks", "self"])}/${end}`;
  }

  getTransformPeekHref(dataset) {
    const newVersion = this.getNewDatasetVersion();
    const end = `transformPeek?newVersion=${encodeURIComponent(
      newVersion
    )}&limit=${ROWS_LIMIT}`;
    return `${dataset.getIn(["apiLinks", "self"])}/${end}`;
  }

  getRunTransformationLink({ fullPath, version, newVersion, type }) {
    return `/dataset/${fullPath.join(
      "."
    )}/version/${version}/${type}?newVersion=${encodeURIComponent(
      newVersion
    )}&limit=${ROWS_LIMIT}`;
  }

  getUntitledSqlHref({ newVersion, sessionId }) {
    if (sessionId) {
      return `/datasets/new_untitled_sql?newVersion=${encodeURIComponent(
        newVersion
      )}&sessionId=${sessionId}&limit=0`;
    } else {
      return `/datasets/new_untitled_sql?newVersion=${encodeURIComponent(
        newVersion
      )}&limit=0`;
    }
  }

  getTmpUntitledSqlHref({ newVersion, sessionId }) {
    if (sessionId) {
      return `/datasets/new_tmp_untitled_sql?newVersion=${encodeURIComponent(
        newVersion
      )}&sessionId=${sessionId}&limit=0`;
    } else {
      return `/datasets/new_tmp_untitled_sql?newVersion=${encodeURIComponent(
        newVersion
      )}&limit=0`;
    }
  }

  getUntitledSqlAndRunHref({ newVersion, sessionId }) {
    // does not need limit=0 as does not return data in all cases
    if (sessionId) {
      return `/datasets/new_untitled_sql_and_run?newVersion=${encodeURIComponent(
        newVersion
      )}&sessionId=${sessionId}`;
    } else {
      return `/datasets/new_untitled_sql_and_run?newVersion=${encodeURIComponent(
        newVersion
      )}`;
    }
  }

  getTmpUntitledSqlAndRunHref({ newVersion, sessionId }) {
    if (sessionId) {
      return `/datasets/new_tmp_untitled_sql_and_run?newVersion=${encodeURIComponent(
        newVersion
      )}&sessionId=${sessionId}`;
    } else {
      return `/datasets/new_tmp_untitled_sql_and_run?newVersion=${encodeURIComponent(
        newVersion
      )}`;
    }
  }

  getMappedDataForTransform(item, detailsType) {
    if (detailsType === "transform") {
      return item;
    }
    return (
      (transformationMapperHash[detailsType || item.type] &&
        transformationMapperHash[detailsType || item.type](
          item,
          item.transformType
        )) ||
      item
    );
  }

  hasOrigin(dataset, location) {
    if (dataset.get("isNewQuery")) {
      return true;
    }
    const { query } = location;
    return (
      query.mode === "edit" ||
      dataset.get("canReapply") ||
      query.hasOrigin === "true"
    );
  }

  needSelection(method, transformType) {
    // in props.transform we can get transformType = 'extract' and method = "Values"
    // it happens when we switch between "Keep only" and "Extract"
    if (transformType === "extract") {
      return true;
    }

    return notNeedSelectionMethod.indexOf(method) === -1;
  }

  needsToLoadCardFormValuesFromServer(transform) {
    if (!transform) {
      return true;
    }
    const columnType = transform.get("columnType");
    return !(
      transform.get("transformType") === "extract" &&
      (columnType === LIST || columnType === MAP || columnType === STRUCT)
    );
  }

  getDefaultSelection = (columnName) => ({
    cellText: "",
    length: 0,
    offset: 0,
    columnName,
  });

  getDocumentWidth = () => $(document).width();

  _createFakeElement = (text) => {
    const fakeElement = document.createElement("textarea");
    fakeElement.value = text;
    document.body.appendChild(fakeElement);
    return fakeElement;
  };

  copySelection = (elementOrText) => {
    if (elementOrText === null) return;

    const isText = typeof elementOrText === "string";
    const text = isText ? elementOrText : elementOrText.innerText;
    // for some strange reason it works only in async mode. See clipboardUtils for details
    window.setTimeout(copyTextToClipboard.bind(this, text), 1); // copyTextToClipboard(text);
  };

  escapeFieldNameForSQL(value) {
    return `"${value.replace(/"/g, '""')}"`;
  }

  isFilterIncludedInColumnName(column, filter) {
    if (!column) return false;
    const name = column.get("name");
    // empty string is treated as included, null or undefined filter - not.
    return name && name.toUpperCase().includes(filter.toUpperCase());
  }

  getFilteredColumns(columns, columnFilter) {
    if (!columns || !columns.size || !columnFilter) return columns;

    return columns.filter((column) => {
      return this.isFilterIncludedInColumnName(column, columnFilter);
    });
  }

  getFilteredColumnCount(columns, columnFilter) {
    if (!columns) return 0;
    if (!columnFilter) return columns.size;

    return columns.reduce((count, column) => {
      if (this.isFilterIncludedInColumnName(column, columnFilter)) {
        count += 1;
      }
      return count;
    }, 0);
  }

  isSqlEditorTab(location) {
    const projectId = getSonarContext()?.getSelectedProjectId?.();
    if (!location?.pathname) return false;
    const curLocation = location.pathname;
    return (
      curLocation.includes(
        sqlPaths.unsavedDatasetPath.link({
          projectId,
        })
      ) || curLocation.includes(newQuery())
    );
  }

  isExploreDatasetPage(location) {
    if (!location?.pathname) return false;
    const curLocation = rmProjectBase(location.pathname);
    return (
      curLocation.startsWith("/space") ||
      curLocation.startsWith("/home") ||
      curLocation.startsWith("/source")
    );
  }

  isExploreSourcePage(location) {
    if (!location?.pathname) return false;
    const curLocation = rmProjectBase(location.pathname);
    return curLocation.startsWith("/source");
  }

  getCancellable = (jobStatus) => {
    return (
      jobStatus === JOB_STATUS.running ||
      jobStatus === JOB_STATUS.starting ||
      jobStatus === JOB_STATUS.enqueued ||
      jobStatus === JOB_STATUS.pending ||
      jobStatus === JOB_STATUS.metadataRetrieval ||
      jobStatus === JOB_STATUS.planning ||
      jobStatus === JOB_STATUS.engineStart ||
      jobStatus === JOB_STATUS.queued ||
      jobStatus === JOB_STATUS.executionPlanning
    );
  };

  getIfInEntityHistory(history, historyItem, datasetUI, version) {
    if (!history || !historyItem || !datasetUI || !version) {
      return [false, true];
    }

    let isInVersionHistory = false;
    let hasPhysicalDatasetOrigin = false;

    const versions = historyItem.toJS();
    isInVersionHistory = versions[version] && Object.keys(versions).length > 1;
    const historyList = history.getIn([version, "items"]);
    if (!historyList) {
      return [false, true];
    }
    const originalDatasetHistory = datasetUI.get(
      historyList.get(historyList.size - 1)
    );
    hasPhysicalDatasetOrigin =
      originalDatasetHistory &&
      PHYSICAL_DATASET_TYPES.has(originalDatasetHistory.get("datasetType"));

    return [
      isInVersionHistory && !hasPhysicalDatasetOrigin,
      isInVersionHistory && hasPhysicalDatasetOrigin,
    ];
  }

  isEditedScript(activeScript, currentSql) {
    return (
      activeScript && activeScript.id && activeScript.content !== currentSql
    );
  }

  hasPermissionToModify(activeScript) {
    return (
      activeScript &&
      activeScript.permissions &&
      activeScript.permissions.includes("MODIFY")
    );
  }

  createNewQueryFromDatasetOverlay(datasetPath) {
    return datasetPath ? `SELECT * FROM ${datasetPath}` : "";
  }
}

export default new ExploreUtils();
