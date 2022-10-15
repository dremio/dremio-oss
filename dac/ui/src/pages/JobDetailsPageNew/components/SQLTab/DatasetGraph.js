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
import { useEffect, useState } from "react";
import ReactFlow, { isNode } from "react-flow-renderer";
import dagre from "dagre";
import { injectIntl } from "react-intl";
import { Tooltip } from "dremio-ui-lib";
import { getIconPath } from "@app/utils/getIconPath";
import PropTypes from "prop-types";
import TextWithHelp from "@app/components/TextWithHelp";
import {
  getColorCode,
  initialElements,
  getSortedReflectionsData,
} from "./Utils";
import "./DatasetGraph.less";

const graphPadding = { padding: 15 };

const onLayout = (
  nodeElements,
  dagreGraph,
  direction,
  nodesWithParent,
  nodeColorFlags,
  duplicateNodes,
  algebricNodes
) => {
  dagreGraph.setGraph({ rankdir: direction });
  nodeElements.forEach((node) => {
    let getAnimated = false;
    //setup the edges for all nodes
    if (isNode(node)) {
      dagreGraph.setNode(node.id, { width: 150, height: 265 });
      if (
        node.dataSet &&
        node.dataSet.reflectionsDefinedList &&
        node.dataSet.reflectionsDefinedList.length > 0
      ) {
        node.dataSet.reflectionsDefinedList.map((item) => {
          if (item.isUsed) {
            nodeColorFlags[node.id] = true;
          }
          if (item.reflectionMatchingType === "ALGEBRAIC") {
            getAnimated = true;
            algebricNodes[node.id] = true;
          }
        });
      }
      if (node.parentNodeIdList && node.parentNodeIdList.length > 0) {
        node.parentNodeIdList.map((item) => {
          if (nodeColorFlags[node.id]) {
            nodeColorFlags[item] = true;
          }
          const duplicateLabel = duplicateNodes[item]
            ? duplicateNodes[item] + 1
            : "";
          dagreGraph.setEdge(node.id, item);
          nodeElements.push({
            id: node.id + item,
            source: node.id,
            target: item,
            label: `${duplicateLabel}`,
          });
        });
      }
      if (!nodesWithParent[node.id]) {
        const duplicateLabel = duplicateNodes[node.id]
          ? duplicateNodes[node.id] + 1
          : "";
        dagreGraph.setEdge("1", node.id);
        nodeElements.push({
          id: "1" + node.id,
          source: "1",
          target: node.id,
          label: `${duplicateLabel}`,
          animated: getAnimated,
        });
      }
    }
  });
  dagre.layout(dagreGraph);
  const nodeWidth = 160;
  const layoutedElements = nodeElements.map((node) => {
    if (isNode(node)) {
      const nodeWithPosition = dagreGraph.node(node.id);
      const datasetType =
        node.dataSet && node.dataSet.datasetType
          ? node.dataSet.datasetType
          : "";
      const initialPosition = {
        x: nodeWithPosition.x - nodeWidth / 2 + Math.random() / 1000,
        y: nodeWithPosition.y / 2,
      };
      // color codes based on dataSetType
      switch (datasetType) {
        case "PHYSICAL_DATASET":
          if (nodeColorFlags[node.id]) {
            node.style = getColorCode("PHYSICAL_DATASET_NDS");
          } else {
            node.style = getColorCode(datasetType);
          }
          node.position = initialPosition;
          break;
        case "VIRTUAL_DATASET":
          if (algebricNodes[node.id]) {
            node.style = getColorCode("ALGEBRIC");
          } else if (nodeColorFlags[node.id]) {
            node.style = getColorCode("VIRTUAL_DATASET_NDS");
          } else {
            node.style = getColorCode(datasetType);
          }
          node.position = initialPosition;
          break;
        default:
          node.position = initialPosition;
          node.style = getColorCode(datasetType);
          break;
      }
    }
    return node;
  });
  return layoutedElements;
};
const renderNodeLabel = (item) => {
  return item.sql ? (
    <TextWithHelp
      text={item.sql}
      helpText={item.dataSet.datasetName}
      showToolTip
      color="light"
    />
  ) : (
    item.dataSet.datasetName
  );
};

const DatasetGraph = ({
  intl: { formatMessage },
  datasetGraph: dataSetGraphData,
  algebricMatch,
}) => {
  const algebricMatchData = algebricMatch.toJS().reduce((obj, item) => {
    obj[item.datasetID]
      ? obj[item.datasetID].reflectionsDefinedList.push(
          ...item.reflectionsDefinedList
        )
      : (obj[item.datasetID] = { ...item });
    return obj;
  }, {});

  const algebricMatchReflection = Object.values(algebricMatchData).map(
    (item) => {
      return {
        id: item.datasetID,
        dataSet: item,
      };
    }
  );

  // leaf nodes with parent id will get added in nodesWithParent
  const nodesWithParent = {};
  /*
   nodeColorFlags are use to identify the PDS nodes which are
   scanned directly during Query with colorcode #A672BB and not
   directly scanned during Query with colorcode rgba(204, 178, 214, 1)
  */
  const nodeColorFlags = {};
  const duplicateNodes = {};
  const algebricNodes = {};
  const datasetGraph = dataSetGraphData.toJS();
  if (datasetGraph.length > 0) {
    datasetGraph.push({
      id: "1",
      dataSet: {
        datasetName: formatMessage({ id: "Common.ThisQuery" }),
      },
      parentNodeIdList: [datasetGraph[0].id],
    });
  }
  algebricMatchReflection.map((item) => datasetGraph.push(item));

  datasetGraph.map((item) => {
    item.data = {
      label: renderNodeLabel(item),
    };
    if (item.parentNodeIdList && item.parentNodeIdList.length > 0) {
      item.parentNodeIdList.map((id) => {
        nodesWithParent[id] = true;
      });
    }
    if (
      item.dataSet.reflectionsDefinedList &&
      item.dataSet.reflectionsDefinedList.length > 0
    ) {
      const reflectionData = getSortedReflectionsData(
        item.dataSet.reflectionsDefinedList
      );
      reflectionData.map((data) => {
        nodesWithParent[data.reflectionID] = true;
        // setup co-ordinates for reflectionType nodes
        item.data = {
          label: (
            <span className="reflectionData">
              <span>{item.data.label}</span>
              <span
                className={
                  data.isUsed
                    ? "reflectionData__contentRaw"
                    : "reflectionData__contentAgg"
                }
              >
                <Tooltip title="Job.Reflection">
                  {data.reflectionType === "RAW" ? (
                    <img
                      src={
                        data.isUsed
                          ? getIconPath("interface/reflection")
                          : getIconPath("interface/reflections-created-raw")
                      }
                      alt="Reflection"
                      className={
                        data.isUsed
                          ? "reflectionData__reflectionIcon"
                          : "reflectionData__reflectionRaw"
                      }
                    />
                  ) : (
                    <img
                      src={
                        data.isUsed
                          ? getIconPath("interface/reflections-filled-agg")
                          : getIconPath("interface/reflections-created-agg")
                      }
                      alt="Reflection"
                      className={
                        data.isUsed
                          ? "reflectionData__reflectionAggFilled"
                          : "reflectionData__reflectionAgg"
                      }
                    />
                  )}
                </Tooltip>
                <span style={{ textAlign: "start" }}>
                  {data.reflectionName}
                </span>
              </span>
            </span>
          ),
        };
      });
    }
  });
  const uniqueGraphNodes = datasetGraph.filter((item) => {
    const duplicateItemId = duplicateNodes[item.id];
    duplicateNodes[item.id] = duplicateItemId >= 0 ? duplicateItemId + 1 : 0;
    return !duplicateNodes[item.id] && item;
  });
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  const [nodeElements, setNodeElements] = useState(initialElements);
  const [reactFlowInstance, setReactFlowInstance] = useState(null);
  useEffect(() => {
    const layoutElements = onLayout(
      uniqueGraphNodes,
      dagreGraph,
      "TB",
      nodesWithParent,
      nodeColorFlags,
      duplicateNodes,
      algebricNodes
    );
    setNodeElements(layoutElements);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    //For the small graphs it will render as per the width height given as default
    if (reactFlowInstance && nodeElements.length && nodeElements.length > 20) {
      reactFlowInstance.fitView(graphPadding);
    }
  }, [reactFlowInstance, nodeElements]);

  const onLoad = (_reactFlowInstance) => {
    setReactFlowInstance(_reactFlowInstance);
  };
  return (
    <div className="datasetGraph">
      {datasetGraph.length ? (
        <ReactFlow
          elements={nodeElements}
          onLoad={onLoad}
          defaultZoom={1}
        ></ReactFlow>
      ) : (
        <div className="datasetGraph__errorDisplay">
          <span>{formatMessage({ id: "DatsetGraph.NoData" })}</span>
          <dremio-icon name="narwhal/query" class="datasetGraph__gnarlyIcon" />
        </div>
      )}
    </div>
  );
};
DatasetGraph.propTypes = {
  datasetGraph: PropTypes.object,
  intl: PropTypes.object.isRequired,
  algebricMatch: PropTypes.object,
};
export default injectIntl(DatasetGraph);
