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
import classNames from "clsx";
import { base } from "@app/uiTheme/less/Aggregate/AggregateForm.less";
import {
  getColumnByName,
  isAlreadySelected,
} from "utils/explore/aggregateUtils";
import ColumnDragItem from "utils/ColumnDragItem";

import AggregateContent from "./AggregateContent";
import AggregateHeader from "./AggregateHeader";
import AggregateFooter from "./AggregateFooter";
import { SpinnerOverlay } from "dremio-ui-lib/components";

class AggregateForm extends Component {
  static getFields = () => [
    "columnsDimensions[].column",
    "columnsMeasures[].measure",
    "columnsMeasures[].column",
  ];

  static validate = () => {
    return {};
  };

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map), // NOTE: some users pass a fake DS with just displayFullPath
    fields: PropTypes.object,
    values: PropTypes.object,
    columns: PropTypes.instanceOf(Immutable.List),
    canSelectMeasure: PropTypes.bool,
    // when `false` field can be selected only once
    canUseFieldAsBothDimensionAndMeasure: PropTypes.bool,
    location: PropTypes.object,
    style: PropTypes.object,
    contentStyle: PropTypes.object,
    canAlter: PropTypes.any,
    className: PropTypes.any,
    loadingRecommendations: PropTypes.bool,
    isTriggerRecommendation: PropTypes.bool,
  };

  static defaultProps = {
    canSelectMeasure: true,
    canUseFieldAsBothDimensionAndMeasure: true,
  };

  constructor(props) {
    super(props);
    this.state = {
      isDragInProgress: false,
      dragItem: new ColumnDragItem(),
    };
  }

  onDragStart = (e = {}) => {
    this.setState({
      isDragInProgress: true,
      dragItem: new ColumnDragItem(e.id, e.type),
    });
  };

  handleDrop = (dragOrigin, dropData) => {
    const { columns, fields } = this.props;
    const { columnsMeasures, columnsDimensions } = fields;
    const { id: columnName } = dropData;
    const columnToAdd = getColumnByName(columns, columnName);
    const newColumn = {
      column: columnName,
    };

    if (dragOrigin !== dropData.type) {
      if (dragOrigin === "measures") {
        if (dropData.type === "dimensions") {
          columnsDimensions.removeField(dropData.index);
        }
        newColumn.measure = [
          "FLOAT",
          "DECIMAL",
          "INTEGER",
          "BIGINT",
          "DOUBLE",
        ].includes(columnToAdd && columnToAdd.getIn(["type", "name"]))
          ? "Sum"
          : "Count";
        columnsMeasures.addField(newColumn);
      } else if (!isAlreadySelected(fields.columnsDimensions, columnName)) {
        if (dropData.type === "measures") {
          columnsMeasures.removeField(dropData.index);
        }
        columnsDimensions.addField(newColumn);
      }
    }

    this.stopDrag();
  };

  addAnother = (type) => {
    const { fields } = this.props;
    if (type === "measures") {
      fields.columnsMeasures.addField({ measure: "Sum" });
    } else {
      fields.columnsDimensions.addField({});
    }
  };

  removeAllFields = (fields) => {
    const runLoop = async () => {
      for (let i = 0; i < fields.length; i++) {
        await fields.removeField(0);
      }
    };

    runLoop();
  };

  handleClearAllDimensions = () => {
    const {
      fields: { columnsDimensions },
    } = this.props;
    this.removeAllFields(columnsDimensions);
  };

  handleClearAllMeasures = () => {
    const {
      fields: { columnsMeasures },
    } = this.props;
    this.removeAllFields(columnsMeasures);
  };

  stopDrag = () => {
    this.setState({
      isDragInProgress: false,
      dragItem: new ColumnDragItem(),
    });
  };

  render() {
    const {
      style,
      dataset,
      fields,
      values,
      canSelectMeasure,
      canUseFieldAsBothDimensionAndMeasure,
      columns,
      canAlter,
      className,
      location,
      loadingRecommendations,
      isTriggerRecommendation,
    } = this.props;
    return (
      <div
        className={classNames("aggregate-form", base, className)}
        style={{
          ...(style ?? {}),
          ...(isTriggerRecommendation && { position: "relative" }),
        }}
      >
        {loadingRecommendations && isTriggerRecommendation && (
          <SpinnerOverlay
            in
            className="AccelerationAggregate__AggregateForm__loading"
          />
        )}
        <AggregateHeader
          dataset={dataset}
          onClearAllDimensions={this.handleClearAllDimensions}
          onClearAllMeasures={this.handleClearAllMeasures}
          location={location}
        />
        <AggregateContent
          fields={fields}
          values={values}
          canSelectMeasure={canSelectMeasure}
          canUseFieldAsBothDimensionAndMeasure={
            canUseFieldAsBothDimensionAndMeasure
          }
          handleDragStart={this.onDragStart}
          onDragEnd={this.stopDrag}
          onDrop={this.handleDrop}
          isDragInProgress={this.state.isDragInProgress}
          dragItem={this.state.dragItem}
          dragType="aggregate"
          allColumns={columns}
          canAlter={canAlter}
        />
        <AggregateFooter addAnother={this.addAnother} />
      </div>
    );
  }
}

export default AggregateForm;
