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

import { createReflectionFormValues } from "utils/accelerationUtils";
import AccelerationRawMixin from "@inject/components/Acceleration/Advanced/AccelerationRawMixin";
import { Button } from "dremio-ui-lib/components";
import "@app/uiTheme/less/Acceleration/Acceleration.less";
import AccelerationGridController from "./AccelerationGridController";

@AccelerationRawMixin
export default class AccelerationRaw extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    fields: PropTypes.object,
    canAlter: PropTypes.any,
    allowPartitionTransform: PropTypes.bool,
    rawRecommendation: PropTypes.object,
    loadingRecommendations: PropTypes.bool,
  };

  static getFields() {
    return [
      "rawReflections[].id",
      "rawReflections[].tag",
      "rawReflections[].type",
      "rawReflections[].name",
      "rawReflections[].enabled",
      "rawReflections[].arrowCachingEnabled",
      "rawReflections[].partitionDistributionStrategy",
      "rawReflections[].partitionFields[].name",
      "rawReflections[].sortFields[].name",
      "rawReflections[].displayFields[].name",
      "rawReflections[].distributionFields[].name",
      "rawReflections[].shouldDelete",
    ];
  }

  static validate() {
    return {};
  }

  addNewLayout = () => {
    const { allowPartitionTransform, rawRecommendation } = this.props;
    const { rawReflections } = this.props.fields;

    const reflection = createReflectionFormValues(
      allowPartitionTransform && rawRecommendation
        ? rawRecommendation
        : {
            type: "RAW",
          },
      rawReflections.map((e) => e.name.value),
    );

    rawReflections.addField(reflection);
  };

  renderHeader = () => {
    return (
      <div className="AccelerationRaw__header">
        <h3 className="AccelerationRaw__toggleLabel">
          <dremio-icon
            style={{
              blockSize: 24,
              inlineSize: 24,
            }}
            class="mx-1"
            name="interface/reflection-raw-mode"
          ></dremio-icon>
          {laDeprecated("Raw Reflections")}
        </h3>
        {this.checkIfButtonShouldBeRendered() && (
          <Button
            variant="secondary"
            className="mr-05"
            onClick={this.addNewLayout}
          >
            {laDeprecated("New Reflection")}
          </Button>
        )}
      </div>
    );
  };

  render() {
    const {
      dataset,
      reflections,
      fields: { rawReflections },
      canAlter,
      loadingRecommendations,
    } = this.props;
    return (
      <div className={"AccelerationRaw"}>
        {this.renderHeader()}
        <AccelerationGridController
          canAlter={canAlter}
          dataset={dataset}
          reflections={reflections}
          layoutFields={rawReflections}
          loadingRecommendations={loadingRecommendations}
          activeTab="raw"
        />
      </div>
    );
  }
}
