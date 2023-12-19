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
import PropTypes from "prop-types";
import Immutable from "immutable";
import FontIcon from "components/Icon/FontIcon";
import { modalFormProps } from "components/Forms";
import { Toggle } from "components/Fields";
import Message from "components/Message";

import "@app/uiTheme/less/commonModifiers.less";
import "@app/uiTheme/less/Acceleration/Acceleration.less";
import { commonThemes } from "../commonThemes";
import LayoutInfo from "../LayoutInfo";
import AccelerationAggregate from "./AccelerationAggregate";

export class AccelerationBasic extends Component {
  static getFields() {
    return AccelerationAggregate.getFields();
  }
  static validate(values) {
    return AccelerationAggregate.validate(values);
  }
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    location: PropTypes.object.isRequired,
    fields: PropTypes.object,
    handleSubmit: PropTypes.func,
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    loadingRecommendations: PropTypes.bool,
    skipRecommendations: PropTypes.func,
    canAlter: PropTypes.any,
  };

  static contextTypes = {
    reflectionSaveErrors: PropTypes.instanceOf(Immutable.Map).isRequired,
  };

  getHighlightedSection() {
    const { layoutId } = this.props.location.state || {};
    if (!layoutId) return null;

    return this.props.reflections.getIn([layoutId, "type"]);
  }

  render() {
    const {
      fields,
      location,
      reflections,
      dataset,
      loadingRecommendations,
      skipRecommendations,
      canAlter,
    } = this.props;

    if (!fields.rawReflections.length || !fields.aggregationReflections.length)
      return null; // Form still initializing

    const { enabled } = fields.rawReflections[0];
    const toggleLabel = (
      <h3 className={"AccelerationBasic__toggleLabel"}>
        <FontIcon type="RawMode" theme={commonThemes.rawIconTheme} />
        {laDeprecated("Raw Reflections")}
      </h3>
    );

    const firstRawLayout = reflections.find((r) => r.get("type") === "RAW");
    const firstAggLayout = reflections.find(
      (r) => r.get("type") === "AGGREGATION"
    );
    const highlightedSection = this.getHighlightedSection();

    // if this error is encountered, no error message should be rendered
    const SUPPORT_ERROR =
      "Permission denied. A support user cannot create a reflection";

    const rawError = this.context.reflectionSaveErrors.get(
      fields.rawReflections[0].id.value
    );
    const rawErrorInfo = rawError?.get("message")?.get("errorMessage");
    const rawErrorMessage = rawError && rawErrorInfo !== SUPPORT_ERROR && (
      <Message
        messageType="error"
        inFlow={false}
        message={rawError.get("message")}
        messageId={rawError.get("id")}
        className={"AccelerationBasic__message"}
      />
    );

    const aggError = this.context.reflectionSaveErrors.get(
      fields.aggregationReflections[0].id.value
    );

    let errorMessageInfo;
    const aggErrorMessage = aggError?.get("message");

    if (aggErrorMessage) {
      if (typeof aggErrorMessage === "string") {
        errorMessageInfo = aggErrorMessage;
      } else {
        errorMessageInfo = aggError.get("message").get("errorMessage");
      }
    }

    const errorMessageComponent = aggError &&
      errorMessageInfo !== SUPPORT_ERROR && (
        <Message
          messageType="error"
          inFlow={false}
          message={aggError.get("message")}
          messageId={aggError.get("id")}
          className={"AccelerationBasic__message"}
        />
      );

    return (
      <div className={"AccelerationBasic"} data-qa="raw-basic">
        <div className={"AccelerationBasic__header"}>
          <div
            className={`AccelerationBasic__toggleLayout ${
              highlightedSection === "RAW" ? "--bgColor-highlight" : null
            }`}
            data-qa="raw-queries-toggle"
          >
            <Toggle
              {...enabled}
              label={toggleLabel}
              className={"AccelerationBasic__toggle"}
            />
            <LayoutInfo layout={firstRawLayout} />
          </div>
          <div className={"position-relative"}>{rawErrorMessage}</div>
        </div>
        <AccelerationAggregate
          {...modalFormProps(this.props)}
          canAlter={canAlter}
          dataset={dataset}
          reflection={firstAggLayout}
          fields={fields}
          className={"AccelerationBasic__AccelerationAggregate"}
          location={location}
          shouldHighlight={highlightedSection === "AGGREGATION"}
          errorMessage={errorMessageComponent}
          loadingRecommendations={loadingRecommendations}
          skipRecommendations={skipRecommendations}
        />
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  const location = state.routing.locationBeforeTransitions;
  return {
    location,
  };
};

export default connect(mapStateToProps)(AccelerationBasic);
