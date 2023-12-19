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

import {
  base,
  left,
  center,
  right,
} from "@app/uiTheme/less/Aggregate/AggregateHeader.less";
import SimpleButton from "components/Buttons/SimpleButton";
import EllipsedText from "components/EllipsedText";
import { ExploreInfoHeader } from "pages/ExplorePage/components/ExploreInfoHeader";
import { AggregateHeaderWithMixin } from "@inject/components/Aggregate/AggregateHeaderMixin.js";
import * as classes from "@app/uiTheme/radium/replacingRadiumPseudoClasses.module.less";

export class AggregateHeader extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    onClearAllMeasures: PropTypes.func,
    onClearAllDimensions: PropTypes.func,
    location: PropTypes.object,
  };

  /**
   * Renders `Clear All` button when needed, otherwise nothing
   *
   * @param  {Function} clearFunction clear items function to be called on button click
   * @return {React.Element} element to render
   */
  renderClearAll(clearFunction) {
    const renderClearAllButtons = this.checkToRenderClearAllConditionally();
    if (clearFunction) {
      return (
        <SimpleButton
          type="button"
          buttonStyle="secondary"
          className={classes["secondaryButtonPsuedoClasses"]}
          // DX-34369: all SimpleButton usage need to change from style to classname
          style={
            renderClearAllButtons
              ? { minWidth: "auto", height: 20, marginRight: 5 }
              : { display: "none" }
          }
          onClick={clearFunction}
        >
          {laDeprecated("Clear All")}
        </SimpleButton>
      );
    }
    return null;
  }

  render() {
    // todo: loc
    const nameForDisplay = ExploreInfoHeader.getNameForDisplay(
      this.props.dataset,
      {},
      this.props.location
    );
    return (
      <div className={classNames("aggregate-header", base)}>
        <div className={left}>
          <EllipsedText text={`“${nameForDisplay}” dataset columns:`} />
        </div>
        <div className={center}>
          {laDeprecated("Dimensions")}
          {this.renderClearAll(this.props.onClearAllDimensions)}
        </div>
        <div className={right}>
          {laDeprecated("Measures")}
          {this.renderClearAll(this.props.onClearAllMeasures)}
        </div>
      </div>
    );
  }
}
export default AggregateHeaderWithMixin(AggregateHeader);
