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
import { connect } from "react-redux";
import PropTypes from "prop-types";
import classNames from "classnames";
import FontIcon from "@app/components/Icon/FontIcon";

import AggregateFooterMixin from "dyn-load/components/Aggregate/AggregateFooterMixin.js";
import { mapStateToProps } from "dyn-load/components/Aggregate/AggregateFooterMixin.js";
import { displayNone } from "@app/uiTheme/less/commonStyles.less";
import {
  add,
  base,
  center,
  left,
  right,
} from "@app/uiTheme/less/Aggregate/AggregateFooter.less";
@AggregateFooterMixin
class AggregateFooter extends PureComponent {
  static propTypes = {
    addAnother: PropTypes.func,
  };

  render() {
    const conditionalRenderingOfButton = this.checkToRenderFooter();
    return (
      <div className={classNames("aggregate-Footer", base)}>
        <div className={left}></div>
        <div className={center}>
          <div
            className={conditionalRenderingOfButton ? add : displayNone}
            data-qa="add-dimension"
            onClick={this.props.addAnother.bind(this, "dimensions")}
          >
            {" "}
            {/* todo: ax, consistency: button */}
            <FontIcon type="Add" />
            <span>{la("Add a Dimension")}</span>
          </div>
        </div>
        <div className={right}>
          <div
            className={conditionalRenderingOfButton ? add : displayNone}
            data-qa="add-measure"
            onClick={this.props.addAnother.bind(this, "measures")}
          >
            {" "}
            {/* todo: ax, consistency: button */}
            <FontIcon type="Add" />
            <span>{la("Add a Measure")}</span>
          </div>
        </div>
      </div>
    );
  }
}

export default connect(mapStateToProps)(AggregateFooter);
