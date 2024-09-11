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
import { Button } from "dremio-ui-lib/components";

import AggregateFooterMixin from "dyn-load/components/Aggregate/AggregateFooterMixin";
import { mapStateToProps } from "dyn-load/components/Aggregate/AggregateFooterMixin";
@AggregateFooterMixin
class AggregateFooter extends PureComponent {
  static propTypes = {
    addAnother: PropTypes.func,
  };

  render() {
    return !this.checkToRenderFooter() ? (
      <div className="h-4 w-full"></div>
    ) : (
      <div className="flex w-full">
        <div style={{ minWidth: 240 }}></div>
        <div className="h-4 w-full">
          <Button
            variant="tertiary"
            data-qa="add-dimension"
            onClick={this.props.addAnother.bind(this, "dimensions")}
          >
            <dremio-icon class="mr-05" name="interface/add"></dremio-icon>
            <span>{laDeprecated("Add a Dimension")}</span>
          </Button>
        </div>
        <div className="h-4 w-full">
          <Button
            variant="tertiary"
            data-qa="add-measure"
            onClick={this.props.addAnother.bind(this, "measures")}
          >
            <dremio-icon class="mr-05" name="interface/add"></dremio-icon>
            <span>{laDeprecated("Add a Measure")}</span>
          </Button>
        </div>
      </div>
    );
  }
}

export default connect(mapStateToProps)(AggregateFooter);
