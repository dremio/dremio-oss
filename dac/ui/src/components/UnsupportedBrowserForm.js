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
import { Button } from "dremio-ui-lib/components";
import FontIcon from "components/Icon/FontIcon";
import LoginTitle from "pages/AuthenticationPage/components/LoginTitle";
import { intl } from "@app/utils/intl";

class UnsupportedBrowserForm extends Component {
  static propTypes = {
    approveBrowser: PropTypes.func,
  };
  renderWarning() {
    return (
      <span style={{ display: "flex", alignItems: "center" }}>
        <FontIcon type="WarningSolid" theme={styles.theme} />
        <span>
          {la(
            "Dremio works best in the latest versions of Chrome, Safari, Firefox, Edge, and Internet\u00A0Explorer."
          )}
        </span>
      </span>
    );
  }
  render() {
    return (
      <div className="page" style={styles.base}>
        <div style={styles.form}>
          <LoginTitle subTitle={this.renderWarning()} />
          <Button
            variant="primary"
            onClick={this.props.approveBrowser}
            key="details-wizard-next"
          >
            {intl.formatMessage({ id: "Common.OK" })}
          </Button>
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    justifyContent: "center",
    alignItems: "center",
    backgroundColor: "#2A394A",
    overflow: "hidden",
  },
  form: {
    position: "relative",
    backgroundColor: "#344253",
    padding: 40,
  },
  theme: {
    Icon: {
      width: 70,
      height: 70,
    },
  },
};
export default UnsupportedBrowserForm;
