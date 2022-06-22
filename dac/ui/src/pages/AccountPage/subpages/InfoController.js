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

import PropTypes from "prop-types";

import FormUnsavedRouteLeave from "components/Forms/FormUnsavedRouteLeave";
import ApiUtils from "utils/apiUtils/apiUtils";

import Info from "./Info";

export class InfoController extends PureComponent {
  static contextTypes = {
    router: PropTypes.object.isRequired,
    username: PropTypes.string,
  };

  static propTypes = {
    updateFormDirtyState: PropTypes.func, // comes from updateFormDirtyState
  };

  submit = (submitPromise) => {
    return ApiUtils.attachFormSubmitHandlers(submitPromise).then(() =>
      this.props.updateFormDirtyState(false)
    );
  };

  cancel = () => {
    this.context.router.goBack();
  };

  render() {
    return (
      <Info
        updateFormDirtyState={this.props.updateFormDirtyState}
        onFormSubmit={this.submit}
        cancel={this.cancel}
      />
    );
  }
}

export default FormUnsavedRouteLeave(InfoController);
