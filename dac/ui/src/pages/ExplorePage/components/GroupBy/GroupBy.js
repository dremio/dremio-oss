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
import Immutable from "immutable";
import {
  connectComplexForm,
  InnerComplexForm,
} from "components/Forms/connectComplexForm";
import DefaultWizardFooter from "components/Wizards/components/DefaultWizardFooter";

import AggregateForm from "components/Aggregate/AggregateForm";

const SECTIONS = [AggregateForm];

export class GroupBy extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    changeFormType: PropTypes.func,
    fields: PropTypes.object,
    columns: PropTypes.instanceOf(Immutable.List),
    location: PropTypes.object,
    error: PropTypes.object,
    canSelect: PropTypes.any,
  };

  render() {
    const { canSelect, submit } = this.props;
    return (
      <div className="group-by">
        <InnerComplexForm {...this.props} onSubmit={submit}>
          <AggregateForm {...this.props} type="groupBy" canAlter={canSelect} />
          <DefaultWizardFooter
            style={{ marginTop: 10 }}
            {...this.props}
            onFormSubmit={submit}
          />
        </InnerComplexForm>
      </div>
    );
  }
}

const mapStateToProps = (ownProps) => {
  const { location } = ownProps;

  if (location && location.state.columnName) {
    const column = {
      column: location.state.columnName,
      type: location.state.columnType,
    };
    return {
      initialValues: {
        columnsDimensions: [column],
      },
    };
  }
};

export default connectComplexForm(
  {
    form: "groupBy",
  },
  SECTIONS,
  mapStateToProps,
  null
)(GroupBy);
