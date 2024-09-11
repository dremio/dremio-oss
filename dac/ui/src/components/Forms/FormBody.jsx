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
import PropTypes from "prop-types";
import classNames from "clsx";

import { formBody } from "uiTheme/less/forms.less";

const FormBody = (props) => {
  const { className, children, dataQa, style } = props;

  const rootClass = classNames(formBody, { [className]: className });
  return (
    <div className={rootClass} style={style} data-qa={dataQa}>
      {children}
    </div>
  );
};

FormBody.propTypes = {
  style: PropTypes.object,
  className: PropTypes.string,
  children: PropTypes.node,
  dataQa: PropTypes.string,
};

export default FormBody;
