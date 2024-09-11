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

export const RESERVED = ["support.email.addr", "support.email.jobs.subject"];

const InternalSupportEmail = (props) => {
  return (
    <div
      style={{
        padding: "10px 0 20px",
        borderBottom: "1px solid var(--border--neutral)",
      }}
    >
      <h3
        className="text-semibold text-xl"
        style={{ lineHeight: "28px", height: "32px" }}
      >
        {laDeprecated("Internal Support Email")}
      </h3>
      <p className="mb-1">
        {laDeprecated("Users will see changes when they next reload.")}
      </p>
      {props.renderSettings("support.email.addr", { allowEmpty: true })}
      {props.renderSettings("support.email.jobs.subject", { allowEmpty: true })}
    </div>
  );
};

InternalSupportEmail.propTypes = {
  descriptionStyle: PropTypes.object,
  renderSettings: PropTypes.func,
};

export default InternalSupportEmail;
