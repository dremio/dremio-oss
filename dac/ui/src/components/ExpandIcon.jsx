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
import { formatMessage } from "../utils/locale";

const ExpandIcon = ({ expanded }) => {
  return (
    <div
      style={styles.wrapper}
      role="img"
      aria-label={formatMessage(`Common.${expanded ? "Collapse" : "Expand"}`)}
    >
      <dremio-icon
        name={
          expanded ? "interface/double-arrow-up" : "interface/double-arrow-down"
        }
        style={styles.icon}
      />
    </div>
  );
};

ExpandIcon.propTypes = {
  expanded: PropTypes.bool,
};

const styles = {
  wrapper: {
    display: "flex",
    alignItems: "center",
  },
  icon: {
    height: 18,
    width: 18,
    color: "var(--icon--primary)",
  },
};

export default ExpandIcon;
