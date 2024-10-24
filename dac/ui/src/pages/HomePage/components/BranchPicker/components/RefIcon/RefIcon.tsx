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
import { COMMIT_TYPE } from "#oss/constants/nessie";
import { Reference } from "#oss/types/nessie";
import { getIconByType } from "#oss/utils/nessieUtils";

type RefIconProps = {
  reference: Reference;
  hash?: string | null;
  style?: any;
};

function RefIcon({
  reference,
  hash,
  style = { width: "16px", height: "16px" },
}: RefIconProps) {
  return (
    <dremio-icon
      name={getIconByType(reference.type, hash)}
      alt={hash ? COMMIT_TYPE : reference.type}
      style={style}
    />
  );
}

export default RefIcon;
