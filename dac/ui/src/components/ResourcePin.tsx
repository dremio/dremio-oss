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
import { useSelector, useDispatch } from "react-redux";
import classNames from "clsx";

import { stopPropagation } from "@app/utils/reactEventUtils";
import { setEntityActiveState as toggleActivePin } from "@app/reducers/home/pinnedEntities";
import { isEntityPinned } from "@app/selectors/home";
import { IconButton } from "dremio-ui-lib/components";

import "./ResourcePin.less";

type ResourcePinProps = {
  entityId: string;
};

const ResourcePin = (props: ResourcePinProps) => {
  const { entityId } = props;
  const isPinned = useSelector((state) => isEntityPinned(state, entityId));
  const dispatch = useDispatch();
  const pinClass = classNames("pin", { active: isPinned });

  const onPinClick = (e: any) => {
    stopPropagation(e);
    dispatch(toggleActivePin(entityId, !isPinned));
  };

  return (
    <IconButton className={pinClass} onClick={onPinClick} aria-label="Pin">
      {isPinned ? (
        <dremio-icon name="interface/pinned-small" alt="Pinned" />
      ) : (
        <dremio-icon name="interface/unpinned-small" alt="Unpinned" />
      )}
    </IconButton>
  );
};

export default ResourcePin;
