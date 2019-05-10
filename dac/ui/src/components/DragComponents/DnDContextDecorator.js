/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import HTML5Backend from 'react-dnd-html5-backend';
import { DragDropContext } from 'react-dnd';

/**
 * Use this decorator to if you need dnd functionality. This decorator is applied to main {@see App.js}.
 * In 90% is sufficient to use {@see DragTarget} and {@see DropTarget} without additional set up.
 * But in some cases you need decorate a container with DnDContextDecorator. For example
 * {@see CellPopover}. My guess that it is required, because Popover uses portals and by some
 * reason context from main App does not propogate to popover
 */
export const DnDContextDecorator = DragDropContext(HTML5Backend);
