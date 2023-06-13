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
import { columnSorting, useExternalStoreState } from "leantable/react";
import { createTable } from "leantable/core";
import { useMemo } from "react";
import { useDetectScroll } from "dremio-ui-lib/components";

export const StoreSubscriber = (Component: any) => {
    return function WrappedComponent(props: any) {
        const enginesTable = useMemo(() => {
            return createTable([
                columnSorting(),
                (config: any) => ({
                    ...config,
                    getRowProps: (props: any) => {
                        return {
                            ...props
                        };
                    },
                }),
            ]);
        }, []);
        const { scrolledDirections, scrollContainerRef } = useDetectScroll(["left"]);
        const sortedColumns = useExternalStoreState(enginesTable.store, (state: any) => state.sortedColumns);
        return (
            <Component
                {...props}
                enginesTable={enginesTable}
                sortedColumns={sortedColumns}
                scrolledDirections={scrolledDirections}
                scrollContainerRef={scrollContainerRef}
            />
        );
    }
}
