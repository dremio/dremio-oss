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

import { queryOptions, useSuspenseQuery } from "@tanstack/react-query";
import {
  Input,
  ModalContainer,
  Skeleton,
  useModalContainer,
} from "dremio-ui-lib/components";
import { useQuery, keepPreviousData } from "@tanstack/react-query";
import {
  FC,
  PropsWithChildren,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { debounce } from "lodash";
import { datasetQuery } from "@inject/queries/datasets";
import { ErrorBoundary } from "react-error-boundary";
import { DremioUser } from "dremio-ui-common/components/DremioUser.js";
import { userById } from "@inject/queries/users";
import dayjs from "dayjs";
import { CatalogReferenceIcon } from "dremio-ui-common/catalog/CatalogReferenceIcon.js";
import { CatalogReference } from "@dremio/dremio-js/interfaces";
import {
  catalogSearchModalContext,
  useSearchModal,
} from "dremio-ui-common/sonar/CatalogSearch/CatalogSearchModalContext.js";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { Link } from "react-router";
import { newQuery } from "dremio-ui-common/paths/sqlEditor.js";
import { RecentSearches } from "dremio-ui-common/sonar/CatalogSearch/RecentSearches.js";
import { addRecentSearch, recentSearches } from "#oss/queries/recentSearches";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { queryClient } from "#oss/queryClient";
import {
  CatalogTypeFilters,
  CatalogFilterType,
  useCatalogTypeFilterState,
  type CatalogFilterTypeCounts,
} from "dremio-ui-common/sonar/CatalogSearch/CatalogTypeFilters.js";
import { getModifierKeyState } from "dremio-ui-common/utilities/getModifierKey.js";

const searchQuery = (pid?: string, obj: Record<string, any>) => {
  const searchText = obj["and"]["text"];
  return queryOptions({
    queryKey: ["search", JSON.stringify(obj)],
    queryFn: async () => {
      const results = await getApiContext()
        .fetch(
          getApiContext().createApiV2Url()(
            `datasets/search?filter=${searchText}`,
          ),
        )
        .then((res) => res.json());

      if (results.length > 0) {
        queryClient.setQueryData(
          [pid, "recent-searches"],
          addRecentSearch(pid, searchText),
        );
      }

      return results.map((result: any) => {
        return {
          type: result.datasetType,
          path: result.fullPath,
          id: result.id,
        };
      }) as CatalogReference[];
    },
    staleTime: 30 * 1000,
    placeholderData: keepPreviousData,
  });
};

const SearchResult: FC<{ result: CatalogReference }> = (props) => {
  const query = useSuspenseQuery({
    ...datasetQuery(props.result.path),
    retry: false,
    staleTime: 60 * 1000,
  });
  const userQuery = useSuspenseQuery(userById(query.data.owner.id));
  return (
    <div className="text-sm flex flex-row gap-1 dremio-typography-less-important items-center mt-2">
      <div>
        <DremioUser user={userQuery.data} />
      </div>
      &bull;
      <div>Last updated {dayjs(query.data.modifiedAt).fromNow()}</div>
    </div>
  );
};

const SearchModalContents: FC = () => {
  const { t } = getIntlContext();
  const elRef = useRef<HTMLDivElement>(null);
  const [currentInput, setCurrentInput] = useState("");
  const [debouncedSearchText, setDebouncedSearchText] = useState("");
  const handleSearchTextInput = useCallback(
    debounce((text: string) => setDebouncedSearchText(text), 500),
    [],
  );

  const catalogTypeFilters = useCatalogTypeFilterState();

  const queryEnabled = debouncedSearchText.length > 2;

  const query = useQuery({
    ...searchQuery(getSonarContext().getSelectedProjectId?.() || "DEFAULT", {
      and: {
        text: debouncedSearchText,
      },
    }),
    enabled: queryEnabled,
  });

  const modalContext = useSearchModal();

  const recentSearchesQuery = useSuspenseQuery(
    recentSearches(getSonarContext().getSelectedProjectId?.()),
  );

  const typeCounts = useMemo(() => {
    if (!query.data) {
      return null;
    }
    const counts: CatalogFilterTypeCounts = new Map();
    for (const object of query.data) {
      switch (object.type) {
        case "PHYSICAL_DATASET":
        case "PHYSICAL_DATASET_SOURCE_FILE":
        case "PHYSICAL_DATASET_SOURCE_FOLDER":
          counts.set(
            CatalogFilterType.TABLE,
            (counts.get(CatalogFilterType.TABLE) || 0) + 1,
          );
          break;
        case "VIRTUAL_DATASET":
          counts.set(
            CatalogFilterType.VIEW,
            (counts.get(CatalogFilterType.VIEW) || 0) + 1,
          );
          break;
      }
    }
    return counts;
  }, [query.data]);

  return (
    <div className="flex flex-col p-2 w-full" ref={elRef}>
      <div className="shrink-0">
        <Input
          prefix={
            <dremio-icon
              name="interface/search"
              alt=""
              class="mr-05 icon-primary"
            ></dremio-icon>
          }
          clearable
          type="text"
          placeholder={t("Catalog.Search.Modal.SearchTextInput.Placeholder")}
          onInput={(e) => {
            setCurrentInput(e.currentTarget.value);
            handleSearchTextInput(e.currentTarget.value);
          }}
          value={currentInput}
        />
      </div>

      <div className="mt-2 shrink-0">
        <CatalogTypeFilters {...catalogTypeFilters} typeCounts={typeCounts} />
      </div>

      <Suspense
        fallback={
          <div className="flex flex-col gap-1 mt-2 pr-1 overflow-y-auto">
            {query.data?.map((_unused, i) => (
              <div
                key={i}
                className="bg-primary border-thin p-2"
                style={{ borderRadius: "4px" }}
              >
                <div className="flex flex-col gap-05">
                  <div>
                    <Skeleton width="10ch" />
                  </div>
                  <div className="text-sm dremio-typography-less-important">
                    <Skeleton width="15ch" />
                  </div>
                </div>
                <div className="text-sm flex flex-row gap-1 dremio-typography-less-important items-center mt-2">
                  <div className="flex flex-row gap-1 items-center">
                    <Skeleton
                      type="custom"
                      width="24px"
                      height="24px"
                      style={{ borderRadius: "9999px" }}
                    />
                    <Skeleton width="10ch" />
                  </div>
                  <div>
                    <Skeleton width="10ch" />
                  </div>
                </div>
              </div>
            ))}
          </div>
        }
      >
        {!queryEnabled && (
          <RecentSearches
            searches={recentSearchesQuery.data}
            onSearchClicked={(search: string) => {
              setCurrentInput(search);
              setDebouncedSearchText(search);
            }}
          />
        )}
        {query.isError && (
          <div className="flex flex-col gap-1 items-center p-2">
            <p className="text-semibold text-lg">
              No results found for “{debouncedSearchText}”
            </p>
            <p className="dremio-typography-less-important">
              An unexpected error occurred when we tried to search the catalog.
            </p>
          </div>
        )}
        {queryEnabled &&
          query.data &&
          query.data.length === 0 &&
          !query.isFetching && (
            <div className="flex flex-col gap-1 items-center p-2">
              <p className="text-semibold text-lg">
                No results found for “{debouncedSearchText}”
              </p>
              <p className="dremio-typography-less-important">
                We couldn’t find any results based on your filter criteria.
              </p>
            </div>
          )}
        {query.data && query.data.length > 0 && queryEnabled && (
          <div className="flex flex-col gap-1 mt-2 pr-1 overflow-y-auto">
            {query.data.map((result) => {
              return (
                <Link
                  className="no-underline"
                  key={result.id}
                  to={newQuery.link({
                    // context: result.path.at(0),
                    queryPath: JSON.stringify(result.path),
                  })}
                  onClick={(e) => {
                    modalContext.close();
                  }}
                >
                  <div
                    className="bg-primary border-thin p-2 hover"
                    style={{ borderRadius: "4px" }}
                  >
                    <div className="flex flex-row items-center gap-1">
                      <CatalogReferenceIcon catalogReference={result} />
                      <div className="flex flex-col gap-05">
                        <div className="text-semibold">
                          {result.path.at(-1)}
                        </div>
                        <div className="text-sm dremio-typography-less-important">
                          {result.path.join(".")}
                        </div>
                      </div>
                    </div>
                    <ErrorBoundary
                      fallback={
                        <>
                          <div className="text-sm flex flex-row gap-1 dremio-typography-less-important items-center mt-2">
                            Unable to load additional details
                          </div>
                        </>
                      }
                    >
                      <Suspense
                        fallback={
                          <div className="text-sm flex flex-row gap-1 dremio-typography-less-important items-center mt-2">
                            <div className="flex flex-row gap-1 items-center">
                              <Skeleton
                                type="custom"
                                width="24px"
                                height="24px"
                                style={{ borderRadius: "9999px" }}
                              />
                              <Skeleton width="10ch" />
                            </div>
                            <div>
                              <Skeleton width="10ch" />
                            </div>
                          </div>
                        }
                      >
                        <SearchResult result={result} />
                      </Suspense>
                    </ErrorBoundary>
                  </div>
                </Link>
              );
            })}
          </div>
        )}
      </Suspense>
    </div>
  );
};

export const SearchModal: FC = () => {
  const ctx = useSearchModal();
  return (
    <ModalContainer
      {...ctx}
      style={{ maxInlineSize: "800px" }}
      closeOnOutsideClick
    >
      <SearchModalContents />
    </ModalContainer>
  );
};

const useSearchOpenShortcut = (open: () => void) => {
  useEffect(() => {
    const handleKeydown: (this: Document, ev: KeyboardEvent) => void = (e) => {
      if (e.key === "k" && e.getModifierState(getModifierKeyState())) {
        e.preventDefault();
        e.stopPropagation();
        open();
      }
    };
    document.addEventListener("keydown", handleKeydown);

    return () => {
      document.removeEventListener("keydown", handleKeydown);
    };
  }, []);
};

export const SearchModalProvider = (props: PropsWithChildren) => {
  const ctx = useModalContainer();
  useSearchOpenShortcut(ctx.open);
  return (
    <catalogSearchModalContext.Provider value={ctx}>
      {props.children}
    </catalogSearchModalContext.Provider>
  );
};
