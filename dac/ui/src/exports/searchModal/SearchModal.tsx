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
  ModalContainer,
  Skeleton,
  useModalContainer,
} from "dremio-ui-lib/components";
import { useQuery, keepPreviousData } from "@tanstack/react-query";
import React, {
  FC,
  PropsWithChildren,
  Suspense,
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { debounce } from "lodash";
import { Spinner } from "dremio-ui-lib";
import { datasetQuery } from "@inject/queries/datasets";
import { ErrorBoundary } from "react-error-boundary";
import { DremioUser } from "dremio-ui-common/components/DremioUser.js";
import { userById } from "@inject/queries/users";
import dayjs from "dayjs";
import noResultsImg from "./no-results.svg";
import { useSupportFlag } from "../endpoints/SupportFlags/getSupportFlag";
import { CatalogReferenceIcon } from "dremio-ui-common/catalog/CatalogReferenceIcon.js";
import { CatalogReference } from "@dremio/dremio-js/interfaces";

const searchQuery = (obj: Record<string, any>) => {
  const searchText = obj["and"]["text"];
  return queryOptions({
    enabled: searchText?.length > 0,
    queryKey: ["search", JSON.stringify(obj)],
    queryFn: async () => {
      const results = await getApiContext()
        .fetch(
          getApiContext().createApiV2Url()(
            `datasets/search?filter=${searchText}`,
          ),
        )
        .then((res) => res.json());

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

// const TypeTag: FC<{ children: React.ReactNode }> = (props) => {
//   return (
//     <li
//       className="bg-primary border-thin inline-flex items-center flex-row py-05 px-1 gap-05"
//       style={{ borderRadius: "4px" }}
//     >
//       {props.children}
//     </li>
//   );
// };

const SearchModalContents: FC = () => {
  const elRef = useRef<HTMLDivElement>(null);
  const [debouncedSearchText, setDebouncedSearchText] = useState("");
  const handleSearchTextInput = useCallback(
    debounce((text: string) => setDebouncedSearchText(text), 500),
    [],
  );
  const query = useQuery(
    searchQuery({
      and: {
        text: debouncedSearchText,
      },
    }),
  );

  return (
    <div className="flex flex-col p-2 w-full" ref={elRef}>
      <div className="form-control shrink-0">
        <dremio-icon
          name="interface/search"
          alt=""
          class="mr-05 dremio-typography-less-important"
        ></dremio-icon>
        <input
          type="text"
          placeholder="Seach datasets, jobs..."
          onInput={(e) => handleSearchTextInput(e.currentTarget.value)}
        />
        {(query.isFetching || query.isRefetching) && <Spinner />}
      </div>

      {/* <div className="mt-2 shrink-0">
        <ul className="flex flex-row gap-1" aria-label="Object type filter">
          <TypeTag>All</TypeTag>
          <TypeTag>
            <dremio-icon name="entities/dataset-table" alt=""></dremio-icon>{" "}
            Table
          </TypeTag>
          <TypeTag>
            {" "}
            <dremio-icon name="entities/dataset-view" alt=""></dremio-icon> View
          </TypeTag>
          <TypeTag>
            <dremio-icon name="entities/blue-folder" alt=""></dremio-icon>{" "}
            Folder
          </TypeTag>
          <TypeTag>
            <dremio-icon name="sql-editor/function" alt=""></dremio-icon> UDF
          </TypeTag>
          <TypeTag>
            <dremio-icon name="interface/job-overview" alt=""></dremio-icon> Job
          </TypeTag>
          <TypeTag>
            <dremio-icon name="vcs/commit" alt=""></dremio-icon> Commit
          </TypeTag>
        </ul>
      </div> */}

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
        {query.data && query.data.length === 0 && !query.isFetching && (
          <div className="flex flex-col gap-1 items-center p-2">
            <img src={noResultsImg} alt="" style={{ width: "162px" }} />
            <p className="text-semibold text-lg">
              No results found for '{debouncedSearchText}'
            </p>
            <p className="dremio-typography-less-important">
              We couldn’t find any results based on your filter criteria.
            </p>
          </div>
        )}
        {query.data && query.data.length > 0 && (
          <div className="flex flex-col gap-1 mt-2 pr-1 overflow-y-auto">
            {query.data.map((result) => (
              <div
                key={result.id}
                className="bg-primary border-thin p-2 hover"
                style={{ borderRadius: "4px" }}
              >
                <div className="flex flex-row items-center gap-1">
                  <CatalogReferenceIcon catalogReference={result} />
                  <div className="flex flex-col gap-05">
                    <div className="text-semibold">{result.path.at(-1)}</div>
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
            ))}
          </div>
        )}
      </Suspense>
    </div>
  );
};

export const SearchModal: FC = () => {
  const ctx = useSearchModal();
  return (
    <ModalContainer {...ctx} style={{ maxInlineSize: "800px" }}>
      <SearchModalContents />
    </ModalContainer>
  );
};

const searchModalContext = createContext<ReturnType<
  typeof useModalContainer
> | null>(null);

const useSearchOpenShortcut = (open: () => void) => {
  useEffect(() => {
    const handleKeydown: (this: Document, ev: KeyboardEvent) => void = (e) => {
      if (e.key === "k" && e.getModifierState("Control")) {
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
    <searchModalContext.Provider value={ctx}>
      {props.children}
    </searchModalContext.Provider>
  );
};

export const useSearchModal = () => {
  const ctx = useContext(searchModalContext);

  if (!ctx) {
    throw new Error("searchModalContext provider is required");
  }

  return ctx;
};

export const SearchModalWrapper = () => {
  const [enabled, loading] = useSupportFlag("nextgen_search.ui.enable");

  if (enabled !== true) {
    return null;
  }

  return <SearchModal />;
};
