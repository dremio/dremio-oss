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
package com.dremio.datastore;

/**
 * Manages metadata about stores. The metadata is stored in the "default" store.
 */
interface StoreMetadataManager {

  /**
   * Allow updates to the metadata store.
   */
  void allowUpdates();

  /**
   * Get the latest transaction number.
   *
   * Note that the number returned is not per store; the number is global.
   *
   * @return latest transaction number
   */
  long getLatestTransactionNumber();

  /**
   * Sets the latest transaction number for the given store.
   *
   * @param storeName store name
   * @param transactionNumber latest transaction number
   */
  void setLatestTransactionNumber(String storeName, long transactionNumber);

  /**
   * No op implementation.
   */
  StoreMetadataManager NO_OP = new StoreMetadataManager() {
    @Override
    public void allowUpdates() {
    }

    @Override
    public long getLatestTransactionNumber() {
      return 0;
    }

    @Override
    public void setLatestTransactionNumber(String storeName, long transactionNumber) {
    }
  };
}
