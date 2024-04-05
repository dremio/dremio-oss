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
package com.dremio.service.orphanage;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.orphanage.proto.OrphanEntry;
import java.util.ArrayList;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** test class for OrphanageImpl */
public class OrphanageImplTest {

  private KVStoreProvider kvStoreProvider;

  @Before
  public void setup() {
    kvStoreProvider = TempLocalKVStoreProviderCreator.create();
  }

  private OrphanEntry.Orphan prepareIcebergOrphanEntry(String uuid) {

    OrphanEntry.OrphanIcebergMetadata icebergOrphan =
        OrphanEntry.OrphanIcebergMetadata.newBuilder().setIcebergTableUuid(uuid).build();
    long currTime = System.currentTimeMillis();
    OrphanEntry.Orphan orphanEntry =
        OrphanEntry.Orphan.newBuilder()
            .setOrphanType(OrphanEntry.OrphanType.ICEBERG_METADATA)
            .setCreatedAt(currTime)
            .setScheduledAt(currTime)
            .setOrphanDetails(icebergOrphan.toByteString())
            .build();
    return orphanEntry;
  }

  private OrphanEntry.Orphan prepareIcebergOrphanEntry(String uuid, long scheduledAt) {

    OrphanEntry.OrphanIcebergMetadata icebergOrphan =
        OrphanEntry.OrphanIcebergMetadata.newBuilder().setIcebergTableUuid(uuid).build();
    long currTime = System.currentTimeMillis();
    OrphanEntry.Orphan orphanEntry =
        OrphanEntry.Orphan.newBuilder()
            .setOrphanType(OrphanEntry.OrphanType.ICEBERG_METADATA)
            .setCreatedAt(currTime)
            .setScheduledAt(scheduledAt)
            .setOrphanDetails(icebergOrphan.toByteString())
            .build();
    return orphanEntry;
  }

  @Test
  public void addOrphan() throws Exception {

    Orphanage orphanage = new OrphanageImpl(kvStoreProvider);
    OrphanEntry.Orphan orphanEntry = prepareIcebergOrphanEntry("icebergtable1", 123);
    orphanage.addOrphan(orphanEntry);
    Iterable<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> orphans =
        orphanage.getAllOrphans();
    Iterator<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> orphansIterator =
        orphans.iterator();

    if (orphansIterator.hasNext()) {
      Assert.assertEquals(orphanEntry, orphansIterator.next().getValue());
    } else {
      Assert.fail("No orphan got added");
    }
  }

  @Test
  public void addOrUpdateOrphan() throws InterruptedException {

    Orphanage orphanage = new OrphanageImpl(kvStoreProvider);

    OrphanEntry.OrphanId orphanId =
        OrphanEntry.OrphanId.newBuilder().setOrphanId("addupdateorphantest").build();
    OrphanEntry.Orphan orphanEntry = prepareIcebergOrphanEntry("icebergtable2");

    orphanage.addOrUpdateOrphan(orphanId, orphanEntry);
    Document<OrphanEntry.OrphanId, OrphanEntry.Orphan> orphanAdded = orphanage.getOrphan(orphanId);
    Assert.assertEquals(orphanAdded.getValue(), orphanEntry);

    OrphanEntry.Orphan updatedOrphanEntry = prepareIcebergOrphanEntry("icebergtable2", 28794004);
    orphanage.addOrUpdateOrphan(orphanId, updatedOrphanEntry);
    Assert.assertEquals(updatedOrphanEntry, orphanage.getOrphan(orphanId).getValue());
  }

  @Test
  public void deleteOrphan() throws InterruptedException {

    Orphanage orphanage = new OrphanageImpl(kvStoreProvider);
    orphanage.addOrphan(prepareIcebergOrphanEntry("icebergtable3"));
    Iterable<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> orphans =
        orphanage.getAllOrphans();
    Iterator<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> orphansIterator =
        orphans.iterator();

    if (orphansIterator.hasNext()) {
      OrphanEntry.OrphanId key = orphansIterator.next().getKey();
      orphanage.deleteOrphan(key);
      Document<OrphanEntry.OrphanId, OrphanEntry.Orphan> orphanDeleted = orphanage.getOrphan(key);
      Assert.assertEquals(null, orphanDeleted);

    } else {
      Assert.fail("No orphan got added to delete");
    }
  }

  @Test
  public void getOrphan() {

    Orphanage orphanage = new OrphanageImpl(kvStoreProvider);
    OrphanEntry.OrphanId orphanId =
        OrphanEntry.OrphanId.newBuilder().setOrphanId("getorphantestid").build();
    OrphanEntry.Orphan orphanEntry = prepareIcebergOrphanEntry("icebergtable4", 123456);
    orphanage.addOrUpdateOrphan(orphanId, orphanEntry);
    Assert.assertEquals(orphanage.getOrphan(orphanId).getValue(), orphanEntry);
  }

  @Test
  public void getAllOrphans() {

    Orphanage orphanage = new OrphanageImpl(kvStoreProvider);
    OrphanEntry.Orphan firstOrphan = prepareIcebergOrphanEntry("icebergtable5", 123);
    OrphanEntry.Orphan secondOrphan = prepareIcebergOrphanEntry("icebergtable6", 568);
    OrphanEntry.Orphan thirdOrphan = prepareIcebergOrphanEntry("icebergtable7", 198);

    orphanage.addOrphan(firstOrphan);
    orphanage.addOrphan(secondOrphan);
    orphanage.addOrphan(thirdOrphan);

    Iterable<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> orphans =
        orphanage.getAllOrphans();
    Iterator<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> orphansIterator =
        orphans.iterator();
    ArrayList<OrphanEntry.Orphan> orphansList = new ArrayList<>();
    while (orphansIterator.hasNext()) {
      orphansList.add(orphansIterator.next().getValue());
    }
    ArrayList<OrphanEntry.Orphan> orphansListSorted = new ArrayList<>();
    orphansListSorted.add(firstOrphan);
    orphansListSorted.add(thirdOrphan);
    orphansListSorted.add(secondOrphan);
    Assert.assertArrayEquals(orphansList.toArray(), orphansListSorted.toArray());
  }
}
