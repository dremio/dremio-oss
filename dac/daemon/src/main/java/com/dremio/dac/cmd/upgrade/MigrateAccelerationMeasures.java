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
package com.dremio.dac.cmd.upgrade;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeFamily;

import com.dremio.dac.util.DatasetsUtil;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.serialization.JacksonSerializer;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.sys.PersistentStore;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.options.OptionValue;
import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.ReflectionValidator;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MeasureType;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;

/**
 * Upgrade task to migrate old reflection measures to the new ones
 */
public class MigrateAccelerationMeasures extends UpgradeTask {
  public MigrateAccelerationMeasures() {
    super("Migrate acceleration measures types", VERSION_150, VERSION_210, NORMAL_ORDER + 1);
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    System.out.println("  Checking if min/max measures were enabled...");
    try (final KVPersistentStoreProvider kvPersistentStoreProvider = new KVPersistentStoreProvider(
        DirectProvider.wrap(context.getKVStoreProvider()))) {
      final PersistentStore<OptionValue> options = kvPersistentStoreProvider.getOrCreateStore(
          SystemOptionManager.STORE_NAME, SystemOptionManager.OptionStoreCreator.class,
          new JacksonSerializer<>(context.getLpPersistence().getMapper(), OptionValue.class));

      // check if the old min/max option was set - can be null if the option was not changed from the default (true)
      final OptionValue optionValue = options.get("accelerator.enable_min_max");

      if (optionValue == null || optionValue.getBoolVal()) {
        System.out.println("  Min/max measures were enabled, migrating aggregate reflections...");
        migrateGoals(context);
      } else {
        System.out.println("  Min/max measures were disabled, skipping");
      }
    }
  }

  private void migrateGoals(UpgradeContext context) {
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider());
    final Provider<KVStoreProvider> provider = DirectProvider.wrap(context.getKVStoreProvider());
    final ReflectionGoalsStore reflectionGoalsStore = new ReflectionGoalsStore(provider);
    final ReflectionEntriesStore reflectionEntriesStore = new ReflectionEntriesStore(provider);
    final MaterializationStore materializationStore = new MaterializationStore(provider);

    for (ReflectionGoal reflectionGoal : reflectionGoalsStore.getAll()) {
      try {
        migrateGoal(namespaceService, reflectionGoalsStore, reflectionEntriesStore, materializationStore, reflectionGoal);
      } catch (Exception e) {
        System.out.println(String.format("    failed migrating reflection [%s] for dataset [%s]: %s", reflectionGoal.getId(), reflectionGoal.getDatasetId(), e));
      }
    }
  }

  private void migrateGoal(NamespaceService namespaceService, ReflectionGoalsStore reflectionGoalsStore, ReflectionEntriesStore reflectionEntriesStore, MaterializationStore materializationStore, ReflectionGoal reflectionGoal) {
    // details will be modified and re-saved
    ReflectionDetails details = reflectionGoal.getDetails();
    final List<ReflectionMeasureField> measureFieldList = details.getMeasureFieldList();

    // only process aggregate reflections that have measures
    if (reflectionGoal.getType() != ReflectionType.AGGREGATION || measureFieldList == null || measureFieldList.size() == 0) {
      return;
    }

    DatasetConfig datasetConfig = namespaceService.findDatasetByUUID(reflectionGoal.getDatasetId());
    if (datasetConfig == null) {
      System.out.println(String.format("    skipping reflection [%s] for dataset [%s] because dataset could not be found", reflectionGoal.getName(), reflectionGoal.getDatasetId()));
      return;
    }

    final List<Field> datasetFields = DatasetsUtil.getArrowFieldsFromDatasetConfig(datasetConfig);
    final List<ReflectionMeasureField> newMeasureFieldList = new ArrayList<>();

    System.out.println(String.format("    migrating %s measures for reflection [%s] on dataset [%s]", measureFieldList.size(), reflectionGoal.getName(), reflectionGoal.getDatasetId()));

    // for each measure field we add the default measure type given the field type
    for (ReflectionMeasureField measureField : measureFieldList) {
      ReflectionMeasureField newMeasureField = new ReflectionMeasureField();
      newMeasureField.setName(measureField.getName());

      if (datasetFields == null) {
        // If we can't load the schema, add the default NUMERIC measures.  The system will handle for us invalid measure
        // types so this is safe.
        System.out.println(String.format("      no schema found for dataset [%s], adding default measure types to reflection [%s]", reflectionGoal.getDatasetId(), reflectionGoal.getName()));
        newMeasureField.setMeasureTypeList(getDefaultsForNumeric());
        newMeasureFieldList.add(newMeasureField);
      } else {
        final RelDataType relDataType = BatchSchema.fromDataset(datasetConfig).toCalciteRecordType(SqlTypeFactoryImpl.INSTANCE);

        final RelDataTypeField field = relDataType.getField(measureField.getName(), false, false);

        if (field == null) {
          System.out.println(String.format("      could not find field [%s] for reflection [%s] on dataset [%s], adding default measure types", measureField.getName(), reflectionGoal.getName(), reflectionGoal.getDatasetId()));
          newMeasureField.setMeasureTypeList(getDefaultsForNumeric());
          newMeasureFieldList.add(newMeasureField);
        }

        // get the defaults for the field type
        final List<MeasureType> defaultMeasures = ReflectionValidator.getDefaultMeasures(field.getType().getFamily());

        // add min/max
        defaultMeasures.add(MeasureType.MIN);
        defaultMeasures.add(MeasureType.MAX);

        newMeasureField.setMeasureTypeList(defaultMeasures);
        newMeasureFieldList.add(newMeasureField);
      }
    }

    // update the details with the new list
    details.setMeasureFieldList(newMeasureFieldList);

    reflectionGoal.setDetails(details);
    reflectionGoalsStore.save(reflectionGoal);

    // update the reflection entry and materializion goal versions
    ReflectionGoal updatedReflectionGoal = reflectionGoalsStore.get(reflectionGoal.getId());

    ReflectionEntry reflectionEntry = reflectionEntriesStore.get(reflectionGoal.getId());
    // protect against missing entries - the goal was created but the manager never woke up to create the entry
    if (reflectionEntry != null) {
      reflectionEntry.setGoalVersion(updatedReflectionGoal.getVersion());

      Iterable<Materialization> allDone = materializationStore.getAllDone(updatedReflectionGoal.getId());

      for (Materialization materialization : allDone) {
        materialization.setReflectionGoalVersion(updatedReflectionGoal.getVersion());
        materializationStore.save(materialization);
      }
    }
  }

  private List<MeasureType> getDefaultsForNumeric() {
    final List<MeasureType> defaultMeasures = ReflectionValidator.getDefaultMeasures(SqlTypeFamily.NUMERIC);
    defaultMeasures.add(MeasureType.MIN);
    defaultMeasures.add(MeasureType.MAX);

    return defaultMeasures;
  }
}
