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
package com.dremio.plugins.icebergcatalog.store;

import static com.dremio.exec.catalog.CatalogOptions.RESTCATALOG_VIEWS_SUPPORTED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_ALLOWED_NS_SEPARATOR;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.options.OptionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@code allowedNamespaces} / {@code isRecursiveAllowedNamespaces} filter of {@link
 * AbstractRestCatalogAccessor}, against a mocked REST catalog with nested namespaces.
 *
 * <p>The filter only scopes what Dremio <em>discovers</em> (dataset listing for metadata refresh
 * and the folder listing). Direct lookups by path are not filtered; the tests at the end pin that
 * behavior so a change to it is deliberate.
 *
 * <pre>
 * sales                tables: s_top          views: v_sales
 *   sales.eu           tables: s_eu           views: v_eu
 *     sales.eu.de      tables: s_de
 *   sales.us           tables: s_us
 * hr                   tables: h_top
 *   hr.private         tables: h_priv         views: v_priv
 * Mixed_Case           tables: m_top
 *   Mixed_Case.Sub-Ns  tables: m_sub
 * "a.b"                tables: dot_t          (one level whose name contains a dot)
 * </pre>
 */
public class TestRestCatalogAllowedNamespaces {

  private static final String SOURCE = "src";
  private static final String DEFAULT_SEPARATOR = "\\.";

  private static final Namespace SALES = Namespace.of("sales");
  private static final Namespace SALES_EU = Namespace.of("sales", "eu");
  private static final Namespace SALES_EU_DE = Namespace.of("sales", "eu", "de");
  private static final Namespace SALES_US = Namespace.of("sales", "us");
  private static final Namespace HR = Namespace.of("hr");
  private static final Namespace HR_PRIVATE = Namespace.of("hr", "private");
  private static final Namespace MIXED = Namespace.of("Mixed_Case");
  private static final Namespace MIXED_SUB = Namespace.of("Mixed_Case", "Sub-Ns");
  private static final Namespace DOTTED = Namespace.of("a.b");

  private static final List<String> ALL_TABLES =
      ImmutableList.of(
          "sales.s_top",
          "sales.eu.s_eu",
          "sales.eu.de.s_de",
          "sales.us.s_us",
          "hr.h_top",
          "hr.private.h_priv",
          "Mixed_Case.m_top",
          "Mixed_Case.Sub-Ns.m_sub",
          "a.b.dot_t");
  private static final List<String> ALL_VIEWS =
      ImmutableList.of("sales.v_sales", "sales.eu.v_eu", "hr.private.v_priv");

  private final Map<Namespace, List<Namespace>> children = new LinkedHashMap<>();
  private final Map<Namespace, List<String>> tables = new LinkedHashMap<>();
  private final Map<Namespace, List<String>> views = new LinkedHashMap<>();
  private final Set<Namespace> forbidden = new HashSet<>();

  private RESTCatalog catalog;
  private OptionManager optionManager;
  private SupportsIcebergRootPointer plugin;

  @Before
  public void setUp() {
    addNamespace(SALES, ImmutableList.of("s_top"), ImmutableList.of("v_sales"));
    addNamespace(SALES_EU, ImmutableList.of("s_eu"), ImmutableList.of("v_eu"));
    addNamespace(SALES_EU_DE, ImmutableList.of("s_de"), ImmutableList.of());
    addNamespace(SALES_US, ImmutableList.of("s_us"), ImmutableList.of());
    addNamespace(HR, ImmutableList.of("h_top"), ImmutableList.of());
    addNamespace(HR_PRIVATE, ImmutableList.of("h_priv"), ImmutableList.of("v_priv"));
    addNamespace(MIXED, ImmutableList.of("m_top"), ImmutableList.of());
    addNamespace(MIXED_SUB, ImmutableList.of("m_sub"), ImmutableList.of());
    addNamespace(DOTTED, ImmutableList.of("dot_t"), ImmutableList.of());

    catalog = mock(RESTCatalog.class);
    when(catalog.listNamespaces(any(Namespace.class)))
        .then(inv -> new ArrayList<>(children.get(existing(inv.getArgument(0)))));
    when(catalog.listTables(any(Namespace.class)))
        .then(inv -> identifiers(inv.getArgument(0), tables));
    when(catalog.listViews(any(Namespace.class)))
        .then(inv -> identifiers(inv.getArgument(0), views));
    when(catalog.loadNamespaceMetadata(any(Namespace.class)))
        .then(
            inv -> {
              Namespace ns = existing(inv.getArgument(0));
              return ImmutableMap.of("location", "s3://bucket/" + String.join("/", ns.levels()));
            });
    when(catalog.namespaceExists(any(Namespace.class)))
        .then(inv -> children.containsKey(inv.<Namespace>getArgument(0)));
    when(catalog.tableExists(any(TableIdentifier.class)))
        .then(inv -> contains(tables, inv.getArgument(0)));
    when(catalog.viewExists(any(TableIdentifier.class)))
        .then(inv -> contains(views, inv.getArgument(0)));

    optionManager = mock(OptionManager.class);
    when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS)).thenReturn(10L);
    when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS))
        .thenReturn(10L);
    when(optionManager.getOption(RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS)).thenReturn(60L);
    when(optionManager.getOption(RESTCATALOG_ALLOWED_NS_SEPARATOR)).thenReturn(DEFAULT_SEPARATOR);
    when(optionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);

    plugin = mock(SupportsIcebergRootPointer.class);
  }

  // ---------------------------------------------------------------------------------------------
  // Unset / empty configuration
  // ---------------------------------------------------------------------------------------------

  @Test
  public void nullAllowedNamespacesExposesWholeCatalog() {
    AbstractRestCatalogAccessor accessor = accessor(null, true);

    assertThat(listedDatasets(accessor)).containsExactlyInAnyOrderElementsOf(allDatasets());
  }

  @Test
  public void nullAllowedNamespacesIgnoresNonRecursiveFlag() {
    // With no allow list the subtree flag is forced to true: a non-recursive "all" would only
    // list the (empty) root.
    AbstractRestCatalogAccessor accessor = accessor(null, false);

    assertThat(listedDatasets(accessor)).containsExactlyInAnyOrderElementsOf(allDatasets());
  }

  @Test
  public void emptyAllowedNamespacesListHidesEverything() {
    // At the accessor level an empty (non-null) list means "nothing is allowed". Over the REST API
    // a source saved with "allowedNamespaces": [] comes back with a null list (the proto round trip
    // drops empty repeated fields), so such a source shows the whole catalog.
    AbstractRestCatalogAccessor accessor = accessor(Collections.emptyList(), true);

    assertThat(listedDatasets(accessor)).isEmpty();
    assertThat(folders(accessor)).isEmpty();
  }

  @Test
  public void blankEntriesAreIgnored() {
    assertThat(listedDatasets(accessor(Arrays.asList("", "hr"), true)))
        .containsExactlyInAnyOrder("hr.h_top", "hr.private.h_priv", "hr.private.v_priv");
    assertThat(listedDatasets(accessor(Collections.singletonList(""), true))).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // Recursive vs. non-recursive
  // ---------------------------------------------------------------------------------------------

  @Test
  public void recursiveTopLevelNamespaceIncludesWholeSubtree() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales"), true);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder(
            "sales.s_top",
            "sales.v_sales",
            "sales.eu.s_eu",
            "sales.eu.v_eu",
            "sales.eu.de.s_de",
            "sales.us.s_us");
  }

  @Test
  public void nonRecursiveTopLevelNamespaceIncludesOnlyItsDirectDatasets() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales"), false);

    assertThat(listedDatasets(accessor)).containsExactlyInAnyOrder("sales.s_top", "sales.v_sales");
    // Nothing below "sales" is even listed.
    verify(catalog, never()).listTables(SALES_EU);
    verify(catalog, never()).listTables(SALES_US);
  }

  @Test
  public void recursiveNestedNamespaceExcludesParentAndSiblings() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales.eu"), true);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder("sales.eu.s_eu", "sales.eu.v_eu", "sales.eu.de.s_de");
    verify(catalog, never()).listTables(SALES);
    verify(catalog, never()).listTables(SALES_US);
  }

  @Test
  public void nonRecursiveNestedNamespace() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales.eu"), false);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder("sales.eu.s_eu", "sales.eu.v_eu");
  }

  @Test
  public void multipleEntriesRecursive() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales.eu.de", "hr"), true);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder(
            "sales.eu.de.s_de", "hr.h_top", "hr.private.h_priv", "hr.private.v_priv");
  }

  @Test
  public void multipleEntriesNonRecursive() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales.eu.de", "hr"), false);

    assertThat(listedDatasets(accessor)).containsExactlyInAnyOrder("sales.eu.de.s_de", "hr.h_top");
  }

  @Test
  public void nonRecursiveEntriesCanListParentAndChildExplicitly() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales", "sales.us"), false);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder("sales.s_top", "sales.v_sales", "sales.us.s_us");
  }

  @Test
  public void duplicateEntriesCollapse() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("hr", "hr", "hr."), true);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder("hr.h_top", "hr.private.h_priv", "hr.private.v_priv");
  }

  @Test
  public void overlappingRecursiveEntriesListSubtreeOnce() {
    // A recursive entry covers its descendants, so "sales.eu" is not walked a second time (that
    // would list, and load during metadata refresh, every dataset under it twice).
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales", "sales.eu"), true);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder(
            "sales.s_top",
            "sales.v_sales",
            "sales.eu.s_eu",
            "sales.eu.v_eu",
            "sales.eu.de.s_de",
            "sales.us.s_us");
    assertThat(folderNames(accessor))
        .containsExactlyInAnyOrder("sales", "sales.eu", "sales.eu.de", "sales.us");
  }

  @Test
  public void overlappingNonRecursiveEntriesAreAllListed() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales", "sales.eu"), false);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder(
            "sales.s_top", "sales.v_sales", "sales.eu.s_eu", "sales.eu.v_eu");
  }

  @Test
  public void coveredNamespacesAreDroppedOnlyBelowAnAncestor() {
    assertThat(
            AbstractRestCatalogAccessor.dropCoveredNamespaces(
                new HashSet<>(Arrays.asList(SALES, SALES_EU, SALES_EU_DE, HR_PRIVATE, MIXED))))
        .containsExactlyInAnyOrder(SALES, HR_PRIVATE, MIXED);
    assertThat(
            AbstractRestCatalogAccessor.dropCoveredNamespaces(
                new HashSet<>(Arrays.asList(Namespace.empty(), HR))))
        .containsExactly(Namespace.empty());
  }

  @Test
  public void viewsAreNotListedWhenViewsAreDisabled() {
    when(optionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(false);
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales"), true);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder(
            "sales.s_top", "sales.eu.s_eu", "sales.eu.de.s_de", "sales.us.s_us");
    verify(catalog, never()).listViews(any(Namespace.class));
  }

  // ---------------------------------------------------------------------------------------------
  // Names: missing, case, whitespace, separators
  // ---------------------------------------------------------------------------------------------

  @Test
  public void nonexistentNamespaceYieldsNothingAndDoesNotFail() {
    assertThat(listedDatasets(accessor(ImmutableList.of("nonexistent"), true))).isEmpty();
    assertThat(listedDatasets(accessor(ImmutableList.of("nonexistent"), false))).isEmpty();
    assertThat(folders(accessor(ImmutableList.of("nonexistent"), true))).isEmpty();
  }

  @Test
  public void nonexistentEntryDoesNotHideOtherEntries() {
    AbstractRestCatalogAccessor accessor =
        accessor(ImmutableList.of("nonexistent", "sales.nope", "hr.private"), true);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder("hr.private.h_priv", "hr.private.v_priv");
  }

  @Test
  public void matchingIsCaseSensitive() {
    assertThat(listedDatasets(accessor(ImmutableList.of("SALES"), true))).isEmpty();
    assertThat(listedDatasets(accessor(ImmutableList.of("mixed_case"), true))).isEmpty();
    assertThat(listedDatasets(accessor(ImmutableList.of("Mixed_Case"), true)))
        .containsExactlyInAnyOrder("Mixed_Case.m_top", "Mixed_Case.Sub-Ns.m_sub");
  }

  @Test
  public void specialCharactersOtherThanTheSeparatorAreLiteral() {
    assertThat(listedDatasets(accessor(ImmutableList.of("Mixed_Case.Sub-Ns"), false)))
        .containsExactly("Mixed_Case.Sub-Ns.m_sub");
  }

  @Test
  public void entriesAreNotTrimmed() {
    assertThat(listedDatasets(accessor(ImmutableList.of(" sales"), true))).isEmpty();
    assertThat(listedDatasets(accessor(ImmutableList.of("sales "), true))).isEmpty();
    assertThat(listedDatasets(accessor(ImmutableList.of("sales. eu"), true))).isEmpty();
  }

  @Test
  public void trailingSeparatorIsIgnored() {
    // String.split drops trailing empty strings, so "sales.eu." is the same as "sales.eu".
    assertThat(listedDatasets(accessor(ImmutableList.of("sales.eu."), false)))
        .containsExactlyInAnyOrder("sales.eu.s_eu", "sales.eu.v_eu");
  }

  @Test
  public void leadingSeparatorAddsAnEmptyLevel() {
    // ".sales" becomes Namespace("", "sales"), which does not exist.
    assertThat(listedDatasets(accessor(ImmutableList.of(".sales"), true))).isEmpty();
  }

  @Test
  public void separatorOnlyEntryMeansWholeCatalog() {
    // "." splits into no levels at all, i.e. Namespace.empty(), the same as an unset allow list.
    assertThat(listedDatasets(accessor(ImmutableList.of("."), true)))
        .containsExactlyInAnyOrderElementsOf(allDatasets());
  }

  @Test
  public void defaultSeparatorCannotAddressADottedNamespaceName() {
    // "a.b" is split into Namespace("a", "b"); the single-level namespace "a.b" is unreachable.
    assertThat(listedDatasets(accessor(ImmutableList.of("a.b"), true))).isEmpty();
  }

  @Test
  public void customSeparatorAllowsDotsInNamespaceNames() {
    when(optionManager.getOption(RESTCATALOG_ALLOWED_NS_SEPARATOR)).thenReturn("/");

    assertThat(listedDatasets(accessor(ImmutableList.of("a.b", "sales/eu/de"), true)))
        .containsExactlyInAnyOrder("a.b.dot_t", "sales.eu.de.s_de");
  }

  @Test
  public void separatorIsARegularExpression() {
    // An escaped pipe works as a literal separator...
    when(optionManager.getOption(RESTCATALOG_ALLOWED_NS_SEPARATOR)).thenReturn("\\|");
    assertThat(listedDatasets(accessor(ImmutableList.of("sales|eu|de"), true)))
        .containsExactly("sales.eu.de.s_de");

    // ...but an unescaped one matches the empty string and splits every character apart.
    when(optionManager.getOption(RESTCATALOG_ALLOWED_NS_SEPARATOR)).thenReturn("|");
    assertThat(listedDatasets(accessor(ImmutableList.of("hr"), true))).isEmpty();
    verify(catalog, atLeastOnce()).listNamespaces(Namespace.of("h", "r"));
  }

  @Test
  public void separatorIsReadWhenTheAccessorIsCreated() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales/eu/de"), true);
    // Changing the option afterwards does not affect an existing accessor (i.e. the source must
    // be re-saved or restarted to pick up a new separator).
    when(optionManager.getOption(RESTCATALOG_ALLOWED_NS_SEPARATOR)).thenReturn("/");

    assertThat(listedDatasets(accessor)).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // Failures while walking the catalog
  // ---------------------------------------------------------------------------------------------

  @Test
  public void forbiddenNamespaceIsSkippedDuringRecursiveListing() {
    forbidden.add(HR_PRIVATE);
    AbstractRestCatalogAccessor accessor = accessor(null, true);

    assertThat(listedDatasets(accessor))
        .doesNotContain("hr.private.h_priv", "hr.private.v_priv")
        .contains("hr.h_top", "sales.eu.de.s_de");
    assertThat(folderNames(accessor)).doesNotContain("hr.private").contains("hr", "sales.eu.de");
  }

  @Test
  public void forbiddenAllowedNamespaceYieldsNothing() {
    forbidden.add(SALES);
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales", "hr"), true);

    assertThat(listedDatasets(accessor))
        .containsExactlyInAnyOrder("hr.h_top", "hr.private.h_priv", "hr.private.v_priv");
  }

  // ---------------------------------------------------------------------------------------------
  // Folder listing (getFolderStream)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void foldersForWholeCatalog() {
    assertThat(folderNames(accessor(null, true)))
        .containsExactlyInAnyOrder(
            "sales",
            "sales.eu",
            "sales.eu.de",
            "sales.us",
            "hr",
            "hr.private",
            "Mixed_Case",
            "Mixed_Case.Sub-Ns",
            "a.b");
  }

  @Test
  public void foldersForRecursiveEntry() {
    assertThat(folderNames(accessor(ImmutableList.of("sales"), true)))
        .containsExactlyInAnyOrder("sales", "sales.eu", "sales.eu.de", "sales.us");
  }

  @Test
  public void foldersForNonRecursiveEntryIncludeDirectChildren() {
    // Direct child namespaces are returned as (empty) folders even though their datasets are not
    // listed. Verified live: the Dremio tree shows sales/eu and sales/us without tables.
    assertThat(folderNames(accessor(ImmutableList.of("sales"), false)))
        .containsExactlyInAnyOrder("sales", "sales.eu", "sales.us");
  }

  @Test
  public void foldersForNestedEntryIncludeTheParent() {
    // Dremio's names refresh deletes every folder the source does not list, with its content, so
    // the parent "sales" must be listed (without properties: no request on it is needed).
    List<IcebergNamespaceWithProperties> folders =
        folders(accessor(ImmutableList.of("sales.eu"), true));

    assertThat(folders)
        .extracting(f -> String.join(".", f.getNamespace().levels()))
        .containsExactlyInAnyOrder("sales", "sales.eu", "sales.eu.de");
    assertThat(folders)
        .filteredOn(f -> f.getNamespace().equals(SALES))
        .singleElement()
        .satisfies(f -> assertThat(f.getProperties()).isEmpty());
    verify(catalog, never()).loadNamespaceMetadata(SALES);
    verify(catalog, never()).listNamespaces(SALES);
  }

  @Test
  public void foldersForDeeplyNestedEntriesIncludeEveryAncestorOnce() {
    assertThat(folderNames(accessor(ImmutableList.of("sales.eu.de", "sales.us", "hr"), false)))
        .containsExactlyInAnyOrder(
            "sales", "sales.eu", "sales.eu.de", "sales.us", "hr", "hr.private");
  }

  @Test
  public void ancestorDiscoveredBelowAnotherEntryKeepsItsProperties() {
    // "sales.eu" is both a direct child of the non-recursive entry "sales" (listed with its
    // location) and the parent of "sales.eu.de": it is listed once, with its properties.
    List<IcebergNamespaceWithProperties> folders =
        folders(accessor(ImmutableList.of("sales", "sales.eu.de"), false));

    assertThat(folders)
        .extracting(f -> String.join(".", f.getNamespace().levels()))
        .containsExactlyInAnyOrder("sales", "sales.eu", "sales.us", "sales.eu.de");
    assertThat(folders)
        .filteredOn(f -> f.getNamespace().equals(SALES_EU))
        .singleElement()
        .satisfies(f -> assertThat(f.getProperties()).containsKey("location"));
  }

  @Test
  public void folderListingListsEveryParentBeforeItsChildren() {
    // Dremio's names refresh only recognizes a listed folder once its parent was listed; a child
    // listed first is deleted as "no longer found" (seen live with "sales.eu" before "sales").
    List<List<String>> configs =
        ImmutableList.of(
            ImmutableList.of("sales.eu"),
            ImmutableList.of("sales.eu.de", "hr"),
            ImmutableList.of("sales.eu.de", "sales"),
            ImmutableList.of("hr.private", "sales.eu", "Mixed_Case.Sub-Ns"));
    for (List<String> allowed : configs) {
      for (boolean recursive : new boolean[] {true, false}) {
        List<Namespace> listed =
            folders(accessor(allowed, recursive)).stream()
                .map(IcebergNamespaceWithProperties::getNamespace)
                .collect(Collectors.toList());
        assertThat(new HashSet<>(listed)).hasSameSizeAs(listed);
        for (int i = 0; i < listed.size(); i++) {
          String[] levels = listed.get(i).levels();
          for (int depth = 1; depth < levels.length; depth++) {
            Namespace parent = Namespace.of(Arrays.copyOf(levels, depth));
            assertThat(listed.indexOf(parent))
                .as(
                    "%s (recursive=%s): %s listed before its parent %s",
                    allowed, recursive, listed.get(i), parent)
                .isBetween(0, i - 1);
          }
        }
      }
    }
  }

  @Test
  public void allowedParentWhosePropertiesAreDeniedIsStillListedBeforeItsChildren() {
    // The principal may read the properties of "sales.eu" but not of "sales": "sales" is listed
    // without properties, otherwise the names refresh would delete the "sales.eu" subtree.
    when(catalog.loadNamespaceMetadata(SALES))
        .thenThrow(new ForbiddenException("Forbidden: not authorized for op LOAD_NAMESPACE"));
    for (boolean recursive : new boolean[] {true, false}) {
      List<IcebergNamespaceWithProperties> folders =
          folders(accessor(ImmutableList.of("sales", "sales.eu"), recursive));

      assertThat(folders)
          .extracting(f -> String.join(".", f.getNamespace().levels()))
          .as("recursive=%s", recursive)
          .startsWith("sales")
          .contains("sales.eu", "sales.eu.de", "sales.us")
          .doesNotHaveDuplicates();
      assertThat(folders.get(0).getProperties()).isEmpty();
      assertThat(folders)
          .filteredOn(f -> f.getNamespace().equals(SALES_EU))
          .singleElement()
          .satisfies(f -> assertThat(f.getProperties()).containsKey("location"));
    }
  }

  @Test
  public void missingAllowedNamespaceListsNoAncestors() {
    assertThat(folderNames(accessor(ImmutableList.of("nope.child", "hr.private"), true)))
        .containsExactlyInAnyOrder("hr", "hr.private");
  }

  @Test
  public void foldersCarryTheNamespaceLocation() {
    List<IcebergNamespaceWithProperties> folders = folders(accessor(ImmutableList.of("hr"), false));

    assertThat(folders)
        .extracting(f -> f.getProperties().get("location"))
        .containsExactlyInAnyOrder("s3://bucket/hr", "s3://bucket/hr/private");
  }

  // ---------------------------------------------------------------------------------------------
  // listDatasetIdentifiers (path based; no production caller at the moment)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void listDatasetIdentifiersWithoutAllowList() {
    AbstractRestCatalogAccessor accessor = accessor(null, true);

    assertThat(names(accessor.listDatasetIdentifiers(ImmutableList.of(SOURCE, "sales", "eu"))))
        .containsExactlyInAnyOrder("sales.eu.s_eu", "sales.eu.v_eu", "sales.eu.de.s_de");
  }

  @Test
  public void listDatasetIdentifiersRequiresAnExactAllowListEntry() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales"), true);

    assertThat(names(accessor.listDatasetIdentifiers(ImmutableList.of(SOURCE, "sales"))))
        .contains("sales.s_top", "sales.eu.de.s_de");
    assertThat(accessor.listDatasetIdentifiers(ImmutableList.of(SOURCE, "hr"))).isEmpty();
    // Current behavior: a descendant of a recursive entry is not accepted as a starting point.
    assertThat(accessor.listDatasetIdentifiers(ImmutableList.of(SOURCE, "sales", "eu"))).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // Direct lookups are not filtered (current behavior)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void directLookupOfHiddenTableIsNotFiltered() {
    // allowedNamespaces scopes discovery, it is not an access control: a query that names a
    // table outside the allow list still resolves it (verified live: SELECT on a hidden table
    // completes and the table then shows up in the catalog tree).
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales"), false);

    assertThat(accessor.getDatasetHandle(ImmutableList.of(SOURCE, "hr", "h_top"), plugin))
        .isPresent();
    assertThat(accessor.getDatasetHandle(ImmutableList.of(SOURCE, "sales", "eu", "s_eu"), plugin))
        .isPresent();
    assertThat(
            accessor.getDatasetHandle(ImmutableList.of(SOURCE, "hr", "private", "v_priv"), plugin))
        .isPresent();
    assertThat(accessor.datasetExists(ImmutableList.of(SOURCE, "hr", "private", "h_priv")))
        .isTrue();
    assertThat(accessor.namespaceExists(ImmutableList.of(SOURCE, "hr", "private"))).isTrue();
  }

  @Test
  public void directLookupOfMissingTableIsEmpty() {
    AbstractRestCatalogAccessor accessor = accessor(ImmutableList.of("sales"), true);

    assertThat(accessor.getDatasetHandle(ImmutableList.of(SOURCE, "sales", "nope"), plugin))
        .isEmpty();
    assertThat(accessor.datasetExists(ImmutableList.of(SOURCE, "nope", "t"))).isFalse();
    assertThat(accessor.namespaceExists(ImmutableList.of(SOURCE, "nope"))).isFalse();
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private AbstractRestCatalogAccessor accessor(List<String> allowed, boolean recursive) {
    return new IcebergRestCatalogAccessor(() -> catalog, optionManager, allowed, recursive);
  }

  private void addNamespace(Namespace ns, List<String> tableNames, List<String> viewNames) {
    children.putIfAbsent(ns, new ArrayList<>());
    Namespace parent =
        ns.length() == 1
            ? Namespace.empty()
            : Namespace.of(Arrays.copyOf(ns.levels(), ns.length() - 1));
    children.computeIfAbsent(parent, p -> new ArrayList<>()).add(ns);
    tables.put(ns, tableNames);
    views.put(ns, viewNames);
  }

  private Namespace existing(Namespace ns) {
    if (forbidden.contains(ns)) {
      throw new ForbiddenException("Forbidden: not authorized on %s", ns);
    }
    if (!children.containsKey(ns)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", ns);
    }
    return ns;
  }

  private List<TableIdentifier> identifiers(Namespace ns, Map<Namespace, List<String>> source) {
    existing(ns);
    return source.getOrDefault(ns, Collections.emptyList()).stream()
        .map(name -> TableIdentifier.of(ns, name))
        .collect(Collectors.toList());
  }

  private static boolean contains(Map<Namespace, List<String>> source, TableIdentifier id) {
    return source.getOrDefault(id.namespace(), Collections.emptyList()).contains(id.name());
  }

  private static List<String> allDatasets() {
    List<String> all = new ArrayList<>(ALL_TABLES);
    all.addAll(ALL_VIEWS);
    return all;
  }

  /** Dataset names (without the source) returned by the metadata-refresh listing. */
  private List<String> listedDatasets(AbstractRestCatalogAccessor accessor) {
    List<String> names = new ArrayList<>();
    Iterator<? extends DatasetHandle> handles;
    try {
      handles = accessor.listDatasetHandles(SOURCE, plugin).iterator();
    } catch (ConnectorException e) {
      throw new IllegalStateException(e);
    }
    while (handles.hasNext()) {
      List<String> path = handles.next().getDatasetPath().getComponents();
      assertThat(path.get(0)).isEqualTo(SOURCE);
      names.add(String.join(".", path.subList(1, path.size())));
    }
    return names;
  }

  private static List<IcebergNamespaceWithProperties> folders(
      AbstractRestCatalogAccessor accessor) {
    return accessor.getFolderStream().collect(Collectors.toList());
  }

  private static List<String> folderNames(AbstractRestCatalogAccessor accessor) {
    return folders(accessor).stream()
        .map(f -> String.join(".", f.getNamespace().levels()))
        .collect(Collectors.toList());
  }

  private static Set<String> names(Set<TableIdentifier> identifiers) {
    return identifiers.stream().map(TableIdentifier::toString).collect(Collectors.toSet());
  }
}
