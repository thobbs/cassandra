package org.apache.cassandra.index;

import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.restrictions.IndexRestrictions;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;

import static org.apache.cassandra.Util.throwAssert;
import static org.apache.cassandra.cql3.statements.IndexTarget.CUSTOM_INDEX_OPTION_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CustomIndexTest extends CQLTester
{
    @Test
    public void testInsertsOnCfsBackedIndex() throws Throwable
    {
        // test to ensure that we don't deadlock when flushing CFS backed custom indexers
        // see CASSANDRA-10181
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex("CREATE CUSTOM INDEX myindex ON %s(c) USING 'org.apache.cassandra.index.internal.CustomCassandraIndex'");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 2, 0, 0);
    }

    @Test
    public void indexControlsIfIncludedInBuildOnNewSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        String toInclude = "include";
        String toExclude = "exclude";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(b) USING '%s'",
                                  toInclude, IndexIncludedInBuild.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(b) USING '%s'",
                                  toExclude, IndexExcludedFromBuild.class.getName()));

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, 0);
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, 2);
        flush();

        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
        IndexIncludedInBuild included = (IndexIncludedInBuild)indexManager.getIndexByName(toInclude);
        included.reset();
        assertTrue(included.rowsInserted.isEmpty());

        IndexExcludedFromBuild excluded = (IndexExcludedFromBuild)indexManager.getIndexByName(toExclude);
        excluded.reset();
        assertTrue(excluded.rowsInserted.isEmpty());

        indexManager.buildAllIndexesBlocking(getCurrentColumnFamilyStore().getLiveSSTables());

        assertEquals(3, included.rowsInserted.size());
        assertTrue(excluded.rowsInserted.isEmpty());
    }

    @Test
    public void indexReceivesWriteTimeDeletionsCorrectly() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        String indexName = "test_index";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(d) USING '%s'",
                                  indexName, StubIndex.class.getName()));

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 2, 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 3, 3);

        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
        StubIndex index = (StubIndex)indexManager.getIndexByName(indexName);
        assertEquals(4, index.rowsInserted.size());
        assertTrue(index.partitionDeletions.isEmpty());
        assertTrue(index.rangeTombstones.isEmpty());

        execute("DELETE FROM %s WHERE a=0 AND b=0");
        assertTrue(index.partitionDeletions.isEmpty());
        assertEquals(1, index.rangeTombstones.size());

        execute("DELETE FROM %s WHERE a=0");
        assertEquals(1, index.partitionDeletions.size());
        assertEquals(1, index.rangeTombstones.size());
    }
    @Test
    public void nonCustomIndexesRequireExactlyOneTargetColumn() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY (k,c))");

        assertInvalidMessage("Only CUSTOM indexes support multiple columns", "CREATE INDEX multi_idx on %s(v1,v2)");
        assertInvalidMessage("Only CUSTOM indexes can be created without specifying a target column",
                           "CREATE INDEX no_targets on %s()");

        createIndex(String.format("CREATE CUSTOM INDEX multi_idx ON %%s(v1, v2) USING '%s'", StubIndex.class.getName()));
        assertIndexCreated("multi_idx", "v1", "v2");
    }

    @Test
    public void rejectDuplicateColumnsInTargetList() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY (k,c))");

        assertInvalidMessage("Duplicate column v1 in index target list",
                             String.format("CREATE CUSTOM INDEX ON %%s(v1, v1) USING '%s'",
                                           StubIndex.class.getName()));

        assertInvalidMessage("Duplicate column v1 in index target list",
                             String.format("CREATE CUSTOM INDEX ON %%s(v1, v1, c, c) USING '%s'",
                                           StubIndex.class.getName()));
    }

    @Test
    public void requireFullQualifierForFrozenCollectionTargets() throws Throwable
    {
        // this is really just to prove that we require the full modifier on frozen collection
        // targets whether the index is multicolumn or not
        createTable("CREATE TABLE %s(" +
                    " k int," +
                    " c int," +
                    " fmap frozen<map<int, text>>," +
                    " flist frozen<list<int>>," +
                    " fset frozen<set<int>>," +
                    " PRIMARY KEY(k,c))");

        assertInvalidMessage("Cannot create keys() index on frozen column fmap. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, keys(fmap)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create entries() index on frozen column fmap. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, entries(fmap)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create values() index on frozen column fmap. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, fmap) USING'%s'", StubIndex.class.getName()));

        assertInvalidMessage("Cannot create keys() index on frozen column flist. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, keys(flist)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create entries() index on frozen column flist. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, entries(flist)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create values() index on frozen column flist. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, flist) USING'%s'", StubIndex.class.getName()));

        assertInvalidMessage("Cannot create keys() index on frozen column fset. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, keys(fset)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create entries() index on frozen column fset. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, entries(fset)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create values() index on frozen column fset. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, fset) USING'%s'", StubIndex.class.getName()));

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, full(fmap)) USING'%s'", StubIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, full(flist)) USING'%s'", StubIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, full(fset)) USING'%s'", StubIndex.class.getName()));
    }

    @Test
    public void defaultIndexNameContainsTargetColumns() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY(k,c))");

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1, v2) USING '%s'", StubIndex.class.getName()));
        assertEquals(1, getCurrentColumnFamilyStore().metadata.getIndexes().size());
        assertIndexCreated(currentTable() + "_idx", "v1", "v2");

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v1, v2) USING '%s'", StubIndex.class.getName()));
        assertEquals(2, getCurrentColumnFamilyStore().metadata.getIndexes().size());
        assertIndexCreated(currentTable() + "_idx_1", "c", "v1", "v2");

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v2) USING '%s'", StubIndex.class.getName()));
        assertEquals(3, getCurrentColumnFamilyStore().metadata.getIndexes().size());
        assertIndexCreated(currentTable() + "_idx_2", "c", "v2");

        // duplicate the previous index with some additional options and check the name is generated as expected
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v2) USING '%s' WITH OPTIONS = {'foo':'bar'}",
                                  StubIndex.class.getName()));
        assertEquals(4, getCurrentColumnFamilyStore().metadata.getIndexes().size());
        Map<String, String> options = new HashMap<>();
        options.put("foo", "bar");
        assertIndexCreated(currentTable() + "_idx_3", options, "c", "v2");
    }

    @Test
    public void createMultiColumnIndexes() throws Throwable
    {
        // smoke test for various permutations of multicolumn indexes
        createTable("CREATE TABLE %s (" +
                    " pk1 int," +
                    " pk2 int," +
                    " c1 int," +
                    " c2 int," +
                    " v1 int," +
                    " v2 int," +
                    " mval map<text, int>," +
                    " lval list<int>," +
                    " sval set<int>," +
                    " fmap frozen<map<text,int>>," +
                    " flist frozen<list<int>>," +
                    " fset frozen<set<int>>," +
                    " PRIMARY KEY ((pk1, pk2), c1, c2))");

        testCreateIndex("idx_1", "pk1", "pk2");
        testCreateIndex("idx_2", "pk1", "c1");
        testCreateIndex("idx_3", "pk1", "c2");
        testCreateIndex("idx_4", "c1", "c2");
        testCreateIndex("idx_5", "c2", "v1");
        testCreateIndex("idx_6", "v1", "v2");
        testCreateIndex("idx_7", "pk2", "c2", "v2");
        testCreateIndex("idx_8", "pk1", "c1", "v1", "mval", "sval", "lval");

        createIndex(String.format("CREATE CUSTOM INDEX inc_frozen ON %%s(" +
                                  "  pk2, c2, v2, full(fmap), full(fset), full(flist)" +
                                  ") USING '%s'",
                                  StubIndex.class.getName()));
        assertIndexCreated("inc_frozen",
                           new HashMap<>(),
                           ImmutableList.of(indexTarget("pk2", IndexTarget.Type.VALUES),
                                            indexTarget("c2", IndexTarget.Type.VALUES),
                                            indexTarget("v2", IndexTarget.Type.VALUES),
                                            indexTarget("fmap", IndexTarget.Type.FULL),
                                            indexTarget("fset", IndexTarget.Type.FULL),
                                            indexTarget("flist", IndexTarget.Type.FULL)));

        createIndex(String.format("CREATE CUSTOM INDEX all_teh_things ON %%s(" +
                                  "  pk1, pk2, c1, c2, v1, v2, keys(mval), lval, sval, full(fmap), full(fset), full(flist)" +
                                  ") USING '%s'",
                                  StubIndex.class.getName()));
        assertIndexCreated("all_teh_things",
                           new HashMap<>(),
                           ImmutableList.of(indexTarget("pk1", IndexTarget.Type.VALUES),
                                            indexTarget("pk2", IndexTarget.Type.VALUES),
                                            indexTarget("c1", IndexTarget.Type.VALUES),
                                            indexTarget("c2", IndexTarget.Type.VALUES),
                                            indexTarget("v1", IndexTarget.Type.VALUES),
                                            indexTarget("v2", IndexTarget.Type.VALUES),
                                            indexTarget("mval", IndexTarget.Type.KEYS),
                                            indexTarget("lval", IndexTarget.Type.VALUES),
                                            indexTarget("sval", IndexTarget.Type.VALUES),
                                            indexTarget("fmap", IndexTarget.Type.FULL),
                                            indexTarget("fset", IndexTarget.Type.FULL),
                                            indexTarget("flist", IndexTarget.Type.FULL)));
    }

    @Test
    public void createMultiColumnIndexIncludingUserTypeColumn() throws Throwable
    {
        String myType = KEYSPACE + '.' + createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 frozen<" + myType + ">)");
        testCreateIndex("udt_idx", "v1", "v2");
        Indexes indexes = getCurrentColumnFamilyStore().metadata.getIndexes();
        IndexMetadata expected = IndexMetadata.fromIndexTargets(getCurrentColumnFamilyStore().metadata,
                                                                ImmutableList.of(indexTarget("v1", IndexTarget.Type.VALUES),
                                                                                 indexTarget("v2", IndexTarget.Type.VALUES)),
                                                                "udt_idx",
                                                                IndexMetadata.Kind.CUSTOM,
                                                                ImmutableMap.of(CUSTOM_INDEX_OPTION_NAME,
                                                                                StubIndex.class.getName()));
        IndexMetadata actual = indexes.get("udt_idx").orElseThrow(throwAssert("Index udt_idx not found"));
        assertEquals(expected, actual);
    }

    @Test
    public void createIndexWithoutTargets() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY(k,c))");
        // only allowed for CUSTOM indexes
        assertInvalidMessage("Only CUSTOM indexes can be created without specifying a target column",
                             "CREATE INDEX ON %s()");

        // parentheses are mandatory
        assertInvalidSyntax("CREATE CUSTOM INDEX ON %%s USING '%s'", StubIndex.class.getName());
        createIndex(String.format("CREATE CUSTOM INDEX no_targets ON %%s() USING '%s'", StubIndex.class.getName()));
        assertIndexCreated("no_targets", new HashMap<>());
    }

    @Test
    public void testCustomIndexExpressionSyntax() throws Throwable
    {
        Object[] row = row(0, 0, 0, 0);
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", row);

        assertInvalidMessage(String.format(IndexRestrictions.INDEX_NOT_FOUND, "custom_index", keyspace(), currentTable()),
                             "SELECT * FROM %s WHERE expr(custom_index, 'foo bar baz')");

        createIndex(String.format("CREATE CUSTOM INDEX custom_index ON %%s(c) USING '%s'", StubIndex.class.getName()));

        assertInvalidMessage(String.format(IndexRestrictions.INDEX_NOT_FOUND, "no_such_index", keyspace(), currentTable()),
                             "SELECT * FROM %s WHERE expr(no_such_index, 'foo bar baz ')");

        // simple case
        assertRows(execute("SELECT * FROM %s WHERE expr(custom_index, 'foo bar baz')"), row);
        assertRows(execute("SELECT * FROM %s WHERE expr(\"custom_index\", 'foo bar baz')"), row);
        assertRows(execute("SELECT * FROM %s WHERE expr(custom_index, $$foo \" ~~~ bar Baz$$)"), row);

        // multiple expressions on the same index
        assertInvalidMessage(IndexRestrictions.MULTIPLE_EXPRESSIONS,
                             "SELECT * FROM %s WHERE expr(custom_index, 'foo') AND expr(custom_index, 'bar')");

        // multiple expressions on different indexes
        createIndex(String.format("CREATE CUSTOM INDEX other_custom_index ON %%s(d) USING '%s'", StubIndex.class.getName()));
        assertInvalidMessage(IndexRestrictions.MULTIPLE_EXPRESSIONS,
                             "SELECT * FROM %s WHERE expr(custom_index, 'foo') AND expr(other_custom_index, 'bar')");

        assertInvalidMessage(SelectStatement.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE expr(custom_index, 'foo') AND d=0");
        assertRows(execute("SELECT * FROM %s WHERE expr(custom_index, 'foo') AND d=0 ALLOW FILTERING"), row);
    }

    @Test
    public void customIndexDoesntSupportCustomExpressions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE CUSTOM INDEX custom_index ON %%s(c) USING '%s'",
                                  NoCustomExpressionsIndex.class.getName()));
        assertInvalidMessage(String.format( IndexRestrictions.CUSTOM_EXPRESSION_NOT_SUPPORTED, "custom_index"),
                             "SELECT * FROM %s WHERE expr(custom_index, 'foo bar baz')");
    }

    @Test
    public void customIndexRejectsExpressionSyntax() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE CUSTOM INDEX custom_index ON %%s(c) USING '%s'",
                                  ExpressionRejectingIndex.class.getName()));
        assertInvalidMessage("None shall pass", "SELECT * FROM %s WHERE expr(custom_index, 'foo bar baz')");
    }

    @Test
    public void customExpressionsMustTargetCustomIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX non_custom_index ON %s(c)");
        assertInvalidMessage(String.format(IndexRestrictions.NON_CUSTOM_INDEX_IN_EXPRESSION, "non_custom_index"),
                             "SELECT * FROM %s WHERE expr(non_custom_index, 'c=0')");
    }

    @Test
    public void customExpressionsDisallowedInModifications() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE CUSTOM INDEX custom_index ON %%s(c) USING '%s'", StubIndex.class.getName()));

        assertInvalidMessage(ModificationStatement.CUSTOM_EXPRESSIONS_NOT_ALLOWED,
                             "DELETE FROM %s WHERE expr(custom_index, 'foo bar baz ')");
        assertInvalidMessage(ModificationStatement.CUSTOM_EXPRESSIONS_NOT_ALLOWED,
                             "UPDATE %s SET d=0 WHERE expr(custom_index, 'foo bar baz ')");
    }

    private void testCreateIndex(String indexName, String... targetColumnNames) throws Throwable
    {
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(%s) USING '%s'",
                                  indexName,
                                  Arrays.stream(targetColumnNames).collect(Collectors.joining(",")),
                                  StubIndex.class.getName()));
        assertIndexCreated(indexName, targetColumnNames);
    }

    private void assertIndexCreated(String name, String... targetColumnNames)
    {
        assertIndexCreated(name, new HashMap<>(), targetColumnNames);
    }

    private void assertIndexCreated(String name, Map<String, String> options, String... targetColumnNames)
    {
        List<IndexTarget> targets = Arrays.stream(targetColumnNames)
                                          .map(s -> new IndexTarget(ColumnIdentifier.getInterned(s, true),
                                                                    IndexTarget.Type.VALUES))
                                          .collect(Collectors.toList());
        assertIndexCreated(name, options, targets);
    }

    private void assertIndexCreated(String name, Map<String, String> options, List<IndexTarget> targets)
    {
        // all tests here use StubIndex as the custom index class,
        // so add that to the map of options
        options.put(CUSTOM_INDEX_OPTION_NAME, StubIndex.class.getName());
        CFMetaData cfm = getCurrentColumnFamilyStore().metadata;
        IndexMetadata expected = IndexMetadata.fromIndexTargets(cfm, targets, name, IndexMetadata.Kind.CUSTOM, options);
        Indexes indexes = getCurrentColumnFamilyStore().metadata.getIndexes();
        for (IndexMetadata actual : indexes)
            if (actual.equals(expected))
                return;

        fail(String.format("Index %s not found in CFMetaData", expected));
    }

    private static IndexTarget indexTarget(String name, IndexTarget.Type type)
    {
        return new IndexTarget(ColumnIdentifier.getInterned(name, true), type);
    }

    public static final class IndexIncludedInBuild extends StubIndex
    {
        public IndexIncludedInBuild(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public boolean shouldBuildBlocking()
        {
            return true;
        }
    }

    public static final class IndexExcludedFromBuild extends StubIndex
    {
        public IndexExcludedFromBuild(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public boolean shouldBuildBlocking()
        {
            return false;
        }
    }

    public static final class NoCustomExpressionsIndex extends StubIndex
    {
        public NoCustomExpressionsIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public AbstractType<?> customExpressionValueType()
        {
            return null;
        }
    }

    public static final class ExpressionRejectingIndex extends StubIndex
    {
        public ExpressionRejectingIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public Searcher searcherFor(ReadCommand command) throws InvalidRequestException
        {
            throw new InvalidRequestException("None shall pass");
        }
    }
}
