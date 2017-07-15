using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.DatabaseObjects;

namespace QubaDC.Hybrid.CRUD
{
    class HybridSelectHandler
    {
        public HybridSelectHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public SchemaManager SchemaManager { get; private set; }

        internal String HandleSelect(SelectOperation selectOperation,Boolean RenderAsHash)
        {
            String Format =
@"SELECT
{0}
FROM {1}
{2}
{3}"; //0 => Columns, 1 => FROM Part, 2 => Where, 3 => Order By
            String columns = "";
            if (!RenderAsHash)
                columns = RenderColumns(selectOperation.Columns,selectOperation.LiteralColumns);
            else
                columns = this.RenderAsHash(selectOperation.Columns,selectOperation.LiteralColumns);
            String fromPart = RenderFromPart(selectOperation);
            String wherePart = RenderWherePart(selectOperation.Restriction);
            String OrderBy = RenderOrderBy(selectOperation.SortingColumns);
            String select = String.Format(Format, columns, fromPart, wherePart, OrderBy);
            return select;

        }

        private string RenderAsHash(ColumnReference[] columns, LiteralColumn[] literalColumns)
        {
            var cols = columns.Select(x => RenderColumn(x)).Union(literalColumns.Select(x => RenderLiteralColumn(x))).Select(x => "MD5(" + x + ")").ToArray();
            var res = String.Join(", ", cols);
            String sel = String.Format("MD5( GROUP_CONCAT( CONCAT_WS('#',{0}) SEPARATOR '#' ) )", res);
            return sel;
        }

        internal string HandleHybridSelect(SelectOperation selectOperation, SchemaInfo s, bool RenderAsHash)
        {

            if (RenderAsHash)
            {

                String Format =
    @"SELECT
{0}
FROM {1}
"; //0 => Columns, 1 => FROM Part, 2 => Where, 3 => Order By

                String fromPart = RenderHybridFromPart(selectOperation, s);
                String wherePart = RenderWherePart(selectOperation.Restriction);
                String OrderBy = RenderOrderBy(selectOperation.SortingColumns);

                String inerselectAlias = "innerSelect";
                String innerColumns  = RenderColumns(selectOperation.Columns, selectOperation.LiteralColumns);
                //https://mariadb.com/kb/en/mariadb/why-is-order-by-in-a-from-subquery-ignored/
                String innerSelect = String.Format("(SELECT {0} FROM {1} {2} {3} LIMIT 18446744073709551615) AS {4}", innerColumns,fromPart,wherePart,OrderBy, inerselectAlias);

                ColumnReference[] outerColumns = selectOperation.Columns.Select(x => new ColumnReference()
                {
                    ColumnName = x.ColumnName,
                    TableReference = inerselectAlias
                }).ToArray();


                String outerHash = this.RenderAsHash(outerColumns, selectOperation.LiteralColumns);
                String select = String.Format(Format, outerHash, innerSelect);
                return select;
            }
            else
            {

                String Format =
    @"SELECT
{0}
FROM {1}
{2}
{3}"; //0 => Columns, 1 => FROM Part, 2 => Where, 3 => Order By
                String columns = "";
                if (!RenderAsHash)
                    columns = RenderColumns(selectOperation.Columns, selectOperation.LiteralColumns);
                else
                    columns = this.RenderAsHash(selectOperation.Columns, selectOperation.LiteralColumns);
                String fromPart = RenderHybridFromPart(selectOperation, s);
                String wherePart = RenderWherePart(selectOperation.Restriction);
                String OrderBy = RenderOrderBy(selectOperation.SortingColumns);
                String select = String.Format(Format, columns, fromPart, wherePart, OrderBy);
                return select;
            }
        }

        private string RenderHybridFromPart(SelectOperation selectOperation, SchemaInfo s)
        {
            var histTable = s.Schema.FindHistTable(selectOperation.FromTable);
            String table = RenderHybridFromTable(selectOperation.FromTable,histTable);
            var parts = selectOperation.JoinedTables.Select(x => RenderHybridJoinedTable(x,s));
            var joined = String.Join(System.Environment.NewLine, parts);
            var result = String.Join(System.Environment.NewLine, table, joined);
            return table;
        }

        private object RenderHybridJoinedTable(JoinedTable x, SchemaInfo s)
        {

            String Format = "{0} {1} {2}";
            //0 => condition
            //1 => tablename
            //2 => joinrestriction
            String joinType = CRUDRenderer.RenderJoinType(x.Join);
            FromTable t = new FromTable()
            {
                TableAlias = x.TableAlias,
                TableName = x.TableName,
                TableSchema = x.TableSchema
            };
            var histTable = s.Schema.FindHistTable(t);

            String table = RenderHybridFromTable(t, histTable);

            String rest = CRUDRenderer.RenderRestriction(x.JoinCondition);
            String result = String.Format(Format, joinType, table, rest);
            return result;
        }

        private string RenderHybridFromTable(FromTable fromTable, TableSchema histTableSchema)
        {

            string baseFormat = "(SELECT *, null as {0} FROM {1} UNION Select * from {2}) {3}";
            String endTsColumn = CRUDRenderer.Quote(HybridConstants.EndTS);
            String baseTable = CRUDRenderer.Quote(fromTable.TableSchema) + "."
                                 + CRUDRenderer.Quote(fromTable.TableName);

            string histTable = CRUDRenderer.Quote(histTableSchema.Schema) + "."
                            + CRUDRenderer.Quote(histTableSchema.Name);
            string alias = " AS " + CRUDRenderer.Quote(fromTable.TableAlias);


            String result = String.Format(baseFormat, endTsColumn, baseTable, histTable, alias);
            return result;            
        }

        private string RenderColumns(ColumnReference[] columns, LiteralColumn[] literalColumns)
        {
            var cols = columns.Select(x => RenderColumn(x)).Union(literalColumns.Select(x => RenderLiteralColumn(x)));
            var res = String.Join(", ", cols);
            return res;
    }

        private string RenderLiteralColumn(LiteralColumn x)
        {
            return x.ColumnLiteral + " AS " + CRUDRenderer.Quote(x.ColumnName);
        }

      
        private string RenderOrderBy(ColumnSorting[] sortingColumns)
        {
            String[] sortings = sortingColumns.Select(x => RenderSorintg(x)).ToArray();
            String result = String.Join(", ", sortings);
            if (result == "")
                return result;
            return "ORDER BY "+result;            
        }

        private String RenderSorintg(ColumnSorting x)
        {
            String asc = x.SortAscending ? "ASC" : "DESC";
            String result = RenderColumn(x.Column) + " " + asc;
            return result;
        }

        private string RenderWherePart(Restriction restriction)
        {
            String rest = CRUDRenderer.RenderRestriction(restriction);
            if (!String.IsNullOrWhiteSpace(rest))
                return "WHERE " + rest;
            else
                return "";
        }

        private string RenderFromPart(SelectOperation selectOperation)
        {
            String table = RenderFromTable(selectOperation.FromTable);
            var parts = selectOperation.JoinedTables.Select(x => RenderJoinedTable(x));
            var joined = String.Join(System.Environment.NewLine, parts);
            var result = String.Join(System.Environment.NewLine, table, joined);
            return result;
        }

        private String RenderJoinedTable(JoinedTable x)
        {            
            String Format = "{0} {1} {2}";
            //0 => condition
            //1 => tablename
            //2 => joinrestriction
            String joinType = CRUDRenderer.RenderJoinType(x.Join);
            String table = CRUDRenderer.Quote(x.TableSchema) + "."
                 + CRUDRenderer.Quote(x.TableName) +
                 (x.TableAlias == null ? "" :
                 "AS " + CRUDRenderer.Quote(x.TableAlias));

            String rest = CRUDRenderer.RenderRestriction(x.JoinCondition);
            String result = String.Format(Format, joinType, table, rest);
            return result;
        }


        private string RenderFromTable(FromTable fromTable)
        {
            return CRUDRenderer.Quote(fromTable.TableSchema) + "."
                 + CRUDRenderer.Quote(fromTable.TableName) +
                 (fromTable.TableAlias == null ? "" :
                 " AS " + CRUDRenderer.Quote(fromTable.TableAlias));
        }



        private String RenderColumn(ColumnReference x)
        {
            return CRUDRenderer.Quote(x.TableReference) +"."+ CRUDRenderer.Quote(x.ColumnName);

        }
    }
}