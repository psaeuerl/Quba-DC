using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    class IntegratedSelectHandler
    {
        public IntegratedSelectHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender)
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

        internal string HandleIntegratedSelect(SelectOperation selectOperation, SchemaInfo exeuctionTimeSchema, SchemaInfo currentSchema, bool RenderAsHash, Dictionary<String, Guid?> TableRefToGuidMapping)
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
            String fromPart = RenderIntegratedFromPart(selectOperation, exeuctionTimeSchema, currentSchema, TableRefToGuidMapping);
            String wherePart = RenderWherePart(selectOperation.Restriction);
            String OrderBy = RenderOrderBy(selectOperation.SortingColumns);
            String select = String.Format(Format, columns, fromPart, wherePart, OrderBy);
            return select;
        }

        private string RenderIntegratedFromPart(SelectOperation selectOperation, SchemaInfo exeuctionTimeSchema, SchemaInfo currentSchema, Dictionary<string, Guid?> TableRefToGuidMapping)
        {
            var histTable = exeuctionTimeSchema.Schema.FindHistTable(selectOperation.FromTable);
            var expectedGuid = TableRefToGuidMapping[selectOperation.FromTable.TableAlias];

            Boolean currentSchemaContainsTable = false;
            TableSchemaWithHistTable current = null;
            if (expectedGuid.HasValue)
            {
                currentSchemaContainsTable = currentSchema.Schema.Tables.Any(x => x.Table.AddTimeSetGuid == expectedGuid.Value);
                if (currentSchemaContainsTable)
                    current = currentSchema.Schema.Tables.First(x => x.Table.AddTimeSetGuid == expectedGuid.Value);
            }

            ColumnReference[] columns = selectOperation.GetColumnsForTableReference(selectOperation.FromTable.TableAlias);
            string table = RenderIntegratedFromTable(selectOperation.FromTable, histTable, currentSchemaContainsTable);
            var parts = selectOperation.JoinedTables.Select(x => RenderIntegratedJoinedTable(selectOperation, x, exeuctionTimeSchema, currentSchema, TableRefToGuidMapping));
            var joined = String.Join(System.Environment.NewLine, parts);
            var result = String.Join(System.Environment.NewLine, table, joined);
            return table;
        }

        private string RenderIntegratedFromTable(FromTable fromTable, TableSchema histTable, bool currentSchemaContainsTable)
        {
            return currentSchemaContainsTable ? RenderFromTable(fromTable)
                                            : RenderFromTable(new FromTable()
                                            {
                                                TableAlias = fromTable.TableAlias,
                                                TableName = histTable.Name,
                                                TableSchema = histTable.Schema
                                            });
        }

        private object RenderIntegratedJoinedTable(SelectOperation selectOperation, JoinedTable x, SchemaInfo s, SchemaInfo currentSchema, Dictionary<String, Guid?> TableRefToGuidMapping)
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

            var expectedGuid = TableRefToGuidMapping[selectOperation.FromTable.TableAlias];
            Boolean currentSchemaContainsTable = false;
            TableSchemaWithHistTable current = null;
            if (expectedGuid.HasValue)
            {
                currentSchemaContainsTable = currentSchema.Schema.Tables.Any(y => y.Table.AddTimeSetGuid == expectedGuid.Value);
                current = currentSchema.Schema.Tables.First(y => y.Table.AddTimeSetGuid == expectedGuid.Value);
            }
            ColumnReference[] columns = selectOperation.GetColumnsForTableReference(x.TableAlias);

            String table = RenderIntegratedFromTable(t, histTable, currentSchemaContainsTable);

            String rest = CRUDRenderer.RenderRestriction(x.JoinCondition);
            String result = String.Format(Format, joinType, table, rest);
            return result;
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