using System;
using System.Collections.Generic;
using QubaDC.CRUD;
using QubaDC.Utility;
using System.Linq;

namespace QubaDC.DatabaseObjects
{

    public class Schema
    {
        public IEnumerable<TableSchemaWithHistTable> Tables { get { return this._Tables; } }
        public IEnumerable<TableSchema> HistTables { get { return this._HistTables; } }

        private List<TableSchemaWithHistTable> _Tables { get ; set; } = new List<TableSchemaWithHistTable>();

        private List<TableSchema> _HistTables { get;  set; } = new List<TableSchema>();      

        public void AddTable(TableSchema table, TableSchema histequivalent)
        {
            AssertTableColumns(table);
            AssertTableColumns(histequivalent);
            table.AddTimeSetGuid = Guid.NewGuid();
            this._Tables.Add(new TableSchemaWithHistTable()
            {
                Table = table,
                HistTableName = histequivalent.Name,
                HistTableSchema = histequivalent.Schema
            });
            this._HistTables.Add(histequivalent);
        }

        private void AssertTableColumns(TableSchema table)
        {
            String[] columns = table.Columns.ToArray();
            String[] columnDefColumns = table.ColumnDefinitions.Select(x => x.ColumName).ToArray();
            var columnsExceptColumnDef = columns.Except(columnDefColumns).ToArray();
            var columnsDefExceptColumns = columnDefColumns.Except(columns).ToArray();
            Guard.StateTrue(columnsExceptColumnDef.Count() == 0, "Could not find ColumnDef: " + String.Join(",", columnsExceptColumnDef));
            Guard.StateTrue(columnsDefExceptColumns.Count() == 0, "Could not find Column: " + String.Join(",", columnsDefExceptColumns));
        }

        public TableSchema FindHistTable(Table insertTable)
        {
            TableSchemaWithHistTable withHist=
                this._Tables.Find(x => x.Table.Name == insertTable.TableName && x.Table.Schema == insertTable.TableSchema);
            if (withHist == null)
                throw new InvalidOperationException("Schema does not contain Table: " + insertTable.TableSchema + "." + insertTable.TableName);
            TableSchema h = this._HistTables.Find(x => x.Schema == withHist.HistTableSchema && x.Name == withHist.HistTableName);
            return h; 
        }

        public TableSchemaWithHistTable FindTable(String Schema, String Name)
        {
            return FindTable(new Table() { TableSchema = Schema, TableName = Name });
        }

        public Boolean ContainsTable(String Schema, String Name)
        {
            var tables = _Tables.Where(x => x.MatchesTable(new Table() { TableName = Name, TableSchema = Schema })).ToArray();
            Guard.StateTrue(tables.Length <= 1, "Expected to find zero or one matching table for: " + Schema + "." + Name + " got: " + tables.Length);

            return tables.Length == 1;
        }

        public TableSchemaWithHistTable FindTable(Table t)
        {
            var tables = _Tables.Where(x => x.MatchesTable(t)).ToArray();
            Guard.StateTrue(tables.Length == 1, "Expected to find one matching table for: " + t.TableSchema + "." + t.TableName + " got: " + tables.Length);
            var table = tables.First();
            return table;
        }

        internal void RemoveTable(Table oldTable)
        {
            TableSchemaWithHistTable t = this.FindTable(oldTable);
            TableSchema histTable = this.FindHistTable(oldTable);
            Guard.StateTrue(_Tables.Remove(t), "Could not remove table");
            Guard.StateTrue(_HistTables.Remove(histTable), "Could not remove histTable");

        }
    }
}
