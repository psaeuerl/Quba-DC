using System;
using System.Collections.Generic;
using QubaDC.CRUD;
using QubaDC.Utility;

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
            this._Tables.Add(new TableSchemaWithHistTable()
            {
                Table = table,
                HistTableName = histequivalent.Name,
                HistTableSchema = histequivalent.Schema
            });
            this._HistTables.Add(histequivalent);
        }

        internal Table FindHistTable(Table insertTable)
        {
            TableSchemaWithHistTable withHist=
                this._Tables.Find(x => x.Table.Name == insertTable.TableName && x.Table.Schema == insertTable.TableSchema);
            TableSchema h = this._HistTables.Find(x => x.Schema == withHist.HistTableSchema && x.Name == withHist.HistTableName);
            return h.ToTable();
        }

    }
}
