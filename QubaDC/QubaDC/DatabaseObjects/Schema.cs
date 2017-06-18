﻿using System;
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
            this._Tables.Add(new TableSchemaWithHistTable()
            {
                Table = table,
                HistTableName = histequivalent.Name,
                HistTableSchema = histequivalent.Schema
            });
            this._HistTables.Add(histequivalent);
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
