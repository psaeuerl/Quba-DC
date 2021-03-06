﻿using QubaDC.CRUD;
using System;

namespace QubaDC.DatabaseObjects
{
    public class TableSchemaWithHistTable 
    {
        public TableSchema Table { get; set; }
        public String HistTableName { get; set; }
        public String HistTableSchema { get; set; }

        public String MetaTableName { get; set; }

        public String MetaTableSchema { get; set; }
        public  bool MatchesTable(Table t)
        {
            return this.Table.Name == t.TableName && this.Table.Schema == t.TableSchema;
        }


        internal Table ToTable()
        {
            return Table.ToTable();
        }
    }
}