using System;

namespace QubaDC.DatabaseObjects
{
    public class TableSchemaWithHistTable 
    {
        public TableSchema Table { get; set; }
        public String HistTableName { get; set; }
        public String HistTableSchema { get; set; }
    }
}