using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.SMO;

namespace QubaDC.DatabaseObjects
{
    public class TableSchema
    {
        public String Name { get; set; }
        public String Schema { get; set; }

        public String[] Columns { get; set; }

        public ColumnDefinition[] ColumnDefinitions { get; set; }

        public TableSchema()
        {
            this.Name = String.Empty;
            this.Schema = String.Empty; ;
            this.Columns = new string[] { };
        }

        public TableSchema(String Schema, String name, params String[] columns)
        {
            this.Name = name;
            this.Schema = Schema;
            this.Columns = columns;
        }

        internal Table ToTable()
        {
            return new Table()
            {
                TableName = Name,
                TableSchema = Schema
            };
        }
    }
}
