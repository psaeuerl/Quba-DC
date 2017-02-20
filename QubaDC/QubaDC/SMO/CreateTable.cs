using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public class CreateTable : SchemaModificationOperator
    {
        public String TableName { get; set; }

        public String Schema { get; set; }

        public ColumnDefinition[] Columns { get; set; }

        public override void Accept(SMOVisitor visitor)
        {
            visitor.Visit(this);
        }    

        public TableSchema ToTableSchema()
        {
            return new TableSchema()
            {
                Columns = this.Columns.Select(x => x.ColumName).ToArray(),
                Name = TableName,
                Schema = Schema
            };
        }

        public Table ToTable()
        {
            return new Table()
            {
                TableName = this.TableName,
                TableSchema = this.Schema
            };
        }

        public string[] GetColumnNames()
        {
            return this.Columns.Select(x => x.ColumName).ToArray();
        }
    }
}
