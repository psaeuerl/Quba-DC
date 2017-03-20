using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;
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
        public string[] PrimaryKey { get; set; }

        public override void Accept(SMOVisitor visitor)
        {
            visitor.Visit(this);
        }    

        public void ThrowIfNotValid()
        {
            Guard.ArgumentNotNullOrWhiteSpace(TableName, nameof(TableName));
            Guard.ArgumentNotNullOrWhiteSpace(Schema, nameof(Schema));
            foreach (var x in Columns)
                x.ThrowIfNotValid();
            
            Guard.ArgumentNotNull(PrimaryKey,nameof(PrimaryKey));
            if (PrimaryKey.Length == 0)
                return;
            var NotContained = PrimaryKey.Except(Columns.Select(x => x.ColumName));
            Guard.StateTrue(NotContained.Count() == 0, "Following PKS are not contained: " + String.Join(",", NotContained));
        }

        //public TableSchema ToTableSchema()
        //{
        //    return new TableSchema()
        //    {
        //        Columns = this.Columns.Select(x => x.ColumName).ToArray(),
        //        Name = TableName,
        //        Schema = Schema
        //    };
        //}

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

        internal FromTable ToFromTable(string reference)
        {
            return new FromTable()
            {
                TableName = this.TableName,
                TableSchema = this.Schema,
                 TableAlias = reference
            };
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

    }
}
