using QubaDC.CRUD;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Tests.DataBuilder
{
    public class CreateTableBuilder
    {
        public static CreateTable BuildBasicTable(String schema)
        {
            return new CreateTable()
            {
                TableName = "baisctable",
                Schema = schema,
                Columns = new ColumnDefinition[] {
                    new ColumnDefinition() {  ColumName = "ID",  DataType =" INT", Nullable = false },
                    new ColumnDefinition() {  ColumName = "Schema",  DataType =" MediumText", Nullable = false }
                },
                PrimaryKey = new String[] {"ID"}
            };
        }

        public static InsertOperation GetBasicTableInsert(string currentdatabase, string id, string value)
        {
            CreateTable t = CreateTableBuilder.BuildBasicTable(currentdatabase);
            return new InsertOperation()
            {
                ColumnNames = t.GetColumnNames(),
                InsertTable = t.ToTable(),
                ValueLiterals = new String[] {id, value}
            };
        }
    }
}
