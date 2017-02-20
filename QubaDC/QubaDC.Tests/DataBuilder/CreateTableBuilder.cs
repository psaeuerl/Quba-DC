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
                }
            };
        }
    }
}
