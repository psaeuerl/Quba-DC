using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Evaluation
{
    public class EvaluationCreateTable
    {
        public static CreateTable GetTable(String dbName)
        {
            CreateTable t = new CreateTable()
            {
                Columns = new ColumnDefinition[]
                 {
                                      new ColumnDefinition() { ColumName = "PhaseNumber", DataType = "INT", Nullable = false },
                                       new ColumnDefinition() { ColumName = "ID", DataType = "INT", Nullable = false },
                                      new ColumnDefinition() { ColumName = "Section", DataType = "INT", Nullable = false },
                                     new ColumnDefinition() { ColumName = "ValueToUpdate", DataType = "INT", Nullable = false },
                                     new ColumnDefinition() {ColumName = "CLOBPayLoade", DataType = "MEDIUMTEXT", Nullable = false }
                 },
                Schema = dbName,
                TableName = "dataTable"

            };
            return t;
        }
    }
}
