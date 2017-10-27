using QubaDC.CRUD;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Evaluation
{
    public class SelectOperationGenerator
    {

        public static SelectOperation GetBasicSelectAll(String dbName)
        {
            CreateTable ct = EvaluationCreateTable.GetTable(dbName);
            SelectOperation s1 = new SelectOperation()
            {
                Columns = ct.Columns.Select(x => new ColumnReference() { ColumnName = x.ColumName, TableReference = "t1" }).ToArray(),
                FromTable = new FromTable()
                {
                    TableAlias = "t1",
                    TableName = ct.TableName,
                    TableSchema = ct.Schema
                },
            };
            return s1;
        }
    }
}
