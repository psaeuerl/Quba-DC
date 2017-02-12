using QubaDC.CRUD;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySqlSMORenderer : SMORenderer
    {
        public override IEnumerable<string> GetHistoryTableColumns()
        {
            return new String[]
            {
                "`startts` TIMESTAMP(6) NOT NULL",
                "`endts` TIMESTAMP(6) NOT NULL",
                "`guid` char(36) NOT NULL",
            };
        }

        public override string RenderCreateTable(CreateTable ct, Boolean RemoveUnusedColumnInformation)
        {
            String stmt =
@"CREATE TABLE `{0}`.`{1}` (
{2}
);
";
            var columnDefinitions = ct.ColumnDefinitions;

            //PS => Not that good Design
            //Assumption
            //ColumnDefinitions have the form <columname> <datatype> <null/not null> ... other
            if (RemoveUnusedColumnInformation)
            {
                columnDefinitions = columnDefinitions.Select(x =>
                {
                    int indx = x.IndexOf(" NULL");
                    return x.Substring(0, indx + " NULL".Length);
                }).ToArray();
            }
            String result = String.Format(stmt, ct.Schema, ct.TableName, String.Join(","+System.Environment.NewLine, columnDefinitions));
            return result;
        }
    }
}
