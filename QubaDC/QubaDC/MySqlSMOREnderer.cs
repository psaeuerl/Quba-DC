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
    

        public override string RenderCreateTable(CreateTable ct, Boolean IncludeAdditionalInformation = true)
        {
            String stmt =
@"CREATE TABLE `{0}`.`{1}` (
{2}
);
";
            String[] columnDefinitions = ct.Columns.Select(x =>
                '`' + x.ColumName + "` "
                + x.DataType + " "
                + (x.Nullable ? "NULL" : "NOT NULL") + " "
                + (IncludeAdditionalInformation ? x.AdditionalInformation : ""))
            .ToArray();
            String columns = String.Join(", " + System.Environment.NewLine, columnDefinitions);
         
            String result = String.Format(stmt, ct.Schema, ct.TableName, String.Join(","+System.Environment.NewLine, columns));
            return result;
        }
    }
}
