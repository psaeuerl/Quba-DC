using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySqlQueryStore : QueryStore
    {

        public MySqlQueryStore(MySQLDataConnection dataConnection, QueryStoreSelectHandler handler) : base(dataConnection, handler)
        {
            this.TypedConnection = dataConnection;          
        }

        public MySQLDataConnection TypedConnection { get; private set; }

        protected override string GetCreateQueryStoreTableStatement()
        {

            String Statemnet =
   @"CREATE TABLE `"+this.TypedConnection.DataBase+ @"`.`"+QueryStore.QueryStoreTable+@"` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `Query` MEDIUMTEXT NOT NULL,
  `QuerySerialized` MEDIUMTEXT NOT NULL,
  `ReWrittenQuery` MEDIUMTEXT NOT NULL,
  `ReWrittenQuerySerialized` MEDIUMTEXT NOT NULL,
  `Timestamp` DATETIME(3) NULL,
  `Hash` MEDIUMTEXT NOT NULL,
  `GUID` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`ID`));";
            return Statemnet;

        }

        internal override string RenderInsert(string originalrenderd, string originalSerialized, string rewrittenSerialized, string select, string time, string hash, Guid guid)
        {
            String insert = @"INSERT INTO `{0}`.`{1}`
(
`Query`,
`QuerySerialized`,
`ReWrittenQuery`,
`ReWrittenQuerySerialized`,
`Timestamp`,
`Hash`,
`GUID`)
VALUES
('{2}'
,'{3}'
,'{4}'
,'{5}'
,{6}
,'{7}'
,'{8}');";

            String result = String.Format(insert,
                this.TypedConnection.DataBase, 
                QueryStore.QueryStoreTable, 
                originalrenderd.Replace("'","\\'").Replace(System.Environment.NewLine," "), 
                originalSerialized.Replace("'", "\\'").Replace(System.Environment.NewLine, " "),
                rewrittenSerialized.Replace("'", "\\'").Replace(System.Environment.NewLine, " "),
                select.Replace("'", "\\'").Replace(System.Environment.NewLine, " "),
                time, 
                hash.Replace("'", "\\'"),
                guid.ToString());
            return result;
        }

    }
}
