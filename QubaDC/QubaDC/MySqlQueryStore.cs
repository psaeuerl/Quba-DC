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

        internal override string RenderSelectForQueryStore(Guid gUID)
        {
            String query = @"SELECT `querystore`.`ID`,
    `querystore`.`Query`,
    `querystore`.`QuerySerialized`,
    `querystore`.`ReWrittenQuery`,
    `querystore`.`ReWrittenQuerySerialized`,
    `querystore`.`Timestamp`,
    `querystore`.`Hash`,
    `querystore`.`HashSelect`,
    `querystore`.`HashSelectSerialized`,
    `querystore`.`GUID`
FROM `{0}`.`{1}`
WHERE `querystore`.`GUID` = '{2}';
";
            String guid = gUID.ToString();
            String schema = this.TypedConnection.DataBase;
            String table = QueryStore.QueryStoreTable;

            String result = String.Format(query, schema, table, guid);
            return result;            
        }

        protected override string GetCreateQueryStoreTableStatement()
        {

            String Statemnet =
   @"CREATE TABLE `"+this.TypedConnection.DataBase+ @"`.`"+QueryStore.QueryStoreTable+ @"` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `Query` MEDIUMTEXT NOT NULL,
  `QuerySerialized` MEDIUMTEXT NOT NULL,
  `ReWrittenQuery` MEDIUMTEXT NOT NULL,
  `ReWrittenQuerySerialized` MEDIUMTEXT NOT NULL,
  `Timestamp` DATETIME(3) NULL,
  `Hash` MEDIUMTEXT NOT NULL,
  `HashSelectSerialized` MEDIUMTEXT NOT NULL,
  `HashSelect` MEDIUMTEXT NOT NULL,
  `GUID` VARCHAR(50) NOT NULL,
  `AdditionalInformation` MEDIUMTEXT NULL,
  PRIMARY KEY (`ID`));";
            return Statemnet;

        }

        internal override string RenderInsert(string originalrenderd, string originalSerialized, string rewrittenSerialized, string select, string time, string hash, Guid guid, String hashselect, String hashselectserialized, String additionalinformation)
        {
            String insert = @"INSERT INTO `{0}`.`{1}`
(
`Query`,
`QuerySerialized`,
`ReWrittenQuery`,
`ReWrittenQuerySerialized`,
`Timestamp`,
`Hash`,
`HashSelect`,
`HashSelectSerialized`,
`GUID`,
`AdditionalInformation`)
VALUES
('{2}'
,'{3}'
,'{4}'
,'{5}'
,{6}
,'{7}'
,'{10}'
,'{9}'
,'{8}'
,{11});";

            String result = String.Format(insert,
                this.TypedConnection.DataBase,
                QueryStore.QueryStoreTable,
                originalrenderd.Replace("'", "\\'").Replace(System.Environment.NewLine, " "),
                originalSerialized.Replace("'", "\\'").Replace(System.Environment.NewLine, " "),
                rewrittenSerialized.Replace("'", "\\'").Replace(System.Environment.NewLine, " "),
                select.Replace("'", "\\'").Replace(System.Environment.NewLine, " "),
                time,
                hash == null? "null" : hash.Replace("'", "\\'"),
                guid.ToString(),
                hashselect.Replace("'", "\\'").Replace(System.Environment.NewLine, " "),
                hashselectserialized.Replace("'", "\\'").Replace(System.Environment.NewLine, " "),
                additionalinformation == null ? "null" : "'"+additionalinformation+"'")
                ;
            return result;
        }

    }
}
