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
   @"CREATE TABLE `"+this.TypedConnection.DataBase+ @"`.`querystore` (
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

        internal override int StoreResult(QueryStoreSelectResult res)
        {
            ;
            throw new NotImplementedException();
        }
    }
}
