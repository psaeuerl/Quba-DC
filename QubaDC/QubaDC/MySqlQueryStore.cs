using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySqlQueryStore : QueryStore
    {


        public MySqlQueryStore(MySQLDataConnection dataConnection) : base(dataConnection)
        {
            this.TypedConnection = dataConnection;
        }

        public MySQLDataConnection TypedConnection { get; private set; }

        protected override string GetCreateQueryStoreTableStatement()
        {

            String Statemnet =
   @"CREATE TABLE `"+this.TypedConnection.DataBase+@"`.`querystore` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `Query` MEDIUMTEXT NOT NULL,
  `ReWrittenQuery` MEDIUMTEXT NOT NULL,
  `Timestamp` DATETIME(6) NULL,
  `Checkvalue` MEDIUMTEXT NOT NULL,
  `SchemaVersionIssued` INT NOT NULL,
  PRIMARY KEY (`ID`));";
            return Statemnet;

        }
    }
}
