using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySQLGlobalUpdateTimeManager : GlobalUpdateTimeManager
    {
        private MySQLDataConnection Connection { get; set; }

        public MySQLGlobalUpdateTimeManager(MySQLDataConnection con)
        {
            this.Connection = con;

        }
        public override string GetCreateUpdateTimeTableStatement()
        {
            String stmt =
@"CREATE TABLE `" + this.Connection.DataBase + @"`.`"+QubaDC.QubaDCSystem.GlobalUpdateTableName+ @"` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `Timestamp` DATETIME(6) NOT NULL,
  `Operation` VARCHAR(1000) NOT NULL,
  PRIMARY KEY (`ID`));
";
            return stmt;
        }
    }
}
