using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySQLQubaDCSystem : QubaDCSystem
    {
        public MySQLQubaDCSystem(MySQLDataConnection con, SMOVisitor separatedSMOHandler, CRUDVisitor separatedCRUDHandler)
            : base(con, separatedSMOHandler, separatedCRUDHandler)
        {
            this.TypedConnection = con; ;
        }

        public MySQLDataConnection TypedConnection { get; private set; }

        protected override string GetCreateSMOTrackingTableStatement()
        {
            String stmt =
@"CREATE TABLE `"+this.TypedConnection.DataBase+@"`.`qubadcsmotable` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `Schema` MEDIUMTEXT NOT NULL,
  `SMO` VARCHAR(1000) NOT NULL,
  `Timestamp` TIMESTAMP(7) NULL,
  PRIMARY KEY (`ID`),
  UNIQUE INDEX `ID_UNIQUE` (`ID` ASC));
";
            return stmt;
        }
    }
}
