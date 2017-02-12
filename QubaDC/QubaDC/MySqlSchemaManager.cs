using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.DatabaseObjects;

namespace QubaDC
{
    public class MySqlSchemaManager : SchemaManager
    {
        private MySQLDataConnection Connection;

        public MySqlSchemaManager(MySQLDataConnection con)
        {
            this.Connection = con;
        }

        public override string GetCreateSchemaStatement()
        {
            String stmt =
@"CREATE TABLE `" + this.Connection.DataBase + @"`.`qubadcsmotable` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `Schema` MEDIUMTEXT NOT NULL,
  `SMO` VARCHAR(1000) NOT NULL,
  `Timestamp` TIMESTAMP(6) NULL,
  PRIMARY KEY (`ID`),
  UNIQUE INDEX `ID_UNIQUE` (`ID` ASC));
";
            return stmt;
        }

        public override Schema GetCurrentSchema()
        {
            throw new NotImplementedException();
        }

        public override void StoreSchema(Schema schema)
        {
            throw new NotImplementedException();
        }
    }
}
