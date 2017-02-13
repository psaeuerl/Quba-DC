using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.DatabaseObjects;
using System.Data;
using QubaDC.Utility;

namespace QubaDC
{
    public class MySqlSchemaManager : SchemaManager
    {
        private MySQLDataConnection Connection { get; set; }

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
            String QueryFormat =
@"SELECT 
	`ID`
    ,`Schema`
    ,`SMO`
    ,`Timestamp` 
FROM `{0}`.qubadcsmotable
ORDER BY id DESC LIMIT 0, 1";
            String Query = String.Format(QueryFormat, this.Connection.DataBase);
            DataTable t =  this.Connection.ExecuteQuery(Query);
            Guard.StateEqual(1, t.Rows.Count);
            DataRow row = t.Select().First();
            return new Schema()
            {


            };            
        }

        public override void StoreSchema(Schema schema)
        {
            throw new NotImplementedException();
        }
    }
}
