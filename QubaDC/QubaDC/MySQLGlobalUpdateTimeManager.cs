using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySQLGlobalUpdateTimeManager : TableLastUpdateManager
    {
        private MySQLDataConnection Connection { get; set; }

        public MySQLGlobalUpdateTimeManager(MySQLDataConnection con)
        {
            this.Connection = con;

        }
        public override string GetCreateUpdateTimeTableStatement()
        {
            throw new InvalidOperationException(" Will be removed");
            String stmt =
             @"CREATE TABLE " + GetTableName()
+ @" (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `Timestamp` DATETIME(6) NOT NULL,
  `Operation` VARCHAR(1000) NOT NULL,
  PRIMARY KEY (`ID`));
";
            return stmt;
        }

        public override string GetTableName()
        {

           return "`" + Connection.DataBase + @"`.`" + QubaDCSystem.GlobalUpdateTableName + @"`";
        }
        public override QubaDC.CRUD.Table GetTable()
        {
            return new CRUD.Table()
            {
                TableName = QubaDCSystem.GlobalUpdateTableName,
                TableSchema = Connection.DataBase
            };
        }

        public override TableLastUpdate GetLatestUpdate()
        {

            String stmt = "Select ID,Timestamp,Operation FROM " + GetTableName()
                        + "WHERE Timestamp = (Select MAX(Timestamp) FROM " + GetTableName() + ")";
            DataTable t = this.Connection.ExecuteQuery(stmt);
            var rows = t.Select();
            //Rows.Lenght 0 should not happen as table is created with empty-schema in the beginning
            if (rows.Length == 0)
                throw new InvalidOperationException("Got no latestUpdate");
            //More than one row is acutally not a problem
            //a.) very unlikely to occur if not outright impossible due to transaction management both accessing the same table
            //b.) latestupdate is used to determine against which schema to execute queries, nothing more, therefor not dead critical
            var row = rows[0];
            return new TableLastUpdate()
            {
                Operation = row.Field<String>("Operation"),
                ID = row.Field<Int32>("ID"),
                DateTime = row.Field<DateTime>("Timestamp")

            };
        }
    }
}
