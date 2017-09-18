using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;

namespace QubaDC
{
    public class MySQLTableMetadataManager : TableMetadataManager
    {
        private MySQLDataConnection Connection { get; set; }

        public MySQLTableMetadataManager(MySQLDataConnection con)
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
            throw new InvalidOperationException("GetLatestUpdate is obsolete");
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

        internal override string GetCreateMetaTableFor(string schema, string tableName)
        {
            Table t = GetMetaTableFor(schema, tableName); 
            String ct = @" CREATE TABLE `{0}`.`{1}` (
  `lastUpdate` datetime(3) NOT NULL,
  `canBeQueried` BOOL NOT NULL
);";
            String result = String.Format(ct, t.TableSchema, t.TableName);
            return result;            
        }

        internal override Table GetMetaTableFor(string schema, string tableName)
        {
            return new Table()
            {
                TableName = tableName + "_metadata",
                TableSchema = schema
            };
        }

        internal override string GetStartInsertFor(string schema, string tableName)
        {
            Table t = GetMetaTableFor(schema, tableName);
            String baseStmt = @"INSERT INTO `{0}`.`{1}`
(`lastUpdate`,
`canBeQueried`)
VALUES
(@updateTime,
true);";
            String result = String.Format(baseStmt, t.TableSchema, t.TableName);
            return result;
        }

        public override DateTime GetLatestUpdate(params Table[] tables)
        {
            string stmt = "SELECT MAX(lastUpdate) FROM {0}";
            string tbl = "`{0}`.`{1}`";
            String tableSQL = String.Join(", ", tables.Select(x => String.Format(tbl, x.TableSchema, x.TableName+"_metadata")));
            String result = String.Format(stmt, tableSQL);
            DataTable res = this.Connection.ExecuteQuery(result);
            DateTime resDatetime=  res.Select().First().Field<DateTime>(0);
            return resDatetime;
        }

        internal override bool GetCanBeQueriedFor(Table changingTable, DbConnection con)
        {
            string stmt = "SELECT canBeQueried FROM {0}";
            String part = String.Format("`{0}`.`{1}`", changingTable.TableSchema, changingTable.TableName + "_metadata");
            String result = String.Format(stmt, part);
            DataTable res = this.Connection.ExecuteQuery(result, con);
            Boolean resDatetime = res.Select().First().Field<Boolean>(0);
            return resDatetime;
        }

        internal override string GetSetLastUpdateStatement(Table insertTable, string v)
        {
            Table t = GetMetaTableFor(insertTable);
            String query = "UPDATE `{0}`.`{1}` SET lastUpdate = {2};";
            String resQuery = String.Format(query, t.TableSchema, t.TableName, v);
            return resQuery;
        }

        private Table GetMetaTableFor(Table insertTable)
        {
            return GetMetaTableFor(insertTable.TableSchema, insertTable.TableName);
        }
    }
}
