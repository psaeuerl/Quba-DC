﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.DatabaseObjects;
using System.Data;
using QubaDC.Utility;
using QubaDC.SMO;
using System.Data.Common;

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
  `Timestamp` DATETIME(6) NULL,
  PRIMARY KEY (`ID`),
  UNIQUE INDEX `ID_UNIQUE` (`ID` ASC));
";
            return stmt;
        }


        public override string GetInsertSchemaStatement(Schema schema,SchemaModificationOperator smo, bool useUpdateVariable = false)
        {
            string nowOrVariable = useUpdateVariable ? "@updateTime" : "NOW(3)";
            String InsertFormat =
             @"INSERT INTO `{0}`.`qubadcsmotable`
(`Schema`,
`SMO`,
`Timestamp`)
VALUES(
'{1}',
'{2}',
{3}
);";
            String argDb = this.Connection.DataBase;
            String argSchema = JsonSerializer.SerializeObject(schema);
            String argSMO = JsonSerializer.SerializeObject(smo).Replace("'", "\\'");
            //TODO => Use Parameterized Query
            String result = String.Format(InsertFormat, argDb, argSchema, argSMO, nowOrVariable);
            return result;
        }



        private SchemaInfo ExecuteGetCurrentSchema(Func<String,DataTable> ExecuteQuery, Action<String> log)
        {
            String QueryFormat =  GetSelectFormat() + " LIMIT 0, 1";
            String Query = String.Format(QueryFormat, this.Connection.DataBase);
            log(Query);
            DataTable t = ExecuteQuery(Query);
            Guard.StateEqual(true, t.Rows.Count <= 1);
            if (t.Rows.Count == 0)
            {
                return new SchemaInfo();
            }
            DataRow row = t.Select().First();

            return RowToSchemainfo(row);
        }

        private static SchemaInfo RowToSchemainfo(DataRow row)
        {
            return new SchemaInfo()
            {

                ID = row.Field<int>("ID"),
                Schema = JsonSerializer.DeserializeObject<Schema>(row.Field<String>("Schema")),
                SMO = JsonSerializer.DeserializeObject<SchemaModificationOperator>(row.Field<String>("SMO")),
                TimeOfCreation = row.Field<DateTime>("Timestamp")
            };
        }

        public override SchemaInfo GetCurrentSchema()
        {
            return ExecuteGetCurrentSchema(x => this.Connection.ExecuteQuery(x), (x)=> {; });          
        }

        public override SchemaInfo[] GetAllSchemataOrderdByIdDescending()
        {
            string QueryFormat = GetSelectFormat();
            String Query = String.Format(QueryFormat, this.Connection.DataBase);
            DataTable t = this.Connection.ExecuteQuery(Query);
            var result = t.Select().Select(x => RowToSchemainfo(x)).ToArray();
            return result;
        }

        private static string GetSelectFormat()
        {
            return @"SELECT 
	`ID`
    ,`Schema`
    ,`SMO`
    ,`Timestamp` 
FROM `{0}`.qubadcsmotable
ORDER BY id DESC";
        }

        public override string GetInsertToGlobalUpdateTrigger()
        {
            throw new InvalidOperationException("Not needed => will be removed");
            String result = String.Format(@"DELIMITER $$
CREATE TRIGGER `{0}`.qubadcsmotable_to_global_timestamp
AFTER INSERT
ON `{0}`.`{1}`
FOR EACH ROW
BEGIN

    INSERT INTO `{0}`.`{2}`
    (
`Timestamp`,
`Operation`
)
    VALUES
    (
        NOW(3),
        CONCAT('Schemaupdate: ', NEW.ID)
    );
END $$
DELIMITER;", this.Connection.DataBase,QubaDCSystem.QubaDCSMOTable,QubaDCSystem.GlobalUpdateTableName);
            return result;
        }

        public override SchemaInfo GetSchemaActiveAt(DateTime dateTime)
        {

            String select = 
@"SELECT * FROM `{0}`.`{1}`
where timestamp = (

Select MAX(timestamp) FROM `{0}`.`{1}`
WHERE timestamp <= {2}
)";
            String dtLiteral = MySQLDialectHelper.RenderDateTime(dateTime);
            String Query = String.Format(select, this.Connection.DataBase, QubaDCSystem.QubaDCSMOTable, dtLiteral);
            DataTable t = this.Connection.ExecuteQuery(Query);
            if (t.Rows.Count == 0)
            {
                return new SchemaInfo();
            }
            if (t.Rows.Count > 1)
                throw new InvalidOperationException("GetSchemaAt returned more than one row");
            DataRow row = t.Select().First();

            return RowToSchemainfo(row);
        }

        internal override string GetTableName()
        {
            return String.Format("`{0}`.`{1}`", this.Connection.DataBase, QubaDCSystem.QubaDCSMOTable);
        }

        internal override string RenderEnsureSchema(SchemaInfo xy)
        {
            String stmt = @"call {0}.ensureSMOid({1});";
            String res = String.Format(stmt, this.Connection.DataBase, xy.ID);
            return res;
        }

        internal override string GetStoredProcedureExistsStatement()
        {
            throw new InvalidOperationException("Will probably be removed");
   //         return String.Format("SELECT 1 FROM mysql.proc p WHERE db = '{0}' AND name = 'ensureSMOid'", this.Connection.DataBase);
        }

        internal override string GetCreateEnsureIDCreateProcedure()
        {
            String stmt = @"
delimiter //
create procedure {0}.ensureSMOid(IN expectedId LONG)
begin
	DECLARE EXIT HANDLER FOR SQLSTATE '42000'
		SELECT 'Invoiced barcodes may not have accounting removed.';
	SELECT MAX(ID) into @maxId FROM {0}.qubadcsmotable;
	IF (!(@maxId <=> expectedId))
	THEN
		SET @er := CONCAT('Optimistic Check failed, expected ',expectedId,' got ',@maxId);
		 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @er;
	END IF;
end;
//
delimiter ;";
            String res = String.Format(stmt, this.Connection.DataBase);
            return res;
        }

        public override SchemaInfo GetCurrentSchema(DbConnection openConnection, Action<string> log)
        {
            return ExecuteGetCurrentSchema(x => this.Connection.ExecuteQuery(x, openConnection),log);

        }
    }
}
