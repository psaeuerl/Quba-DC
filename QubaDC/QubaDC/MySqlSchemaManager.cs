using System;
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


        public override string GetInsertSchemaStatement(Schema schema,SchemaModificationOperator smo)
        {
            String InsertFormat =
             @"INSERT INTO `{0}`.`qubadcsmotable`
(`Schema`,
`SMO`,
`Timestamp`)
VALUES(
'{1}',
'{2}',
NOW(3)
);";
            String argDb = this.Connection.DataBase;
            String argSchema = JsonSerializer.SerializeObject(schema);
            String argSMO = JsonSerializer.SerializeObject(smo).Replace("'", "\\'");
            //TODO => Use Parameterized Query
            String result = String.Format(InsertFormat, argDb, argSchema, argSMO);
            return result;
        }

        public override SchemaInfo GetCurrentSchema(DbConnection openConnection)
        {
            return ExecuteGetCurrentSchema(x => this.Connection.ExecuteQuery(x,openConnection));
        }

        private SchemaInfo ExecuteGetCurrentSchema(Func<String,DataTable> ExecuteQuery)
        {
            String QueryFormat =  GetSelectFormat() + " LIMIT 0, 1";
            String Query = String.Format(QueryFormat, this.Connection.DataBase);
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
            return ExecuteGetCurrentSchema(x => this.Connection.ExecuteQuery(x));          
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
    }
}
