using QubaDC.CRUD;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySqlSMORenderer : SMORenderer
    {
        public override string RenderCreateInsertTrigger(CreateTable createTable, CreateTable ctHistTable)
        {
            String format =
        @"
DELIMITER $$
CREATE TRIGGER {8}.{0}_to{1}_insert
AFTER INSERT
ON {2}
FOR EACH ROW
BEGIN

    INSERT INTO {3}
    ({4})
    VALUES
    (
    {5},
        NOW(3),
        null
    );

    INSERT INTO {6}
    (`Timestamp`, `Operation`)
    VALUES
    (
      NOW(3),
      CONCAT('Insert on table: ','{7}')
    );
END $$
DELIMITER;";
            //0 => source_table_name
            //1 => targettable
            //2 => source_table_identifier
            //3 => targettable_identifier
            //4 => targettable columns
            //5 => NEW.`column`from sourcetabl
            String trigger = String.Format(format
                , createTable.TableName
                , ctHistTable.TableName
                , GetQuotedTable(createTable)
                , GetQuotedTable(ctHistTable)
                , GetQuotedColumns(null, ctHistTable.Columns)
                , GetQuotedColumns("NEW", createTable.Columns)
                , GetQuotedTable(ctHistTable.Schema, QubaDCSystem.GlobalUpdateTableName)
                , GetQuotedTable(createTable)
                , Quote(ctHistTable.Schema)
                );
            return trigger;
        }


        private String GetQuotedColumns(string ColumnIdentifier, ColumnDefinition[] columns)
        {
            String prefix = ColumnIdentifier == null ? "" : ColumnIdentifier + ".";
            var quoatedColumns = columns.Select(x => prefix + GetQuoatedColumns(x)).ToArray();
            String result = String.Join("," + System.Environment.NewLine, quoatedColumns);
            return result;
        }

        private string GetQuoatedColumns(ColumnDefinition x)
        {
            return Quote(x.ColumName);
        }

        private String Quote(String x)
        {
            return String.Format("`{0}`", x);
        }

        private object GetQuotedTable(CreateTable createTable)
        {
            return GetQuotedTable(createTable.Schema,createTable.TableName);
        }

        private  object GetQuotedTable(String schema, String name )
        {
            return String.Format("`{0}`.`{1}`", schema, name);
        }

        //CREATE TABLE `development`.`testtable` (
        //  `idTestTable` INT NOT NULL,
        // PRIMARY KEY(`idTestTable`));


        public override string RenderCreateTable(CreateTable ct, Boolean IncludeAdditionalInformation = true)
        {
            String stmt =
@"CREATE TABLE `{0}`.`{1}` (
{2}
{3}
);
";
            String[] columnDefinitions = ct.Columns.Select(x =>
                '`' + x.ColumName + "` "
                + x.DataType + " "
                + (x.Nullable ? "NULL" : "NOT NULL") + " "
                + (IncludeAdditionalInformation ? x.AdditionalInformation : ""))
            .ToArray();
            String PrimaryKey = "";
            if(ct.PrimaryKey!=null)
            {
                String PKFormat = ", PRIMARY KEY({0})";
                var cols = ct.PrimaryKey.Select(x => "`" + x + "`");
                var pkToFormat = String.Join(",",cols);
                PrimaryKey = String.Format(PKFormat, pkToFormat);                    
            }
            String columns = String.Join(", " + System.Environment.NewLine, columnDefinitions);
         
            String result = String.Format(stmt, ct.Schema, ct.TableName, String.Join(","+System.Environment.NewLine, columns),PrimaryKey);
            return result;
        }
    }
}
