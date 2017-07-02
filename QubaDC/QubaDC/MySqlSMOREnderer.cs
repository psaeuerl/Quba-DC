using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
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
        public override string RenderCreateInsertTrigger(TableSchema createTable, TableSchema ctHistTable)
        {
            String format =
        @"
DELIMITER $$
CREATE TRIGGER {8}.insert_{0}_to_{1}
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
                , createTable.Name
                , ctHistTable.Name
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

        private string GetQuoatedColumn(ColumnDefinition x)
        {
            return Quote(x.ColumName);
        }

        private String Quote(String x)
        {
            return String.Format("`{0}`", x);
        }


        private String GetQuotedTable(String schema, String name)
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
            if (ct.PrimaryKey != null)
            {
                String PKFormat = ", PRIMARY KEY({0})";
                var cols = ct.PrimaryKey.Select(x => "`" + x + "`");
                var pkToFormat = String.Join(",", cols);
                PrimaryKey = String.Format(PKFormat, pkToFormat);
            }
            String columns = String.Join(", " + System.Environment.NewLine, columnDefinitions);

            String result = String.Format(stmt, ct.Schema, ct.TableName, String.Join("," + System.Environment.NewLine, columns), PrimaryKey);
            return result;
        }

        internal override string RenderCreateDeleteTrigger(TableSchema createTable, TableSchema ctHistTable)
        {
            String format =
                @"
DELIMITER $$
CREATE TRIGGER {0}.delete_{1}_to_{2}
AFTER DELETE
ON {5}
FOR EACH ROW
BEGIN
	UPDATE {3}
    SET endts = NOW(3)
    WHERE
    {4};

  INSERT INTO {6}
    (`Timestamp`, `Operation`)
    VALUES
    (
      NOW(3),
      CONCAT('Delete on table: ','{1}')
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
                , Quote(ctHistTable.Schema)
                , createTable.Name
                , ctHistTable.Name
                , GetQuotedTable(ctHistTable)
               , GetColumnWherePart(createTable.Columns)
               , GetQuotedTable(createTable)
               , GetQuotedTable(ctHistTable.Schema, QubaDCSystem.GlobalUpdateTableName)

               );

            return trigger;
        }

        internal override string RenderCreateUpdateTrigger(TableSchema createTable, TableSchema ctHistTable)
        {
            String format =
        @"
DELIMITER $$
CREATE TRIGGER {0}.update_{1}_to_{2}
AFTER UPDATE
ON {5}
FOR EACH ROW
BEGIN

    UPDATE {3}
    SET endts = NOW(3)
    WHERE
    {4};

    INSERT INTO {3}
    ({8})
    VALUES
    (
    {9},
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


            String trigger = String.Format(format
                , Quote(ctHistTable.Schema)
                , createTable.Name
                , ctHistTable.Name
                , GetQuotedTable(ctHistTable)
                , GetColumnWherePart(createTable.Columns)
                , GetQuotedTable(createTable)
                , GetQuotedTable(ctHistTable.Schema, QubaDCSystem.GlobalUpdateTableName)
                                , GetQuotedTable(createTable)
                , GetQuotedColumns(null, ctHistTable.Columns)
                , GetQuotedColumns("NEW", createTable.Columns)

               );
            return trigger;
        }

        private object GetQuotedColumns(String ColumnIdentifier, string[] columns)
        {
            String prefix = ColumnIdentifier == null ? "" : ColumnIdentifier + ".";
            var quoatedColumns = columns.Select(x => prefix + Quote(x)).ToArray();
            String result = String.Join("," + System.Environment.NewLine, quoatedColumns);
            return result;
        }

        private object GetColumnWherePart(string[] baseColumns)
        {
            var columns = baseColumns.Select(x => Quote(x))
              .Select(x => String.Format("{0} = OLD.{0}", x));
            var where = String.Join(" AND" + System.Environment.NewLine, columns);
            return where;
        }


        private String GetQuotedTable(TableSchema ctHistTable)
        {
            return GetQuotedTable(ctHistTable.Schema, ctHistTable.Name);
        }



        internal override string RenderRenameTable(RenameTable renameTable)
        {
            String baseRename = "RENAME TABLE {0} TO {1}";
            String oldName = GetQuotedTable(renameTable.OldSchema, renameTable.OldTableName);
            String newName = GetQuotedTable(renameTable.NewSchema, renameTable.NewTableName);
            String result = String.Format(baseRename, oldName, newName);
            return result;
        }

        internal override string RenderDropTable(String Schema, String Table)
        {
            return "DROP TABLE " + GetQuotedTable(Schema, Table);
        }

        internal override string RenderCopyTable(String schema,String tablename,String newschema, String newname)
        {
            String baseFormat = "CREATE TABLE {0} LIKE {1}; ";
            String oldTable = GetQuotedTable(schema,tablename);
            String newTable = GetQuotedTable(newschema, newname);
            String result = String.Format(baseFormat, newTable, oldTable);
            return result;
        }

        internal override string RenderInsertToTableFromSelect(TableSchema table, TableSchema copiedTableSchema, Restriction rc)
        {
            String baseFormat = "INSERT {0} SELECT * FROM {1} {2};";
            String oldTable = GetQuotedTable(table);
            String target = GetQuotedTable(copiedTableSchema);

            String restriction =  this.CRUDRenderer.RenderRestriction(rc);
            if (!String.IsNullOrWhiteSpace(restriction))
                restriction = "WHERE " + restriction;

            String result = String.Format(baseFormat, target, oldTable,restriction);
            return result;
        }
    }
}
