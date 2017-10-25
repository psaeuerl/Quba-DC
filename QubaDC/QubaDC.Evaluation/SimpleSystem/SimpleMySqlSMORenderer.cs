using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SimpleSystem
{
    public class SimpleMySqlSMORenderer : SMORenderer
    {
        public override string RenderCreateInsertTrigger(TableSchema createTable, TableSchema ctHistTable)
        {
            String format =
        @"
DELIMITER $$
CREATE TRIGGER {8}.insert_trigger_{0}
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
RenderColumnDefinition(IncludeAdditionalInformation, x))
            .ToArray();
            String PrimaryKey = "";
            if (ct.PrimaryKey != null && ct.PrimaryKey.Length > 0)
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

        private static string RenderColumnDefinition(bool IncludeAdditionalInformation, ColumnDefinition x)
        {
            return '`' + x.ColumName + "` "
                            + x.DataType + " "
                            + (x.Nullable ? "NULL" : "NOT NULL") + " "
                            + (IncludeAdditionalInformation ? x.AdditionalInformation : "");
        }

        public override string RenderCreateDeleteTrigger(TableSchema createTable, TableSchema ctHistTable)
        {
            String format =
                @"
DELIMITER $$
CREATE TRIGGER {0}.delete_trigger_{1}
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

        public override string RenderCreateUpdateTrigger(TableSchema createTable, TableSchema ctHistTable)
        {
            String format =
        @"
DELIMITER $$
CREATE TRIGGER {0}.update_trigger_{1}
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
              .Select(x => String.Format("{0} <=> OLD.{0}", x));
            var where = String.Join(" AND" + System.Environment.NewLine, columns);
            return where;
        }


        private String GetQuotedTable(TableSchema ctHistTable)
        {
            return GetQuotedTable(ctHistTable.Schema, ctHistTable.Name);
        }



        public override string RenderRenameTable(RenameTable renameTable)
        {
            String baseRename = "RENAME TABLE {0} TO {1}";
            String oldName = GetQuotedTable(renameTable.OldSchema, renameTable.OldTableName);
            String newName = GetQuotedTable(renameTable.NewSchema, renameTable.NewTableName);
            String result = String.Format(baseRename, oldName, newName);
            return result;
        }

        public override string RenderDropTable(String Schema, String Table)
        {
            return "DROP TABLE " + GetQuotedTable(Schema, Table);
        }

        public override string RenderCopyTable(String schema,String tablename,String newschema, String newname)
        {
            String baseFormat = "CREATE TABLE {0} LIKE {1}; ";
            String oldTable = GetQuotedTable(schema,tablename);
            String newTable = GetQuotedTable(newschema, newname);
            String result = String.Format(baseFormat, newTable, oldTable);
            return result;
        }

        public override string RenderInsertFromOneTableToOther(TableSchema table, TableSchema copiedTableSchema, Restriction rc, string[] columns, string[] insertcolumns = null, string[] literals = null)
        {
            String baseFormat = "INSERT {0} SELECT {3} FROM {1} {2};";
            String columnString = "*";
            if (columns != null || literals != null)
            {
                IEnumerable<String> parts = new String[] { };
                if (columns != null)
                    parts = parts.Concat(columns.Select(x => Quote(x)));
                if (literals != null)
                    parts = parts.Concat(literals);
                columnString = String.Join(", ", parts);
            }
            String oldTable = GetQuotedTable(table);
            String target = GetQuotedTable(copiedTableSchema);

            String restriction =  this.CRUDRenderer.RenderRestriction(rc);
            if (!String.IsNullOrWhiteSpace(restriction))
                restriction = "WHERE " + restriction;

            String result = String.Format(baseFormat, target, oldTable,restriction, columnString);
            return result;
        }

        public override string RenderDropColumns(string schema, string name, string[] columns)
        {
            String dropcolumns = String.Join("," + System.Environment.NewLine, columns.Select(x => "DROP COLUMN " + Quote(x)));
            String table = GetQuotedTable(schema, name);
            String Drop = String.Format("ALTER TABLE {0} {1}", table, dropcolumns);
            return Drop;
        }

        public override string RenderCopyTable(string schema, string name, string select)
        {
            String baseFormat = "CREATE TABLE {0} AS {1}; ";
            String newTable = GetQuotedTable(schema, name);
            String result = String.Format(baseFormat, newTable, select);
            return result;
        }

        public override string RenderInsertToTableFromSelect(TableSchema joinedTableSchema, string select)
        {
            String baseFormat = "INSERT {0} ({2}) {1};";
            
            String target = GetQuotedTable(joinedTableSchema);
            String columns = String.Join(", ", joinedTableSchema.Columns.Select(x => this.CRUDRenderer.Quote(x)));

            String result = String.Format(baseFormat, target, select,columns);
            return result;
        }

        public override string RenderAddColumn(TableSchema copiedTableSchema, ColumnDefinition column)
        {
            String dropcolumns ="ADD  " + RenderColumnDefinition( true,column);
            String table = GetQuotedTable(copiedTableSchema.Schema, copiedTableSchema.Name);
            String Drop = String.Format("ALTER TABLE {0} {1}", table, dropcolumns);
            return Drop;
        }

        public override string RenderDropInsertTrigger(TableSchema copiedTableSchema, TableSchema ctHistTable)
        {
            String baseFormat = "Drop TRIGGER {2}.insert_trigger_{0}";
            String result = String.Format(baseFormat, copiedTableSchema.Name, ctHistTable.Name, Quote(ctHistTable.Schema));
            return result;
        }

        public override string RenderDropUpdaterigger(TableSchema copiedTableSchema, TableSchema ctHistTable)
        {
            String baseFormat = "Drop TRIGGER {2}.update_trigger_{0}"; 
             String result = String.Format(baseFormat, copiedTableSchema.Name, ctHistTable.Name, Quote(ctHistTable.Schema));
            return result;
        }

        public override string RenderDropDeleteTrigger(TableSchema copiedTableSchema, TableSchema ctHistTable)
        {
            String baseFormat = "Drop TRIGGER {2}.delete_trigger_{0}";
            String result = String.Format(baseFormat, copiedTableSchema.Name, ctHistTable.Name, Quote(ctHistTable.Schema));
            return result;
        }

        public override string RenderRenameColumn(RenameColumn renameColumn, ColumnDefinition cd, TableSchema schema)
        {
            String baseFormat = "ALTER TABLE {0} CHANGE {1} {2};";
            String table = GetQuotedTable(schema.Schema, schema.Name);
            String baseColumn = Quote(renameColumn.ColumnName);
            String type = RenderColumnDefinition(true, cd);
            String result = String.Format(baseFormat, table, baseColumn,  type);
            return result;
        }
    }
}
