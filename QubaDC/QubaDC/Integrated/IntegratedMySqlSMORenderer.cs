using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated
{
    public class IntegratedMySqlSMORenderer : SMORenderer
    {


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

        private object GetQuotedColumns(String ColumnIdentifier, string[] columns)
        {
            String prefix = ColumnIdentifier == null ? "" : ColumnIdentifier + ".";
            var quoatedColumns = columns.Select(x => prefix + Quote(x)).ToArray();
            String result = String.Join("," + System.Environment.NewLine, quoatedColumns);
            return result;
        }

        private object GetColumnWherePart(string[] baseColumns, string compPrefix ="OLD")
        {
            var columns = baseColumns.Select(x => Quote(x))
              .Select(x => String.Format("{0} <=> {1}.{0}", x, compPrefix));
            var where = String.Join(" AND" + System.Environment.NewLine, columns);
            return where;
        }


        private String GetQuotedTable(TableSchema ctHistTable)
        {
            return GetQuotedTable(ctHistTable.Schema, ctHistTable.Name);
        }



        //        internal override string RenderRenameTable(RenameTable renameTable)
        //        {
        //            String baseRename = "RENAME TABLE {0} TO {1}";
        //            String oldName = GetQuotedTable(renameTable.OldSchema, renameTable.OldTableName);
        //            String newName = GetQuotedTable(renameTable.NewSchema, renameTable.NewTableName);
        //            String result = String.Format(baseRename, oldName, newName);
        //            return result;
        //        }

        //        internal override string RenderDropTable(String Schema, String Table)
        //        {
        //            return "DROP TABLE " + GetQuotedTable(Schema, Table);
        //        }

        //        internal override string RenderCopyTable(String schema,String tablename,String newschema, String newname)
        //        {
        //            String baseFormat = "CREATE TABLE {0} LIKE {1}; ";
        //            String oldTable = GetQuotedTable(schema,tablename);
        //            String newTable = GetQuotedTable(newschema, newname);
        //            String result = String.Format(baseFormat, newTable, oldTable);
        //            return result;
        //        }

        //        internal override string RenderInsertFromOneTableToOther(TableSchema table, TableSchema copiedTableSchema, Restriction rc, string[] columns, string[] insertcolumns = null)
        //        {
        //            String baseFormat = "INSERT {0} {4} SELECT {3} FROM {1} {2};";
        //            String columnString = "*";
        //            if (columns != null)
        //                columnString = String.Join(",", columns.Select(x => Quote(x)));
        //            String oldTable = GetQuotedTable(table);
        //            String target = GetQuotedTable(copiedTableSchema);

        //            String restriction =  this.CRUDRenderer.RenderRestriction(rc);
        //            if (!String.IsNullOrWhiteSpace(restriction))
        //                restriction = "WHERE " + restriction;

        //            String insColumn = "";
        //            if(insertcolumns != null)
        //            {
        //                insColumn = "(" + String.Join(", ", insertcolumns.Select(x => this.Quote(x))) + ")";
        //            }
        //            String result = String.Format(baseFormat, target, oldTable,restriction, columnString, insColumn);
        //            return result;
        //        }

        //        internal override string RenderDropColumns(string schema, string name, string[] columns)
        //        {
        //            String dropcolumns = String.Join("," + System.Environment.NewLine, columns.Select(x => "DROP COLUMN " + Quote(x)));
        //            String table = GetQuotedTable(schema, name);
        //            String Drop = String.Format("ALTER TABLE {0} {1}", table, dropcolumns);
        //            return Drop;
        //        }

        //        internal override string RenderCopyTable(string schema, string name, string select)
        //        {
        //            String baseFormat = "CREATE TABLE {0} AS {1}; ";
        //            String newTable = GetQuotedTable(schema, name);
        //            String result = String.Format(baseFormat, newTable, select);
        //            return result;
        //        }

        //        internal override string RenderInsertToTableFromSelect(TableSchema joinedTableSchema, string select)
        //        {
        //            String baseFormat = "INSERT INTO {0} ({2}) {1};";

        //            String columns = String.Join(", ",joinedTableSchema.Columns.Select(x => this.CRUDRenderer.Quote(x)));
        //            String target = GetQuotedTable(joinedTableSchema);


        //            String result = String.Format(baseFormat, target, select, columns);
        //            return result;
        //        }

        //        internal override string RenderAddColumn(TableSchema copiedTableSchema, ColumnDefinition column)
        //        {
        //            String dropcolumns ="ADD  " + RenderColumnDefinition( true,column);
        //            String table = GetQuotedTable(copiedTableSchema.Schema, copiedTableSchema.Name);
        //            String Drop = String.Format("ALTER TABLE {0} {1}", table, dropcolumns);
        //            return Drop;
        //        }

        //        internal override string RenderDropInsertTrigger(TableSchema copiedTableSchema, TableSchema ctHistTable)
        //        {
        //            String baseFormat = "Drop TRIGGER {1}.insert_on_{0}";
        //            String result = String.Format(baseFormat, copiedTableSchema.Name, Quote(ctHistTable.Schema));
        //            return result;
        //        }

        //        internal override string RenderDropUpdaterigger(TableSchema copiedTableSchema, TableSchema ctHistTable)
        //        {
        //            String baseFormat = "Drop TRIGGER {1}.update_on_{0}"; 
        //             String result = String.Format(baseFormat, copiedTableSchema.Name, Quote(ctHistTable.Schema));
        //            return result;
        //        }

        //        internal override string RenderDropDeleteTrigger(TableSchema copiedTableSchema, TableSchema ctHistTable)
        //        {
        //            String baseFormat = "Drop TRIGGER {1}.delete_on_{0}";
        //            String result = String.Format(baseFormat, copiedTableSchema.Name, Quote(ctHistTable.Schema));
        //            return result;
        //        }

        //        internal override string RenderRenameColumn(RenameColumn renameColumn, ColumnDefinition cd, TableSchema schema)
        //        {
        //            String baseFormat = "ALTER TABLE {0} CHANGE {1} {2};";
        //            String table = GetQuotedTable(schema.Schema, schema.Name);
        //            String baseColumn = Quote(renameColumn.ColumnName);
        //            String type = RenderColumnDefinition(true, cd);
        //            String result = String.Format(baseFormat, table, baseColumn,  type);
        //            return result;
        //        }
        public override string RenderCreateInsertTrigger(TableSchema createTable, TableSchema ctHistTable)
        {
            String format =
                @"
        DELIMITER $$
        CREATE TRIGGER {3}.insert_on_{0}
        BEFORE INSERT
        ON {1}
        FOR EACH ROW
        BEGIN
                SET NEW.{2} = NOW(3);
                SET NEW.{5} = NULL;

                INSERT INTO {4}
                (`Timestamp`, `Operation`)
                VALUES
                (
                  NOW(3),
                  CONCAT('Insert on table: ','{0}')
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
                , GetQuotedTable(createTable)
                , Quote(IntegratedConstants.StartTS)
                , Quote(ctHistTable.Schema)
                , GetQuotedTable(ctHistTable.Schema, QubaDCSystem.GlobalUpdateTableName)
                , Quote(IntegratedConstants.EndTS)
                );
            return trigger;
        }

        public override string RenderCreateTable(CreateTable ct, bool RemoveAdditionalColumnInfos = false)
        {
            String stmt =
@"CREATE TABLE `{0}`.`{1}` (
            {2}
            {3}
            );
            ";
            String[] columnDefinitions = ct.Columns.Select(x =>
                RenderColumnDefinition(RemoveAdditionalColumnInfos, x))
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

        internal override string RenderAddColumn(TableSchema copiedTableSchema, ColumnDefinition column)
        {
            String dropcolumns = "ADD  " + RenderColumnDefinition(true, column);
            String table = GetQuotedTable(copiedTableSchema.Schema, copiedTableSchema.Name);
            String Drop = String.Format("ALTER TABLE {0} {1}", table, dropcolumns);
            return Drop;
        }

        internal override string RenderCopyTable(string schema, string name, string select)
        {
            String baseFormat = "CREATE TABLE {0} AS {1}; ";
            String newTable = GetQuotedTable(schema, name);
            String result = String.Format(baseFormat, newTable, select);
            return result;
        }

        internal override string RenderCopyTable(string schema, string tablename, string newschema, string newname)
        {
            String baseFormat = "CREATE TABLE {0} LIKE {1}; ";
            String oldTable = GetQuotedTable(schema, tablename);
            String newTable = GetQuotedTable(newschema, newname);
            String result = String.Format(baseFormat, newTable, oldTable);
            return result;
        }

        internal override string RenderCreateDeleteTrigger(TableSchema createTable, TableSchema ctHistTable)
        {
            throw new NotImplementedException("Cannot use Delete Trigger as we cannot update the table that is responsible for firing the trigger");
            //            String format =
            //                                   @"
            //DELIMITER $$
            //CREATE TRIGGER {0}.delete_trigger_{1}
            //AFTER DELETE
            //ON {2}
            //FOR EACH ROW
            //BEGIN
            //    INSERT INTO {2}
            //    ({3})
            //    VALUES
            //    (
            //    {4},
            //    {5},
            //       NOW(3)
            //    );

            //  INSERT INTO {6}
            //    (`Timestamp`, `Operation`)
            //    VALUES
            //    (
            //      NOW(3),
            //      CONCAT('Delete on table: ','{1}')
            //    );

            //END $$
            //DELIMITER;";
            //            //0 => source_table_name
            //            //1 => targettable
            //            //2 => source_table_identifier
            //            //3 => targettable_identifier
            //            //4 => targettable columns
            //            //5 => NEW.`column`from sourcetabl
            //            String trigger = String.Format(format
            //                , Quote(ctHistTable.Schema)
            //                , createTable.Name
            //                , GetQuotedTable(createTable)
            //                , GetQuotedColumns(null,createTable.Columns.Union( IntegratedConstants.GetHistoryTableColumns().Select(x=>x.ColumName)).ToArray())
            //                , GetQuotedColumns("OLD", createTable.Columns)
            //                , GetQuotedColumns("OLD", new String[] { IntegratedConstants.StartTS })
            //                , GetQuotedTable(ctHistTable.Schema, QubaDCSystem.GlobalUpdateTableName)
            //               // , ctHistTable.Name
            //               // , GetQuotedTable(ctHistTable)
            //               //, GetColumnWherePart(createTable.Columns)
            //               //, GetQuotedTable(createTable)
            //               //, GetQuotedTable(ctHistTable.Schema, QubaDCSystem.GlobalUpdateTableName)

            //               );

            //            return trigger;
        }

        internal override string RenderCreateUpdateTrigger(TableSchema createTable, TableSchema ctHistTable)
        {

            //  throw new NotImplementedException("will be handeld via insert+update");
            String format =
                   @"
DELIMITER $$
CREATE TRIGGER {0}.update_trigger_{1}
AFTER UPDATE
ON {2}
FOR EACH ROW
BEGIN
    INSERT INTO {6}
    (`Timestamp`, `Operation`)
    VALUES
    (
      NOW(3),
      CONCAT('Update->Delete on table: ','{2}')
    );
END $$
DELIMITER;";


            String trigger = String.Format(format
                , Quote(createTable.Schema)
                , createTable.Name
                , GetQuotedTable(createTable)
                , GetColumnWherePart(createTable.Columns.Union(IntegratedConstants.GetHistoryTableColumns().Select(x => x.ColumName)).ToArray(), "NEW")
                , GetQuotedColumns(null, createTable.Columns.Union(IntegratedConstants.GetHistoryTableColumns().Select(x => x.ColumName)).ToArray())
                , GetQuotedColumns("OLD", createTable.Columns)
                , GetQuotedTable(ctHistTable.Schema, QubaDCSystem.GlobalUpdateTableName)
                , GetQuotedColumns("OLD", new String[] { IntegratedConstants.StartTS })
               //, ctHistTable.Name
               //, GetQuotedTable(ctHistTable)
               //, GetColumnWherePart(createTable.Columns)
               //, GetQuotedTable(createTable)
               //, GetQuotedTable(ctHistTable.Schema, QubaDCSystem.GlobalUpdateTableName)
               //                , GetQuotedTable(createTable)
               //, GetQuotedColumns(null, ctHistTable.Columns)
               //, GetQuotedColumns("NEW", createTable.Columns)

               );
            return trigger;

        }

        internal override string RenderDropColumns(string schema, string name, string[] columns)
        {
            throw new NotImplementedException();
        }

        internal override string RenderDropDeleteTrigger(TableSchema copiedTableSchema, TableSchema ctHistTable)
        {
            throw new NotImplementedException();
        }

        internal override string RenderDropInsertTrigger(TableSchema copiedTableSchema, TableSchema ctHistTable)
        {
            throw new NotImplementedException();
        }

        internal override string RenderDropTable(string Schema, string Table)
        {
            return "DROP TABLE " + GetQuotedTable(Schema, Table);
        }

        internal override string RenderDropUpdaterigger(TableSchema copiedTableSchema, TableSchema ctHistTable)
        {
            throw new NotImplementedException();
        }

        internal override string RenderInsertFromOneTableToOther(TableSchema table, TableSchema copiedTableSchema, Restriction rc, string[] columns, string[] insertcolumns = null, string[] literals = null)
        {
            String baseFormat = "INSERT {0} SELECT {3} FROM {1} {2};";
            String columnString = "*";
            if (columns != null || literals != null)
            {
                IEnumerable<String> parts = new String[] { };
                if (columns != null)
                    parts = parts.Concat(columns.Select(x => Quote(x)));
                if(literals != null)
                    parts = parts.Concat(literals);
                columnString = String.Join(", ", parts);
            }
            String oldTable = GetQuotedTable(table);
            String target = GetQuotedTable(copiedTableSchema);

            String restriction = this.CRUDRenderer.RenderRestriction(rc);
            if (!String.IsNullOrWhiteSpace(restriction))
                restriction = "WHERE " + restriction;

            String result = String.Format(baseFormat, target, oldTable, restriction, columnString);
            return result;
        }

        internal override string RenderInsertToTableFromSelect(TableSchema joinedTableSchema, string select)
        {
            String baseFormat = "INSERT {0} ({2}) {1};";

            String target = GetQuotedTable(joinedTableSchema);
            String columns = String.Join(", ", joinedTableSchema.Columns.Select(x => this.CRUDRenderer.Quote(x)));

            String result = String.Format(baseFormat, target, select, columns);
            return result;
        }

        internal override string RenderRenameColumn(RenameColumn renameColumn, ColumnDefinition cd, TableSchema schema)
        {
            String baseFormat = "ALTER TABLE {0} CHANGE {1} {2};";
            String table = GetQuotedTable(schema.Schema, schema.Name);
            String baseColumn = Quote(renameColumn.ColumnName);
            String type = RenderColumnDefinition(true, cd);
            String result = String.Format(baseFormat, table, baseColumn, type);
            return result;
        }

        internal override string RenderRenameTable(RenameTable renameTable)
        {
            String baseRename = "RENAME TABLE {0} TO {1}";
            String oldName = GetQuotedTable(renameTable.OldSchema, renameTable.OldTableName);
            String newName = GetQuotedTable(renameTable.NewSchema, renameTable.NewTableName);
            String result = String.Format(baseRename, oldName, newName);
            return result;
        }
    }
}
