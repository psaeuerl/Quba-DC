using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;
using QubaDC.CRUD;
using QubaDC.Restrictions;
using QubaDC.Integrated;
using QubaDC.Integrated.SMO;

namespace QubaDC.Separated.SMO
{
    class IntegratedRenameColumnHandler
    {
        private SchemaManager schemaManager;

        public IntegratedRenameColumnHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager metaManager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = metaManager;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(RenameColumn renameColumn)
        {
            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data



            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                Schema currentSchema = currentSchemaInfo.Schema;


                TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(renameColumn.Schema, renameColumn.TableName);
                TableSchema originalHistTable = currentSchemaInfo.Schema.FindHistTable(originalTable.Table.ToTable());

                var copiedTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Select(x => x == renameColumn.ColumnName ? renameColumn.RenameName : x).ToArray(),
                    Name = originalTable.Table.Name,
                    Schema = originalTable.Table.Schema,
                    ColumnDefinitions = originalTable.Table.ColumnDefinitions
                };
                copiedTableSchema.ColumnDefinitions.First(x => x.ColumName == renameColumn.ColumnName).ColumName = renameColumn.RenameName;

                var copiedHistSchema = new TableSchema()
                {
                    Columns = originalHistTable.Columns.Select(x => x == renameColumn.ColumnName ? renameColumn.RenameName : x).ToArray(),
                    Name = originalTable.Table.Name + "_" + currentSchemaInfo.ID,
                    Schema = originalTable.Table.Schema,
                    ColumnDefinitions = originalHistTable.ColumnDefinitions
                };
                copiedHistSchema.ColumnDefinitions.First(x => x.ColumName == renameColumn.ColumnName).ColumName = renameColumn.RenameName;

                //Guard.StateTrue(copiedTableSchema.Columns.Count() == originalTable.Table.Columns.Count() + 1, "Could add new column: " + dropColumn.Column);
                //Guard.StateTrue(copiedHistSchema.Columns.Count() == originalHistTable.Columns.Count() + 1, "Could add new column: " + dropColumn.Column);
                Table metaTable = this.MetaManager.GetMetaTableFor(copiedTableSchema);
                currentSchema.RemoveTable(originalTable.Table.ToTable());
                currentSchema.AddTable(copiedTableSchema, copiedHistSchema, metaTable);
                String dropOriginalHistTable = SMORenderer.RenderDropTable(originalHistTable.Schema, originalHistTable.Name);
                //con.ExecuteNonQuerySQL(dropOriginalHistTable);

                String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
                {
                    NewSchema = originalTable.HistTableSchema,
                    NewTableName = originalTable.HistTableName,
                    OldSchema = originalTable.Table.Schema,
                    OldTableName = originalTable.Table.Name
                });
                //con.ExecuteNonQuerySQL(renameTableSQL);


               String copyBaseTable =  CopyTable(originalHistTable, copiedTableSchema, true);
                String copyHistTable = CopyTable( originalHistTable, copiedHistSchema, false);


                ColumnDefinition cd = copiedTableSchema.ColumnDefinitions.First(x => x.ColumName == renameColumn.RenameName);
                String renameColumnSQL = SMORenderer.RenderRenameColumn(renameColumn, cd, copiedTableSchema);


                ColumnDefinition cdHist = copiedHistSchema.ColumnDefinitions.First(x => x.ColumName == renameColumn.RenameName);
                String renameColumnSQLHist = SMORenderer.RenderRenameColumn(renameColumn, cdHist, copiedHistSchema);


                ////Insert data from old to new
                SelectOperation s = new SelectOperation()
                {
                    Columns = originalTable.Table.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                    LiteralColumns = new LiteralColumn[] {
                        new LiteralColumn() { ColumnLiteral = "NOW(3)", ColumnName = IntegratedConstants.StartTS },
                        new LiteralColumn() { ColumnLiteral = "NULL", ColumnName = IntegratedConstants.EndTS }},
                    FromTable = new FromTable() { TableAlias = "t1", TableName = originalHistTable.Name, TableSchema = originalTable.Table.Schema }
                };
                String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);

                TableSchema isnertWithStartts = new TableSchema()
                {
                    Columns = copiedTableSchema.Columns.Concat(new String[] { IntegratedConstants.StartTS, IntegratedConstants.EndTS }).ToArray(),
                    Name = copiedTableSchema.Name,
                    Schema = copiedTableSchema.Schema
                };
                String insertFromTable = SMORenderer.RenderInsertToTableFromSelect(isnertWithStartts, select);
    //            con.ExecuteNonQuerySQL(insertFromTable, c);

                //String updateSchema = this.schemaManager.GetInsertSchemaStatement(currentSchema, renameColumn);
                //con.ExecuteNonQuerySQL(updateSchema, c);
                //this.schemaManager.StoreSchema(currentSchema, renameColumn, con, c);


                String[] Statements = new String[]
                {
                    dropOriginalHistTable,
                    renameTableSQL,
                    copyBaseTable,
                    copyHistTable,
                    renameColumnSQL,
                    renameColumnSQLHist,
                    insertFromTable,
                    //createHistTable,
                    //createMetaTable,
                    //baseInsert
                };

                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements
                };
            };


            IntegratedSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 renameColumn,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s));

        }

        private void AlterTable(System.Data.Common.DbConnection c, MySQLDataConnection con, TableSchema copiedTableSchema, ColumnDefinition column)
        {
            String statement = this.SMORenderer.RenderAddColumn(copiedTableSchema, column);
            con.ExecuteNonQuerySQL(statement, c);
        }

        private String CopyTable( TableSchema originalTable, TableSchema copiedTableSchema, Boolean includeold)
        {
            SelectOperation s = new SelectOperation()
            {
                Restriction = new OperatorRestriction()
                {
                    LHS = new LiteralOperand() { Literal = "1" },
                    Op = RestrictionOperator.Equals,
                    RHS = new LiteralOperand() { Literal = "2" }
                },
                Columns = originalTable.Columns.Concat(new String[] { IntegratedConstants.StartTS, IntegratedConstants.EndTS }).Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Name, TableSchema = originalTable.Schema }
            };
            String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(copiedTableSchema.Schema, copiedTableSchema.Name, select);
            return copyTableSQL;
        }
    }
}
