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

namespace QubaDC.Separated.SMO
{
    class HybridRenameColumnHandler
    {
        private SchemaManager schemaManager;

        public HybridRenameColumnHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(RenameColumn renameColumn)
        {

            ////What to do here?
            ////a.) Copy table
            ////b.) Add table to the Schemamanager
            ////c.) Delete Trigger to the table
            ////d.) Recreate Trigger on the table with correct hist table
            ////e.) Copy Data

            var con = (MySQLDataConnection)DataConnection;
            con.DoTransaction((transaction, c) =>
            {
                SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
                Schema currentSchema = xy.Schema;


                TableSchemaWithHistTable originalTable = xy.Schema.FindTable(renameColumn.Schema, renameColumn.TableName);
                TableSchema originalHistTable = xy.Schema.FindHistTable(originalTable.Table.ToTable());

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
                    Name = originalTable.Table.Name + "_" + xy.ID,
                    Schema = originalTable.Table.Schema,
                    ColumnDefinitions = originalHistTable.ColumnDefinitions
                };
                copiedHistSchema.ColumnDefinitions.First(x => x.ColumName == renameColumn.ColumnName).ColumName = renameColumn.RenameName;

                //Guard.StateTrue(copiedTableSchema.Columns.Count() == originalTable.Table.Columns.Count() + 1, "Could add new column: " + dropColumn.Column);
                //Guard.StateTrue(copiedHistSchema.Columns.Count() == originalHistTable.Columns.Count() + 1, "Could add new column: " + dropColumn.Column);

                currentSchema.RemoveTable(originalTable.Table.ToTable());
                currentSchema.AddTable(copiedTableSchema, copiedHistSchema);

                String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
                {
                    NewSchema = originalTable.Table.Schema,
                    NewTableName = originalTable.Table.Name + "_old",
                    OldSchema = originalTable.Table.Schema,
                    OldTableName = originalTable.Table.Name
                });

                con.ExecuteNonQuerySQL(renameTableSQL);
                CopyTable(c, con, originalTable.Table, copiedTableSchema, true, true);
                CopyTable(c, con, originalHistTable, copiedHistSchema, false, false);
                ColumnDefinition cd = copiedTableSchema.ColumnDefinitions.First(x => x.ColumName == renameColumn.RenameName);
                String renameColumnSQL = SMORenderer.RenderRenameColumn(renameColumn, cd, copiedTableSchema);

                con.ExecuteNonQuerySQL(renameColumnSQL);

                ColumnDefinition cdHist = copiedHistSchema.ColumnDefinitions.First(x => x.ColumName == renameColumn.RenameName);
                String renameColumnSQLHist = SMORenderer.RenderRenameColumn(renameColumn, cdHist, copiedHistSchema);

                con.ExecuteNonQuerySQL(renameColumnSQLHist);


                //The way we change here is important!
                //First replace the insert trigger
                //Do the selectoperation!
                //Then do the delte
                //Then recreate the oterhs
                //Otherwise, we get no data
                String dropInsertTrigger = SMORenderer.RenderDropInsertTrigger(copiedTableSchema, originalHistTable);
                con.ExecuteSQLScript(dropInsertTrigger, c);
                //INsert Trigger 
                String trigger = SMORenderer.RenderCreateInsertTrigger(copiedTableSchema, copiedHistSchema);
                ////Add Trigger
                con.ExecuteSQLScript(trigger, c);
            


                ////Insert data from old to new
                SelectOperation s = new SelectOperation()
                {
                    Columns = originalTable.Table.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                    LiteralColumns = new LiteralColumn[] { },
                    FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Table.Name + "_old", TableSchema = originalTable.Table.Schema }
                };
                String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);
                String insertFromTable = SMORenderer.RenderInsertToTableFromSelect(copiedTableSchema, select);
                con.ExecuteNonQuerySQL(insertFromTable);

                this.schemaManager.StoreSchema(currentSchema, renameColumn, con, c);
                //String updateSchema = this.schemaManager.GetInsertSchemaStatement(currentSchema, dropColumn);
                //con.ExecuteNonQuerySQL(updateSchema, c);

                //Before we drop the old table, we delete everything in there, moving the data to the hist table!
                String delete = this.SMORenderer.CRUDRenderer.RenderDelete(new Table() { TableName = originalTable.Table.Name + "_old", TableSchema = originalTable.Table.Schema }, null);
                con.ExecuteNonQuerySQL(delete);

                String dropTableSql = SMORenderer.RenderDropTable(originalTable.Table.Schema, originalTable.Table.Name + "_old");
                con.ExecuteNonQuerySQL(dropTableSql);



                //Delete Trigger
                String deleteTrigger = SMORenderer.RenderCreateDeleteTrigger(copiedTableSchema, copiedHistSchema);
                //Update Trigger
                String UpdateTrigger = SMORenderer.RenderCreateUpdateTrigger(copiedTableSchema, copiedHistSchema);


                con.ExecuteSQLScript(deleteTrigger, c);
                con.ExecuteSQLScript(UpdateTrigger, c);

                transaction.Commit();
            });


        }

        private void AlterTable(System.Data.Common.DbConnection c, MySQLDataConnection con, TableSchema copiedTableSchema, ColumnDefinition column)
        {
            String statement = this.SMORenderer.RenderAddColumn(copiedTableSchema, column);
            con.ExecuteNonQuerySQL(statement, c);
        }

        private void CopyTable(System.Data.Common.DbConnection c, MySQLDataConnection con, TableSchema originalTable, TableSchema copiedTableSchema, Boolean includeOldTable, Boolean includeTsColumn)
        {
            SelectOperation s = new SelectOperation()
            {
                Restriction = new OperatorRestriction()
                {
                    LHS = new LiteralOperand() { Literal = "1" },
                    Op = RestrictionOperator.Equals,
                    RHS = new LiteralOperand() { Literal = "2" }
                },
                Columns = originalTable.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Name + (includeOldTable ? "_old" : ""), TableSchema = originalTable.Schema }
            };
            if (includeTsColumn)
                s.Columns = s.Columns.Union(new ColumnReference[] { new ColumnReference() { ColumnName = Hybrid.HybridConstants.StartTS, TableReference = s.FromTable.TableAlias } }).ToArray();
            String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(copiedTableSchema.Schema, copiedTableSchema.Name, select);
            con.ExecuteNonQuerySQL(copyTableSQL, c);
        }
    }
}
