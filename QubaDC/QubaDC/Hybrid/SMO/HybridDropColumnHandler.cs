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
    class HybridDropColumnHandler
    {
        private SchemaManager schemaManager;

        public HybridDropColumnHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(DropColumn dropColumn)
        {
            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data

            var con = (MySQLDataConnection)DataConnection;
            con.DoTransaction((transaction, c) =>
            {

                SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
                Schema currentSchema = xy.Schema;


                TableSchemaWithHistTable originalTable = xy.Schema.FindTable(dropColumn.Schema, dropColumn.TableName);
                TableSchema originalHistTable = xy.Schema.FindHistTable(originalTable.Table.ToTable());

                var copiedTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Where(x => x != dropColumn.Column).ToArray(),
                    Name = originalTable.Table.Name,
                    Schema = originalTable.Table.Schema,
                     ColumnDefinitions = originalTable.Table.ColumnDefinitions.Where(x => x.ColumName != dropColumn.Column).ToArray(),
            };
                var copiedHistSchema = new TableSchema()
                {
                    Columns =originalHistTable.Columns.Where(x => x != dropColumn.Column).ToArray(),
                    Name = originalTable.Table.Name + "_" + xy.ID,
                    Schema = originalTable.Table.Schema,
                    ColumnDefinitions = originalHistTable.ColumnDefinitions.Where(x => x.ColumName != dropColumn.Column).ToArray(),
                };

                Guard.StateTrue(copiedTableSchema.Columns.Count() + 1 == originalTable.Table.Columns.Count(), "Could not find column: " + dropColumn.Column);
                Guard.StateTrue(copiedHistSchema.Columns.Count() + 1 == originalHistTable.Columns.Count(), "Could not find column: " + dropColumn.Column);
                Guard.StateTrue(copiedTableSchema.ColumnDefinitions.Count() + 1 == originalTable.Table.ColumnDefinitions.Count(), "Could not find column: " + dropColumn.Column);
                Guard.StateTrue(copiedHistSchema.ColumnDefinitions.Count() + 1 == originalHistTable.ColumnDefinitions.Count(), "Could not find column: " + dropColumn.Column);

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

                CopyTable(c, con, originalTable.Table, copiedTableSchema,true,true);
                CopyTable(c, con, originalHistTable, copiedHistSchema,false,false);

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
                    Columns = copiedTableSchema.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                     LiteralColumns = new LiteralColumn[] {  },
                    FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Table.Name + "_old", TableSchema = originalTable.Table.Schema }
                };
                String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);
                String insertFromTable = SMORenderer.RenderInsertToTableFromSelect(copiedTableSchema, select);
                con.ExecuteNonQuerySQL(insertFromTable);

                String updateSchema = this.schemaManager.GetInsertSchemaStatement(currentSchema, dropColumn);
                con.ExecuteNonQuerySQL(updateSchema, c);

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
                FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Name + (includeOldTable ? "_old": ""), TableSchema = originalTable.Schema }
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
