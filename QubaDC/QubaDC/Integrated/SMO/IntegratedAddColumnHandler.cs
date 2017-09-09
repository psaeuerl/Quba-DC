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
using QubaDC.Integrated.SMO;

namespace QubaDC.Separated.SMO
{
    class IntegratedAddColumnHandler
    {
        private readonly GlobalUpdateTimeManager GlobalUpdateTimeManager;
        private SchemaManager schemaManager;

        public IntegratedAddColumnHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer, GlobalUpdateTimeManager manager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.GlobalUpdateTimeManager = manager;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(AddColum dropColumn)
        {
            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data
            SchemaInfo xy = this.schemaManager.GetCurrentSchema();
            Schema currentSchema = xy.Schema;


            TableSchemaWithHistTable originalTable = xy.Schema.FindTable(dropColumn.Schema, dropColumn.TableName);
            TableSchema originalHistTable = xy.Schema.FindHistTable(originalTable.Table.ToTable());

            var copiedTableSchema = new TableSchema()
            {
                Columns = originalTable.Table.Columns.Union(new String[] { dropColumn.Column.ColumName }).ToArray(),
                Name = originalTable.Table.Name,
                Schema = originalTable.Table.Schema,
                ColumnDefinitions = originalTable.Table.ColumnDefinitions.Union(new ColumnDefinition[] { dropColumn.Column }).ToArray(),
            };
            var copiedHistSchema = new TableSchema()
            {
                Columns = copiedTableSchema.Columns.Union(originalHistTable.Columns.Except(copiedTableSchema.Columns)).ToArray(),
                Name = originalTable.Table.Name + "_" + xy.ID,
                Schema = originalTable.Table.Schema,
                ColumnDefinitions = copiedTableSchema.ColumnDefinitions.Union(originalHistTable.ColumnDefinitions
                                        .Where(x => copiedTableSchema.Columns.Contains(x.ColumName) == false)).ToArray()
            };
            currentSchema.RemoveTable(originalTable.Table.ToTable());
            currentSchema.AddTable(copiedTableSchema, copiedHistSchema);

            String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
            {
                NewSchema = originalTable.Table.Schema,
                NewTableName = originalTable.Table.Name + "_old",
                OldSchema = originalTable.Table.Schema,
                OldTableName = originalTable.Table.Name
            });
             String copyOriginalTable =   CopyTable(originalTable.Table, copiedTableSchema, true);


            var con = (MySQLDataConnection)DataConnection;


            String[] PreLockingStatements = null;// new String[] { createBaseTable, createHistTable };
            String[] AfterLockingStatemnts = new String[] { renameTableSQL, copyOriginalTable };
            String[] tablesToLock = 
            new String[] { this.schemaManager.GetTableName(), this.GlobalUpdateTimeManager.GetTableName() ,
                    SMORenderer.CRUDRenderer.PrepareTable(originalTable.Table.ToTable()),
                    SMORenderer.CRUDRenderer.PrepareTa ble(originalHistTable.ToTable())
                };
                //                                             this.SMORenderer.CRUDRenderer.PrepareTable(createTable.ToTable()),
                //                                             this.SMORenderer.CRUDRenderer.PrepareTable(ctHistTable.ToTable()),

            IntegratedSMOExecuter.Execute(SMORenderer, con, PreLockingStatements, AfterLockingStatemnts, tablesToLock);
            //con.DoTransaction((transaction, c) =>
            //{







            //con.ExecuteNonQuerySQL(renameTableSQL);

            //    CopyTable(c, con, originalTable.Table, copiedTableSchema, true);
            //    AlterTable(c, con, copiedTableSchema, dropColumn.Column);
            //    CopyTable(c, con, originalHistTable, copiedHistSchema, false);
            //    AlterTable(c, con, copiedHistSchema, dropColumn.Column);


            //    String dropInsertTrigger = SMORenderer.RenderDropInsertTrigger(copiedTableSchema, originalHistTable);
            //    String dropUpdaterigger = SMORenderer.RenderDropUpdaterigger(copiedTableSchema, originalHistTable);
            //    String dropDeleteTrigger = SMORenderer.RenderDropDeleteTrigger(copiedTableSchema, originalHistTable);

            //    con.ExecuteSQLScript(dropInsertTrigger, c);
            //    con.ExecuteSQLScript(dropUpdaterigger, c);
            //    con.ExecuteSQLScript(dropDeleteTrigger, c);



            //    //INsert Trigger 
            //    String trigger = SMORenderer.RenderCreateInsertTrigger(copiedTableSchema, copiedHistSchema);
            //    //Delete Trigger
            //    String deleteTrigger = SMORenderer.RenderCreateDeleteTrigger(copiedTableSchema, copiedHistSchema);
            //    //Update Trigger
            //    String UpdateTrigger = SMORenderer.RenderCreateUpdateTrigger(copiedTableSchema, copiedHistSchema);

            //    ////Add Trigger
            //    con.ExecuteSQLScript(trigger, c);
            //    con.ExecuteSQLScript(deleteTrigger, c);
            //    con.ExecuteSQLScript(UpdateTrigger, c);




            //    ////Insert data from old to new
            //    SelectOperation s = new SelectOperation()
            //    {
            //        Columns = originalTable.Table.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
            //        LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = dropColumn.InitalValue, ColumnName = dropColumn.Column.ColumName } },
            //        FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Table.Name + "_old", TableSchema = originalTable.Table.Schema }
            //    };
            //    String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);
            //    String insertFromTable = SMORenderer.RenderInsertToTableFromSelect(copiedTableSchema, select);
            //    con.ExecuteNonQuerySQL(insertFromTable);


            //    this.schemaManager.StoreSchema(currentSchema, dropColumn, con, c);

            //    String dropTableSql = SMORenderer.RenderDropTable(originalTable.Table.Schema, originalTable.Table.Name + "_old");
            //    con.ExecuteNonQuerySQL(dropTableSql);

            //    transaction.Commit();
            //});


        }

        private void AlterTable(System.Data.Common.DbConnection c, MySQLDataConnection con, TableSchema copiedTableSchema, ColumnDefinition column)
        {
            String statement = this.SMORenderer.RenderAddColumn(copiedTableSchema, column);
            con.ExecuteNonQuerySQL(statement, c);
        }

        private String CopyTable(TableSchema originalTable, TableSchema copiedTableSchema, Boolean includeold)
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
                FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Name + (includeold ? "_old" : ""), TableSchema = originalTable.Schema }
            };
            String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(copiedTableSchema.Schema, copiedTableSchema.Name, select);
            return copyTableSQL;
        }
    }
}
