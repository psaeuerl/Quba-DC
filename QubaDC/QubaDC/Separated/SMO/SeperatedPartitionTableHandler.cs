using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;
using QubaDC.Restrictions;
using QubaDC.CRUD;

namespace QubaDC.Separated.SMO
{
    class SeperatedPartitionTableHandler
    {
        private SchemaManager schemaManager;

        public SeperatedPartitionTableHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer, TableMetadataManager manager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = manager;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }

        internal void Handle(PartitionTable partitionTable)
        {
            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                String updateTime = this.SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");
                Schema currentSchema = currentSchemaInfo.Schema;




                TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(partitionTable.BaseSchema, partitionTable.BaseTableName);
                TableSchema originalHistTable = currentSchemaInfo.Schema.FindHistTable(originalTable.Table.ToTable());

                var trueTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns,
                    Name = partitionTable.TrueConditionTableName,
                    Schema = partitionTable.TrueConditionSchema,
                    ColumnDefinitions = originalTable.Table.ColumnDefinitions

                };
                var trueTableHist = new TableSchema()
                {
                    Columns = originalHistTable.Columns,
                    Name = partitionTable.TrueConditionTableName + "_" + currentSchemaInfo.ID,
                    Schema = partitionTable.TrueConditionSchema,
                    ColumnDefinitions = originalHistTable.ColumnDefinitions

                };

                Table firstTableMeta = this.MetaManager.GetMetaTableFor(trueTableSchema);
                currentSchema.AddTable(trueTableSchema, trueTableHist, firstTableMeta);

                var falseTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns,
                    Name = partitionTable.FalseConditionTableName,
                    Schema = partitionTable.FalseConditionSchema,
                    ColumnDefinitions = originalTable.Table.ColumnDefinitions

                };
                var falseTableSchemaHist = new TableSchema()
                {
                    Columns = originalHistTable.Columns,
                    Name = partitionTable.FalseConditionTableName + "_" + currentSchemaInfo.ID,
                    Schema = partitionTable.FalseConditionSchema,
                    ColumnDefinitions = originalHistTable.ColumnDefinitions

                };
                Table secondTableMeta = this.MetaManager.GetMetaTableFor(falseTableSchema);
                currentSchema.AddTable(falseTableSchema, falseTableSchemaHist, secondTableMeta);

                ////Copy Tables without Triggers
                String copyTrueTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, trueTableSchema.Schema, trueTableSchema.Name);

                //Copy Hist Table without Triggers
                String copyTrueHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, trueTableHist.Schema, trueTableHist.Name);


                String copyFalseTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, falseTableSchema.Schema, falseTableSchema.Name);

                //Copy Hist Table without Triggers
                String copyFalseHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, falseTableSchemaHist.Schema, falseTableSchemaHist.Name);



                //Insert data from old to true
                Restriction trueRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "TRUE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                String insertTrueFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, trueTableSchema, trueRestriction, null);

                //Insert data from old to false
                Restriction falseRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "FALSE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                String insertFalseFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, falseTableSchema, falseRestriction, null);


                String dropOriginalTable = SMORenderer.RenderDropTable(originalTable.Table.Schema, originalTable.Table.Name);
                currentSchema.RemoveTable(originalTable.Table.ToTable());

                //String updateSchema = this.schemaManager.GetInsertSchemaStatement(currentSchema, partitionTable);
                //con.ExecuteNonQuerySQL(updateSchema, c);

                //TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(partitionTable.BaseSchema, partitionTable.BaseTableName);
                //TableSchema originalHistTable = currentSchemaInfo.Schema.FindHistTable(originalTable.Table.ToTable());

                //var trueTableSchema = new TableSchema()
                //{
                //    Columns = originalTable.Table.Columns,
                //    Name = partitionTable.TrueConditionTableName,
                //    Schema = partitionTable.TrueConditionSchema,
                //    ColumnDefinitions = originalTable.Table.ColumnDefinitions

                //};
                //var trueTableHist = new TableSchema()
                //{
                //    Columns = originalHistTable.Columns,
                //    Name = partitionTable.TrueConditionTableName + "_" + currentSchemaInfo.ID,
                //    Schema = partitionTable.TrueConditionSchema,
                //    ColumnDefinitions = originalHistTable.ColumnDefinitions

                //};
                //Table firstTableMeta = this.MetaManager.GetMetaTableFor(trueTableSchema);
                //currentSchema.AddTable(trueTableSchema, trueTableHist, firstTableMeta);

                //var falseTableSchema = new TableSchema()
                //{
                //    Columns = originalTable.Table.Columns,
                //    Name = partitionTable.FalseConditionTableName,
                //    Schema = partitionTable.FalseConditionSchema,
                //    ColumnDefinitions = originalTable.Table.ColumnDefinitions

                //};
                //var falseTableSchemaHist = new TableSchema()
                //{
                //    Columns = originalHistTable.Columns,
                //    Name = partitionTable.FalseConditionTableName + "_" + currentSchemaInfo.ID,
                //    Schema = partitionTable.FalseConditionSchema,
                //    ColumnDefinitions = originalHistTable.ColumnDefinitions

                //};
                //Table secondTableMeta = this.MetaManager.GetMetaTableFor(falseTableSchema);
                //currentSchema.AddTable(falseTableSchema, falseTableSchemaHist, secondTableMeta);

                //////Copy Tables without Triggers
                //String copyTrueTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, trueTableSchema.Schema, trueTableSchema.Name);

                ////Copy Hist Table without Triggers
                //String copyTrueHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, trueTableHist.Schema, trueTableHist.Name);


                //String copyFalseTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, falseTableSchema.Schema, falseTableSchema.Name);

                ////Copy Hist Table without Triggers
                //String copyFalseHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, falseTableSchemaHist.Schema, falseTableSchemaHist.Name);


                //String nowVariable = SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");
                //String[] allColumns = originalTable.Table.Columns;
                //var StartEndTs = new String[] { nowVariable, "NULL" };


                //var Restriction = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(originalTable.Table.Name, nowVariable /* or NOW(3)?*/);
                ////Insert data from old to true
                //Restriction trueRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "TRUE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                //Restriction trueRestAll = new AndRestriction() { Restrictions = new Restriction[] { Restriction, trueRestriction } };
                //String insertTrueFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, trueTableSchema, trueRestAll, allColumns, null, StartEndTs);


                ////Insert data from old to false
                //Restriction falseRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "FALSE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                //Restriction falseRestAll = new AndRestriction() { Restrictions = new Restriction[] { Restriction, falseRestriction } };
                //String insertFalseFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, falseTableSchema, falseRestAll, allColumns, null, StartEndTs);




                //String[] dropOriginalHist = DropHistTableRenameCurrentToHist(originalTable);

                //currentSchema.RemoveTable(originalTable.Table.ToTable());
                String dropOriginalMetaTable = SMORenderer.RenderDropTable(originalTable.MetaTableSchema, originalTable.MetaTableName);




                ////con.ExecuteNonQuerySQL(insertFromTable);
                //// String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(new Table() { TableName = addColumn.TableName, TableSchema = addColumn.Schema }, updateTime);

                String createFirstMetaTable = this.MetaManager.GetCreateMetaTableFor(trueTableSchema.Schema, trueTableSchema.Name);
                String createSecondMetaTable = this.MetaManager.GetCreateMetaTableFor(falseTableSchema.Schema, falseTableSchema.Name);

                String insertMetadataFirstTable = this.MetaManager.GetStartInsertFor(trueTableSchema.Schema, trueTableSchema.Name);
                String insertMetadataSecondTable = this.MetaManager.GetStartInsertFor(falseTableSchema.Schema, falseTableSchema.Name);

                var StartEndTs = new String[] { updateTime, "NULL" };
                String insertHistFirst = SMORenderer.RenderInsertFromOneTableToOther(trueTableSchema, trueTableHist, null, trueTableSchema.Columns, null, StartEndTs);
                String insertHistSecond = SMORenderer.RenderInsertFromOneTableToOther(falseTableSchema, falseTableSchemaHist, null, falseTableSchema.Columns, null, StartEndTs);

                String[] Statements = new String[]
                {
                    copyTrueTable,
                    copyTrueHistTableSQL,
                    copyFalseTable,
                    copyFalseHistTableSQL,
                    insertTrueFromTable,
                    insertFalseFromTable,
                    dropOriginalTable,
                    dropOriginalMetaTable,
                    createFirstMetaTable,
                    insertMetadataFirstTable,
                    createSecondMetaTable,
                    insertMetadataSecondTable,
                    insertHistFirst,
                    insertHistSecond                  
                };

                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { originalTable.ToTable() },
                    TablesToUnlock = new Table[] { }
                };
            };


            SeparatedSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 partitionTable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , this.MetaManager);
        }

        private void CreateTriggers(System.Data.Common.DbConnection c, MySQLDataConnection con, Schema currentSchema, TableSchema trueTableSchema, TableSchema trueTableHist)
        {
            //Create Triggers on copiedTable

            //INsert Trigger 
            String trigger = SMORenderer.RenderCreateInsertTrigger(trueTableSchema, trueTableHist);
            //Delete Trigger
            String deleteTrigger = SMORenderer.RenderCreateDeleteTrigger(trueTableSchema, trueTableHist);
            //Update Trigger
            String UpdateTrigger = SMORenderer.RenderCreateUpdateTrigger(trueTableSchema, trueTableHist);

            //Add Trigger
            con.ExecuteSQLScript(trigger, c);
            con.ExecuteSQLScript(deleteTrigger, c);
            con.ExecuteSQLScript(UpdateTrigger, c);
        }
    }
}
