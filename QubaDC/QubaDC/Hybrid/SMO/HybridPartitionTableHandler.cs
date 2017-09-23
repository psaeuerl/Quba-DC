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

namespace QubaDC.Hybrid.SMO
{
    class HybridPartitionTableHandler
    {
        private SchemaManager schemaManager;

        public HybridPartitionTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager MetaManager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = MetaManager;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(PartitionTable partitionTable)
        {
            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data


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
                currentSchema.AddTable(trueTableSchema, trueTableHist,firstTableMeta);

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
                Table falseTableMeta = this.MetaManager.GetMetaTableFor(falseTableSchema);
                currentSchema.AddTable(falseTableSchema, falseTableSchemaHist, falseTableMeta);

                ////Copy Tables without Triggers
                String copyTrueTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, trueTableSchema.Schema, trueTableSchema.Name);

                //Copy Hist Table without Triggers
                String copyTrueHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, trueTableHist.Schema, trueTableHist.Name);


                String copyFalseTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, falseTableSchema.Schema, falseTableSchema.Name);

                //Copy Hist Table without Triggers
                String copyFalseHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, falseTableSchemaHist.Schema, falseTableSchemaHist.Name);


                String nowVariable = SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");
                String[] allColumns = originalTable.Table.Columns;
                var startTsValue = new String[] { nowVariable };


                var Restriction = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(originalTable.Table.Name, nowVariable /* or NOW(3)?*/);
                //Insert data from old to true
                Restriction trueRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "TRUE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                TableSchema copiedTrueWithTS = new TableSchema()
                {
                    Columns = trueTableSchema.Columns.Concat(new String[] { HybridConstants.StartTS, }).ToArray(),
                    Name = trueTableSchema.Name,
                    Schema = trueTableSchema.Schema
                };
                String insertTrueFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, copiedTrueWithTS, trueRestriction, allColumns, null, startTsValue);


                //Insert data from old to false
                Restriction falseRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "FALSE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                TableSchema copiedFalseWithSchema = new TableSchema()
                {
                    Columns = falseTableSchema.Columns.Concat(new String[] { HybridConstants.StartTS, }).ToArray(),
                    Name = falseTableSchema.Name,
                    Schema = falseTableSchema.Schema
                };
                String insertFalseFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, copiedFalseWithSchema, falseRestriction, allColumns, null, startTsValue);



                String dropOriginalTable = SMORenderer.RenderDropTable(originalTable.Table.Schema, originalTable.Table.Name);
                currentSchema.RemoveTable(originalTable.Table.ToTable());
                String dropOriginalMetaTable = SMORenderer.RenderDropTable(originalTable.MetaTableSchema, originalTable.MetaTableName);




                //con.ExecuteNonQuerySQL(insertFromTable);
                // String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(new Table() { TableName = addColumn.TableName, TableSchema = addColumn.Schema }, updateTime);

                String createFirstMetaTable = this.MetaManager.GetCreateMetaTableFor(trueTableSchema.Schema, trueTableSchema.Name);
                String createSecondMetaTable = this.MetaManager.GetCreateMetaTableFor(falseTableSchema.Schema, falseTableSchema.Name);

                String insertMetadataFirstTable = this.MetaManager.GetStartInsertFor(trueTableSchema.Schema, trueTableSchema.Name);
                String insertMetadataSecondTable = this.MetaManager.GetStartInsertFor(falseTableSchema.Schema, falseTableSchema.Name);

                //Insert data to hist
                SelectOperation selectCurrentFromBaseTable = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "t1" } },
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updateTime, ColumnName = "ut" } },
                    FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Table.Name, TableSchema = originalTable.Table.Schema },
                };
                String selectCurrentWithUT = this.SMORenderer.CRUDHandler.RenderSelectOperation(selectCurrentFromBaseTable);
                String isnertIntoHist = this.SMORenderer.CRUDRenderer.RenderInsertSelect(new Table()
                { TableSchema = originalHistTable.Schema, TableName = originalHistTable.Name },
                    null,
                    selectCurrentWithUT);



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


                String[] Statements = new String[]
                {
                    copyTrueTable,
                    copyTrueHistTableSQL,
                    copyFalseTable,
                    copyFalseHistTableSQL,
                    isnertIntoHist,
                    insertTrueFromTable,
                    insertFalseFromTable,
                    dropOriginalMetaTable,
                    createFirstMetaTable,
                    insertMetadataFirstTable,
                    createSecondMetaTable,
                    insertMetadataSecondTable,
                    dropOriginalTable,
                };


                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { originalTable.ToTable() },
                    TablesToUnlock = new Table[] { }
                };
            };


            HybridSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 partitionTable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , this.MetaManager);

        }

    }
}
