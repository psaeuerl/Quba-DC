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
using QubaDC.Hybrid.SMO;
using QubaDC.Hybrid;

namespace QubaDC.Separated.SMO
{
    class HybridDecomposeTableHandler
    {
        private SchemaManager schemaManager;

        public HybridDecomposeTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager meta)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = meta;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(DecomposeTable partitionTable)
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

                String nowVariable = SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");
                var startTsValue = new String[] { nowVariable };

                var firstTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Where(x => partitionTable.FirstColumns.Contains(x) || partitionTable.SharedColumns.Contains(x)).ToArray(),
                    Name = partitionTable.FirstTableName,
                    Schema = partitionTable.FirstSchema,
                };
                firstTableSchema.ColumnDefinitions = originalTable.Table.ColumnDefinitions.Where(x => firstTableSchema.Columns.Contains(x.ColumName)).ToArray();
                var firstTableHistSchema = new TableSchema()
                {
                    Columns = firstTableSchema.Columns.Union(originalHistTable.Columns.Except(originalTable.Table.Columns)).ToArray(),
                    Name = partitionTable.FirstTableName + "_" + currentSchemaInfo.ID,
                    Schema = partitionTable.FirstSchema
                };
                firstTableHistSchema.ColumnDefinitions = originalHistTable.ColumnDefinitions.Where(x => firstTableHistSchema.Columns.Contains(x.ColumName)).ToArray();
                Table firstTableMeta = this.MetaManager.GetMetaTableFor(firstTableSchema);
                currentSchema.AddTable(firstTableSchema, firstTableHistSchema,firstTableMeta);

                var secondTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Where(x => partitionTable.SecondColumns.Contains(x) || partitionTable.SharedColumns.Contains(x)).ToArray(),
                    Name = partitionTable.SecondTableName,
                    Schema = partitionTable.SecondSchema
                };
                secondTableSchema.ColumnDefinitions = originalTable.Table.ColumnDefinitions.Where(x => secondTableSchema.Columns.Contains(x.ColumName)).ToArray();
                var secondTableHistSchema = new TableSchema()
                {
                    Columns = secondTableSchema.Columns.Union(originalHistTable.Columns.Except(originalTable.Table.Columns)).ToArray(),
                    Name = partitionTable.SecondTableName + "_" + currentSchemaInfo.ID,
                    Schema = partitionTable.SecondSchema
                };
                secondTableHistSchema.ColumnDefinitions = originalHistTable.ColumnDefinitions.Where(x => secondTableHistSchema.Columns.Contains(x.ColumName)).ToArray();
                Table secondTableMeta = this.MetaManager.GetMetaTableFor(firstTableSchema);
                currentSchema.AddTable(secondTableSchema, secondTableHistSchema, secondTableMeta);

                ////Copy Tables without Triggers
                String[] copyTablesFirst =  CreateCopiedTables(originalTable, originalHistTable, firstTableSchema, firstTableHistSchema);

                String[] copyTablesSecond = CreateCopiedTables( originalTable, originalHistTable, secondTableSchema, secondTableHistSchema);


                //TableSchema copiedFirstSchema = new TableSchema()
                //{
                //    Columns = firstTableSchema.Columns.Concat(new String[] { HybridConstants.StartTS, }).ToArray(),
                //    Name = firstTableSchema.Name,
                //    Schema = firstTableSchema.Schema
                //};
                String insertFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, firstTableSchema, null,
                    firstTableSchema.Columns,
                   null, startTsValue);
                //Insert data from old to true                
                //String insertTrueFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, firstTableSchema, null, firstTableSchema.Columns);



                //TableSchema copiedSecondSchema = new TableSchema()
                //{
                //    Columns = secondTableSchema.Columns.Concat(new String[] { HybridConstants.StartTS, }).ToArray(),
                //    Name = secondTableSchema.Name,
                //    Schema = secondTableSchema.Schema
                //};
                //Insert data from old to true                
                String insertFromSeconDtable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, secondTableSchema, null,
                    firstTableSchema.Columns,
                   null, startTsValue);


                String dropOriginalTable = SMORenderer.RenderDropTable(originalTable.Table.Schema, originalTable.Table.Name);
                currentSchema.RemoveTable(originalTable.Table.ToTable());

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
                //Table falseTableMeta = this.MetaManager.GetMetaTableFor(falseTableSchema);
                //currentSchema.AddTable(falseTableSchema, falseTableSchemaHist, falseTableMeta);

                //////Copy Tables without Triggers
                //String copyTrueTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, trueTableSchema.Schema, trueTableSchema.Name);

                ////Copy Hist Table without Triggers
                //String copyTrueHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, trueTableHist.Schema, trueTableHist.Name);


                //String copyFalseTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, falseTableSchema.Schema, falseTableSchema.Name);

                ////Copy Hist Table without Triggers
                //String copyFalseHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, falseTableSchemaHist.Schema, falseTableSchemaHist.Name);


                //String nowVariable = SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");
                //String[] allColumns = originalTable.Table.Columns;
                //var startTsValue = new String[] { nowVariable };


                //var Restriction = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(originalTable.Table.Name, nowVariable /* or NOW(3)?*/);
                ////Insert data from old to true
                //Restriction trueRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "TRUE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                //TableSchema copiedTrueWithTS = new TableSchema()
                //{
                //    Columns = trueTableSchema.Columns.Concat(new String[] { HybridConstants.StartTS, }).ToArray(),
                //    Name = trueTableSchema.Name,
                //    Schema = trueTableSchema.Schema
                //};
                //String insertTrueFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, copiedTrueWithTS, trueRestriction, allColumns, null, startTsValue);


                ////Insert data from old to false
                //Restriction falseRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "FALSE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                //TableSchema copiedFalseWithSchema = new TableSchema()
                //{
                //    Columns = falseTableSchema.Columns.Concat(new String[] { HybridConstants.StartTS, }).ToArray(),
                //    Name = falseTableSchema.Name,
                //    Schema = falseTableSchema.Schema
                //};
                //String insertFalseFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, copiedFalseWithSchema, falseRestriction, allColumns, null, startTsValue);



                //currentSchema.RemoveTable(originalTable.Table.ToTable());
                String dropOriginalMetaTable = SMORenderer.RenderDropTable(originalTable.MetaTableSchema, originalTable.MetaTableName);




                ////con.ExecuteNonQuerySQL(insertFromTable);
                //// String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(new Table() { TableName = addColumn.TableName, TableSchema = addColumn.Schema }, updateTime);

                String createFirstMetaTable = this.MetaManager.GetCreateMetaTableFor(firstTableSchema.Schema, firstTableSchema.Name);
                String createSecondMetaTable = this.MetaManager.GetCreateMetaTableFor(secondTableSchema.Schema, secondTableSchema.Name);

                String insertMetadataFirstTable = this.MetaManager.GetStartInsertFor(firstTableSchema.Schema, firstTableSchema.Name);
                String insertMetadataSecondTable = this.MetaManager.GetStartInsertFor(secondTableSchema.Schema, secondTableSchema.Name);

                ////Insert data to hist
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


                String[] Statements = copyTablesFirst
                                      .Concat(copyTablesSecond)
                                      .Concat(new String[]
                                        {
                                            insertFromTable,
                                            insertFromSeconDtable,
                                            isnertIntoHist,
                                            createFirstMetaTable,
                                            insertMetadataFirstTable,
                                            createSecondMetaTable,
                                            insertMetadataSecondTable,
                                            dropOriginalMetaTable,
                                            dropOriginalTable,
                                        }).ToArray();


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

        private String[] CreateCopiedTables( TableSchemaWithHistTable originalTable, TableSchema originalHistTable, TableSchema normalschema, TableSchema nomralHistSchema)
        {
            String copyTrueTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, normalschema.Schema, normalschema.Name);

            String[] firstTableDropColumns = originalTable.Table.Columns.Except(normalschema.Columns).ToArray();
            String firstTableDropColumnsSQL = SMORenderer.RenderDropColumns(normalschema.Schema, normalschema.Name, firstTableDropColumns);

            String copyTrueHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, nomralHistSchema.Schema, nomralHistSchema.Name);

            String[] firstHistTableDropColumns = originalHistTable.Columns.Except(nomralHistSchema.Columns).ToArray();
            String firstHistTableDropColumnsSQL = SMORenderer.RenderDropColumns(nomralHistSchema.Schema, nomralHistSchema.Name, firstTableDropColumns);

            return new String[]
            {
                copyTrueTable,
                firstTableDropColumnsSQL,
                copyTrueHistTableSQL,
                firstHistTableDropColumnsSQL
            };
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
