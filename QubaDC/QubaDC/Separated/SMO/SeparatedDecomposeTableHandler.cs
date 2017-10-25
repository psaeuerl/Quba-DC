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
    class SeparatedDecomposeTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedDecomposeTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager metaManager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = metaManager;
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

                Table secondMetaTable = this.MetaManager.GetMetaTableFor(secondTableSchema);
                currentSchema.AddTable(secondTableSchema, secondTableHistSchema, secondMetaTable);

                ////Copy Tables without Triggers
                String[] copyTrueTable = CreateCopiedTables(originalTable, originalHistTable, firstTableSchema, firstTableHistSchema);

                String[] copyFalseTable = CreateCopiedTables( originalTable, originalHistTable, secondTableSchema, secondTableHistSchema);


                //Insert data from old to true                
                String insertTrueFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, firstTableSchema, null, firstTableSchema.Columns);


                //Insert data from old to true                
                String insertFromSecondTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, secondTableSchema, null, secondTableSchema.Columns);




                String DropFirstTable = SMORenderer.RenderDropTable(originalTable.Table.Schema, originalTable.Table.Name);
                currentSchema.RemoveTable(originalTable.Table.ToTable());



                //TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(partitionTable.BaseSchema, partitionTable.BaseTableName);
                //TableSchema originalHistTable = currentSchemaInfo.Schema.FindHistTable(originalTable.Table.ToTable());

                //var firstTableSchema = new TableSchema()
                //{
                //    Columns = originalTable.Table.Columns.Where(x => partitionTable.FirstColumns.Contains(x) || partitionTable.SharedColumns.Contains(x)).ToArray(),
                //    Name = partitionTable.FirstTableName,
                //    Schema = partitionTable.FirstSchema,
                //};
                //firstTableSchema.ColumnDefinitions = originalTable.Table.ColumnDefinitions.Where(x => firstTableSchema.Columns.Contains(x.ColumName)).ToArray();
                //var firstTableHistSchema = new TableSchema()
                //{
                //    Columns = firstTableSchema.Columns.Union(originalHistTable.Columns.Except(originalTable.Table.Columns)).ToArray(),
                //    Name = partitionTable.FirstTableName + "_" + currentSchemaInfo.ID,
                //    Schema = partitionTable.FirstSchema
                //};
                //firstTableHistSchema.ColumnDefinitions = originalHistTable.ColumnDefinitions.Where(x => firstTableHistSchema.Columns.Contains(x.ColumName)).ToArray();

                //Table firstTableMeta = this.MetaManager.GetMetaTableFor(firstTableSchema);
                //currentSchema.AddTable(firstTableSchema, firstTableHistSchema, firstTableMeta);

                //var secondTableSchema = new TableSchema()
                //{
                //    Columns = originalTable.Table.Columns.Where(x => partitionTable.SecondColumns.Contains(x) || partitionTable.SharedColumns.Contains(x)).ToArray(),
                //    Name = partitionTable.SecondTableName,
                //    Schema = partitionTable.SecondSchema
                //};
                //secondTableSchema.ColumnDefinitions = originalTable.Table.ColumnDefinitions.Where(x => secondTableSchema.Columns.Contains(x.ColumName)).ToArray();
                //var secondTableHistSchema = new TableSchema()
                //{
                //    Columns = secondTableSchema.Columns.Union(originalHistTable.Columns.Except(originalTable.Table.Columns)).ToArray(),
                //    Name = partitionTable.SecondTableName + "_" + currentSchemaInfo.ID,
                //    Schema = partitionTable.SecondSchema
                //};
                //secondTableHistSchema.ColumnDefinitions = originalHistTable.ColumnDefinitions.Where(x => secondTableHistSchema.Columns.Contains(x.ColumName)).ToArray();


                //Table secondMetaTable = this.MetaManager.GetMetaTableFor(secondTableSchema);
                //currentSchema.AddTable(secondTableSchema, secondTableHistSchema, secondMetaTable);

                //////Copy Tables without Triggers
                //String[] copyTrueTable = CreateCopiedTables(originalTable, originalHistTable, firstTableSchema, firstTableHistSchema);

                //String[] copyFalseTable = CreateCopiedTables(originalTable, originalHistTable, secondTableSchema, secondTableHistSchema);




                String dropOriginalMetaTable = SMORenderer.RenderDropTable(originalTable.MetaTableSchema, originalTable.MetaTableName);


                String createFirstMetaTable = this.MetaManager.GetCreateMetaTableFor(firstTableSchema.Schema, firstTableSchema.Name);
                String createSecondMetaTable = this.MetaManager.GetCreateMetaTableFor(secondTableSchema.Schema, secondTableSchema.Name);

                String insertMetadataFirstTable = this.MetaManager.GetStartInsertFor(firstTableSchema.Schema, firstTableSchema.Name);
                String insertMetadataSecondTable = this.MetaManager.GetStartInsertFor(secondTableSchema.Schema, secondTableSchema.Name);

                var StartEndTs = new String[] { updateTime, "NULL" };
                String insertHistFirst = SMORenderer.RenderInsertFromOneTableToOther(firstTableSchema, firstTableHistSchema, null, originalTable.Table.Columns, null, StartEndTs);
                String insertHistSecond = SMORenderer.RenderInsertFromOneTableToOther(secondTableSchema, secondTableHistSchema, null, originalTable.Table.Columns, null, StartEndTs);

                String[] Statements = copyTrueTable
                .Concat(copyFalseTable)
                .Concat(new String[]
                {
                    insertTrueFromTable,
                    insertFromSecondTable,

                    createFirstMetaTable,
                    createSecondMetaTable,
                    insertMetadataFirstTable,
                    insertMetadataSecondTable,
                    insertHistFirst,
                    insertHistSecond,
                    DropFirstTable,
                    dropOriginalMetaTable
                }).ToArray();

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
