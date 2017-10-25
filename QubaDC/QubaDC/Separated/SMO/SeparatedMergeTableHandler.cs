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

namespace QubaDC.Separated.SMO
{
    class SeparatedMergeTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedMergeTableHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer, TableMetadataManager metaManager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = metaManager;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }


        internal void Handle(MergeTable mergeTable)
        {

            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data twice!


            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                String updateTime = this.SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");
                Schema currentSchema = currentSchemaInfo.Schema;


                TableSchemaWithHistTable firstTable = currentSchemaInfo.Schema.FindTable(mergeTable.FirstSchema, mergeTable.FirstTableName);
                TableSchema firstHistTable = currentSchemaInfo.Schema.FindHistTable(firstTable.Table.ToTable());

                TableSchemaWithHistTable secondTable = currentSchemaInfo.Schema.FindTable(mergeTable.FirstSchema, mergeTable.SecondTableName);
                TableSchema secondHistTable = currentSchemaInfo.Schema.FindHistTable(secondTable.Table.ToTable());

                Guard.StateTrue(firstTable.Table.Columns.SequenceEqual<String>(secondTable.Table.Columns), "Firsttable and Secondtable do not have the same columns");

                var mergedTableSchema = new TableSchema()
                {
                    Columns = firstTable.Table.Columns,
                    Name = mergeTable.ResultTableName,
                    Schema = mergeTable.ResultSchema,
                    ColumnDefinitions = firstTable.Table.ColumnDefinitions
                };
                var mergedHistTableSchema = new TableSchema()
                {
                    Columns = firstHistTable.Columns,
                    Name = mergeTable.ResultTableName + "_" + currentSchemaInfo.ID,
                    Schema = mergeTable.ResultSchema,
                    ColumnDefinitions = firstHistTable.ColumnDefinitions
                };
                Table firstTableMeta = this.MetaManager.GetMetaTableFor(mergedTableSchema);
                currentSchema.AddTable(mergedTableSchema, mergedHistTableSchema, firstTableMeta);
                currentSchema.RemoveTable(firstTable.Table.ToTable());
                currentSchema.RemoveTable(secondTable.Table.ToTable());
                //Copy Table without Triggers
                String copyTableSQL = SMORenderer.RenderCopyTable(firstTable.Table.Schema, firstTable.Table.Name, mergedTableSchema.Schema, mergedTableSchema.Name);

                //Copy Hist Table without Triggers
                String copyHistTableSQL = SMORenderer.RenderCopyTable(firstHistTable.Schema, firstHistTable.Name, mergedHistTableSchema.Schema, mergedHistTableSchema.Name);



                //Insert data from old to new
                String insertFromFirstTable = SMORenderer.RenderInsertFromOneTableToOther(firstTable.Table, mergedTableSchema, null, null);

                String insertFromSecondTable = SMORenderer.RenderInsertFromOneTableToOther(secondTable.Table, mergedTableSchema, null, null);


                String DropFirstTable = SMORenderer.RenderDropTable(firstTable.Table.Schema, firstTable.Table.Name);
                String DropSecondTable = SMORenderer.RenderDropTable(secondTable.Table.Schema, secondTable.Table.Name);


                //TableSchema isnertWithStartts = new TableSchema()
                //{
                //    Columns = firstTable.Table.Columns.Concat(new String[] { IntegratedConstants.StartTS, IntegratedConstants.EndTS }).ToArray(),
                //    Name = firstTable.Table.Name,
                //    Schema = firstTable.Table.Schema,
                //};
                ////Copy Table without Triggers
                //String copyTableSQL = SMORenderer.RenderCopyTable(firstTable.Table.Schema, firstTable.Table.Name, mergedTableSchema.Schema, mergedTableSchema.Name);

                ////Copy Hist Table without Triggers
                //String copyHistTableSQL = SMORenderer.RenderCopyTable(firstHistTable.Schema, firstHistTable.Name, mergedHistTableSchema.Schema, mergedHistTableSchema.Name);




                //String[] allColumns = firstTable.Table.Columns;
                //var StartEndTs = new String[] { updateTime, "NULL" };
                //var RestrictionT1 = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(firstTable.Table.Name, updateTime);

                ////Insert data from old to new
                //String insertFromFirstTable = SMORenderer.RenderInsertFromOneTableToOther(firstTable.Table, mergedTableSchema, RestrictionT1, allColumns, null, StartEndTs);

                //var RestrictionT2 = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(secondTable.Table.Name, updateTime);
                //String insertFromSecondTable = SMORenderer.RenderInsertFromOneTableToOther(secondTable.Table, mergedTableSchema, RestrictionT2, allColumns, null, StartEndTs);

                //String[] dropHistRenameFirst = DropHistTableRenameCurrentToHist(firstTable);
                //String[] dropHistRenameSecond = DropHistTableRenameCurrentToHist(secondTable);

                ////String DropFirstTable = SMORenderer.RenderDropTable(firstTable.Table.Schema, firstTable.Table.Name);
                ////con.ExecuteNonQuerySQL(DropFirstTable);
                ////String DropSecondTable = SMORenderer.RenderDropTable(secondTable.Table.Schema, secondTable.Table.Name);
                ////con.ExecuteNonQuerySQL(DropSecondTable);

                ////;                




                //con.ExecuteNonQuerySQL(insertFromTable);
                // String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(new Table() { TableName = addColumn.TableName, TableSchema = addColumn.Schema }, updateTime);

                String createJoinedMeta = this.MetaManager.GetCreateMetaTableFor(mergedTableSchema.Schema, mergedTableSchema.Name);
                String InsertJoinedMeta = this.MetaManager.GetStartInsertFor(mergedTableSchema.Schema, mergedTableSchema.Name);

                String dropFirstMetaTable = SMORenderer.RenderDropTable(firstTable.MetaTableSchema, firstTable.MetaTableName);
                String dropSecondlMetaTable = SMORenderer.RenderDropTable(secondTable.MetaTableSchema, secondTable.MetaTableName);

                var StartEndTs = new String[] { updateTime, "NULL" };
                String insertHistJoinedTable = SMORenderer.RenderInsertFromOneTableToOther(mergedTableSchema, mergedHistTableSchema, null, mergedTableSchema.Columns, null, StartEndTs);


                //String[] Statements = new String[]
                //{
                //    copyTableSQL,
                //    copyHistTableSQL,
                //    insertFromFirstTable,
                //    insertFromSecondTable
                //}
                //.Concat(dropHistRenameFirst)
                //.Concat(dropHistRenameSecond)
                //.Concat(new String[]
                //{
                //    createJoinedMeta,
                //    InsertJoinedMeta,
                //    dropFirstMetaTable,
                //    dropSecondlMetaTable
                //}).ToArray();
                ////.Concat(dropOriginalHist)

                String[] Statements = new String[]
                {
                    copyTableSQL,
                    copyHistTableSQL,
                    insertFromFirstTable,
                    insertFromSecondTable,
                    DropFirstTable,
                    DropSecondTable,
                    createJoinedMeta,
                    InsertJoinedMeta,
                    dropFirstMetaTable,
                    dropSecondlMetaTable,
                    insertHistJoinedTable
                };


                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { firstTable.ToTable(), secondTable.ToTable() },
                    TablesToUnlock = new Table[] { }
                };
            };


            SeparatedSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 mergeTable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , this.MetaManager);

        }

    }
}
