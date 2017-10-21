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
using QubaDC.Hybrid.SMO;

namespace QubaDC.Hybrid.SMO
{
    class HybridMergeTableHandler
    {
        private SchemaManager schemaManager;

        public HybridMergeTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager mng)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = mng;
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

                String nowVariable = SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");
                var startTsValue = new String[] { nowVariable };

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

                String createJoinedMetaTable = this.MetaManager.GetCreateMetaTableFor(mergedTableSchema.Schema, mergedTableSchema.Name);


                //Insert data from old to new

                //TODO
                String insertFromFirstTable = SMORenderer.RenderInsertFromOneTableToOther(firstTable.Table, mergedTableSchema,null,
                    mergedTableSchema.Columns,
                   null, startTsValue);

                String insertFromSecondTable = SMORenderer.RenderInsertFromOneTableToOther(secondTable.Table, mergedTableSchema, null,
                    mergedTableSchema.Columns,
                   null, startTsValue);

                //con.ExecuteNonQuerySQL(deleteFromFirstTable);
                String DropFirstTable = SMORenderer.RenderDropTable(firstTable.Table.Schema, firstTable.Table.Name);


                //String deleteFromSecondTable = this.SMORenderer.CRUDRenderer.RenderDelete(secondTable.Table.ToTable(), null);
                //con.ExecuteNonQuerySQL(deleteFromSecondTable);
                String DropSecondTable = SMORenderer.RenderDropTable(secondTable.Table.Schema, secondTable.Table.Name);
             

                String DropFirstMeta = SMORenderer.RenderDropTable(firstTable.MetaTableSchema, firstTable.MetaTableName);
                String DropSecondMeta = SMORenderer.RenderDropTable(secondTable.MetaTableSchema, secondTable.MetaTableName);

                String isnertMetadataJoinedMeta = this.MetaManager.GetStartInsertFor(mergedTableSchema.Schema, mergedTableSchema.Name);

                SelectOperation selectCurrentFromBaseTable = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "t1" } },
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updateTime, ColumnName = "ut" } },
                    FromTable = new FromTable() { TableAlias = "t1", TableName = firstTable.Table.Name, TableSchema = firstTable.Table.Schema },
                };
                String selectCurrentWithUT = this.SMORenderer.CRUDHandler.RenderSelectOperation(selectCurrentFromBaseTable);
                String insertIntoFirstHist = this.SMORenderer.CRUDRenderer.RenderInsertSelect(new Table()
                    { TableSchema = firstHistTable.Schema, TableName = firstHistTable.Name },
                        null,
                        selectCurrentWithUT);

                SelectOperation selectSecond = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "t1" } },
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updateTime, ColumnName = "ut" } },
                    FromTable = new FromTable() { TableAlias = "t1", TableName = secondTable.Table.Name, TableSchema = secondTable.Table.Schema },
                };
                String selectSecondCurrent = this.SMORenderer.CRUDHandler.RenderSelectOperation(selectSecond);
                String insertIntoSecondHist = this.SMORenderer.CRUDRenderer.RenderInsertSelect(new Table()
                { TableSchema = secondHistTable.Schema, TableName = secondHistTable.Name },
                    null,
                    selectSecondCurrent);

                String[] Statements =
                                    new String[]
                                        {
                                            copyTableSQL,
                                            copyHistTableSQL,
                                            createJoinedMetaTable,
                                            insertFromFirstTable,
                                            insertFromSecondTable,
                                            insertIntoFirstHist,
                                            insertIntoSecondHist,
                                            DropFirstTable,
                                            DropSecondTable,
                                            DropFirstMeta,
                                            DropSecondMeta,
                                            isnertMetadataJoinedMeta,
                                        };


                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { firstTable.ToTable(), secondTable.ToTable() },
                    TablesToUnlock = new Table[] { }
                };
            };


            HybridSMOExecuter.Execute(
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
