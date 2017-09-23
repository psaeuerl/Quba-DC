using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;
using QubaDC.Hybrid.SMO;
using QubaDC.CRUD;
using QubaDC.Hybrid;

namespace QubaDC.Separated.SMO
{
    class HybridCopyTableHandler
    {
        private SchemaManager schemaManager;

        public HybridCopyTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager meta)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = meta;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(CopyTable copyTable)
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


                TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(copyTable.Schema, copyTable.TableName);
                TableSchema originalHistTable = currentSchemaInfo.Schema.FindHistTable(originalTable.Table.ToTable());

                var copiedTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns,
                    Name = copyTable.CopiedTableName,
                    Schema = copyTable.CopiedSchema,
                    ColumnDefinitions = originalTable.Table.ColumnDefinitions,
                };
                var copiedHistSchema = new TableSchema()
                {
                    Columns = originalHistTable.Columns,
                    Name = copyTable.CopiedTableName + "_" + currentSchemaInfo.ID,
                    Schema = copyTable.CopiedSchema,
                    ColumnDefinitions = originalHistTable.ColumnDefinitions
                };
                Table copiedMeta = this.MetaManager.GetMetaTableFor(copiedTableSchema);
                currentSchema.AddTable(copiedTableSchema, copiedHistSchema, copiedMeta);

                //Copy Table without Triggers
                String copyTableSQL = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, copiedTableSchema.Schema, copiedTableSchema.Name);

                //Copy Hist Table without Triggers
                String copyHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, copiedHistSchema.Schema, copiedHistSchema.Name);        

                SelectOperation s = new SelectOperation()
                {
                    Columns = originalTable.Table.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                    LiteralColumns = new LiteralColumn[] { 
                                                          new LiteralColumn() {ColumnLiteral =  updateTime, ColumnName = "ut" } },
                    FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Table.Name , TableSchema = originalTable.Table.Schema }
                };
                String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);
                TableSchema copiedWithStartTS = new TableSchema()
                {
                    Columns = copiedTableSchema.Columns.Concat(new String[] { HybridConstants.StartTS, }).ToArray(),
                    Name = copiedTableSchema.Name,
                    Schema = copiedTableSchema.Schema
                };
                String insertFromTable = SMORenderer.RenderInsertToTableFromSelect(copiedWithStartTS, select);


                String createFirstMetaTable = this.MetaManager.GetCreateMetaTableFor(copiedTableSchema.Schema, copiedTableSchema.Name);
                String insertMetadataFirstTable = this.MetaManager.GetStartInsertFor(copiedTableSchema.Schema, copiedTableSchema.Name);

                String[] Statements = new String[]
                {
                    copyTableSQL,
                    copyHistTableSQL,
                    createFirstMetaTable,
                    insertFromTable,
                    insertMetadataFirstTable
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
                 copyTable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , this.MetaManager);

        }

    }
}
