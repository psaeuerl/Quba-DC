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

namespace QubaDC.Separated.SMO
{
    public class HybridDropTableHandler
    {
        private SchemaManager schemaManager;

        public HybridDropTableHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer, TableMetadataManager manager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = manager;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(DropTable dropTable)
        {
            //What to do here?
            //0.) Remove all Columns so they get pushed into the 
            //a.) Drop Table
            //b.) Schemamanager => RemoveTable            

        Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
        {
            String insertTimeVariable = "updateTime";
            String updateTimeVariable = this.SMORenderer.CRUDRenderer.GetSQLVariable(insertTimeVariable);

            String updateTime = this.SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");
            Schema currentSchema = currentSchemaInfo.Schema;


            TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(dropTable.Schema, dropTable.TableName);
            TableSchema originalHistTable = currentSchemaInfo.Schema.FindHistTable(originalTable.Table.ToTable());


            String deleteRows = SMORenderer.CRUDRenderer.RenderDelete(new Table()
            {
                TableName = dropTable.TableName,
                TableSchema = dropTable.Schema
            }, null);

            String dropTableSql = SMORenderer.RenderDropTable(dropTable.Schema, dropTable.TableName);            
            Table oldTable = new Table() { TableSchema = dropTable.Schema, TableName = dropTable.TableName };

            currentSchema.RemoveTable(oldTable);



            //Insert data to hist
            SelectOperation selectCurrentFromBaseTable = new SelectOperation()
            {
                Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "t1" } },
                LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updateTimeVariable, ColumnName = "ut" } },
                FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Table.Name , TableSchema = originalTable.Table.Schema },
            };
            String selectCurrentWithUT = this.SMORenderer.CRUDHandler.RenderSelectOperation(selectCurrentFromBaseTable);
            String isnertIntoHist = this.SMORenderer.CRUDRenderer.RenderInsertSelect(new Table()
            { TableSchema = originalHistTable.Schema, TableName = originalHistTable.Name },
                null,
                selectCurrentWithUT);
            String insertFromTableToHist = SMORenderer.RenderInsertToTableFromSelect(originalHistTable, selectCurrentWithUT);

            String dropOriginalMetaTable = SMORenderer.RenderDropTable(originalTable.MetaTableSchema, originalTable.MetaTableName);

            
            String[] Statements = new String[]
            {
                insertFromTableToHist,
                dropTableSql,
                dropOriginalMetaTable
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
                 dropTable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , this.MetaManager);
            }

    }
}
