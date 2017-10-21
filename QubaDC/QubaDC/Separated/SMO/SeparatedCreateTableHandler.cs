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
    class SeparatedCreateTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedCreateTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager metaManager)
        {
            this.con = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = metaManager;
        }

        public DataConnection con { get; private set; }
        public SMORenderer SMORenderer { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }

        internal void Handle(CreateTable createTable)
        {
            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                String createBaseTable = SMORenderer.RenderCreateTable(createTable);
                ////Create History Table
              
                Schema x = currentSchemaInfo.Schema;
                if (currentSchemaInfo.ID == null)
                {
                    x = new Schema();
                    currentSchemaInfo.ID = 0;
                }

                CreateTable ctHistTable = CreateHistTable(createTable, currentSchemaInfo);

                String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);


                //ColumnDefinition[] histColumns = IntegratedConstants.GetHistoryTableColumns();
                //CreateTable newCt = JsonSerializer.CopyItem<CreateTable>(createTable);
                //newCt.Columns = createTable.Columns.Union(histColumns).ToArray();
                //String createBaseTable = SMORenderer.RenderCreateTable(newCt);

                //CreateTable ctHistTable = CreateHistTable(createTable, currentSchemaInfo);
                //String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);
                String createMetaTable = MetaManager.GetCreateMetaTableFor(createTable.Schema, createTable.TableName);
                Table metaTable = MetaManager.GetMetaTableFor(createTable.Schema, createTable.TableName);
                //Manage Schema Statement
                currentSchemaInfo.Schema.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema(), metaTable);
                //String updateSchema = this.schemaManager.GetInsertSchemaStatement(x, createTable);


                String baseInsert = MetaManager.GetStartInsertFor(createTable.Schema, createTable.TableName); ;

                String[] Statements = new String[]
                {
                    createBaseTable,
                    createHistTable,
                    createMetaTable,
                    baseInsert
                };

                return new UpdateSchema()
                {
                    newSchema = currentSchemaInfo.Schema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { },
                    TablesToUnlock = new Table[] { }
                };
            };


            SeparatedSMOExecuter.Execute(
                this.SMORenderer,
                this.con,
                 this.schemaManager,
                 createTable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , MetaManager);
        }

        private static CreateTable CreateHistTable(CreateTable createTable, SchemaInfo xy)
        {
            List<ColumnDefinition> columndefinitions = new List<ColumnDefinition>();
            columndefinitions.AddRange(createTable.Columns);
            columndefinitions.AddRange(SeparatedConstants.GetHistoryTableColumns());
            CreateTable ctHistTable = new CreateTable()
            {
                Columns = columndefinitions.ToArray(),
                Schema = createTable.Schema,
                TableName = createTable.TableName + "_" + xy.ID
            };
            return ctHistTable;
        }
    }
}
