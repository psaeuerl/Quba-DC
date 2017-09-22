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

namespace QubaDC.Hybrid.SMO
{
    public class HybridCreateTableHandler
    {
        private SchemaManager schemaManager;

        public HybridCreateTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager metaManager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = metaManager;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }

        internal void Handle(CreateTable createTable)
        {
            //Guard.StateTrue(createTable.PrimaryKey.Length > 0, "Primary Key Requiered");

            //var con = (MySQLDataConnection)DataConnection;
            //;
            //con.DoTransaction((transaction, c) =>
            //{
            //    ////What to do?
            //    ////Create Table normal
            //    ////Create Table Hist
            //    ////Create Trigger on normal
            //    ColumnDefinition startTs = HybridConstants.GetStartColumn();

            //    CreateTable newCt = JsonSerializer.CopyItem<CreateTable>(createTable);
            //    newCt.Columns = createTable.Columns.Union(new ColumnDefinition[] { HybridConstants.GetStartColumn() }).ToArray();
            //    String createBaseTable = SMORenderer.RenderCreateTable(newCt);
            //    ;
            //    ////Create History Table
            //    SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
            //    Schema x = xy.Schema;
            //    if (xy.ID == null)
            //    {
            //        x = new Schema();
            //        xy.ID = 0;
            //    }

            //    CreateTable ctHistTable = CreateHistTable(newCt, xy);

            //    String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);

            //    //Manage Schema Statement
            //    x.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema());
            //    //String updateSchema = this.schemaManager.GetInsertSchemaStatement(x, createTable);

            //    //Add tables
            //    con.ExecuteNonQuerySQL(createBaseTable, c);
            //    con.ExecuteNonQuerySQL(createHistTable, c);

            //    ////INsert Trigger 
            //    String trigger = SMORenderer.RenderCreateInsertTrigger(createTable.ToTableSchema(), ctHistTable.ToTableSchema());
            //    ////Delete Trigger
            //    String deleteTrigger = SMORenderer.RenderCreateDeleteTrigger(createTable.ToTableSchema(), ctHistTable.ToTableSchema());
            //    ////Update Trigger
            //    String UpdateTrigger = SMORenderer.RenderCreateUpdateTrigger(createTable.ToTableSchema(), ctHistTable.ToTableSchema());

            //    ////Add Trigger
            //    con.ExecuteSQLScript(trigger, c);
            //    con.ExecuteSQLScript(deleteTrigger, c);
            //    con.ExecuteSQLScript(UpdateTrigger, c);

            //    ////Store Schema
            //    this.schemaManager.StoreSchema(x, createTable, con, c);
            //    transaction.Commit();
            //});

            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                ColumnDefinition startTs = HybridConstants.GetStartColumn();

                CreateTable newCt = JsonSerializer.CopyItem<CreateTable>(createTable);
                newCt.Columns = createTable.Columns.Union(new ColumnDefinition[] { HybridConstants.GetStartColumn() }).ToArray();
                String createBaseTable = SMORenderer.RenderCreateTable(newCt);
                ;
                ////Create History Table
                Schema x = currentSchemaInfo.Schema;
                if (currentSchemaInfo.ID == null)
                {
                    x = new Schema();
                    currentSchemaInfo.ID = 0;
                }

                CreateTable ctHistTable = CreateHistTable(newCt, currentSchemaInfo);

                String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);

                String createMetaTable = MetaManager.GetCreateMetaTableFor(newCt.Schema, newCt.TableName);
                Table metaTable = MetaManager.GetMetaTableFor(newCt.Schema, newCt.TableName);
                //Manage Schema Statement
                currentSchemaInfo.Schema.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema(), metaTable);
                //Manage Schema Statement
                //x.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema());
                //String updateSchema = this.schemaManager.GetInsertSchemaStatement(x, createTable);

                //Add tables
                //con.ExecuteNonQuerySQL(createBaseTable, c);
                //con.ExecuteNonQuerySQL(createHistTable, c);


                //ColumnDefinition[] histColumns = IntegratedConstants.GetHistoryTableColumns();
                //CreateTable newCt = JsonSerializer.CopyItem<CreateTable>(createTable);
                //newCt.Columns = createTable.Columns.Union(histColumns).ToArray();
                //String createBaseTable = SMORenderer.RenderCreateTable(newCt);

                //CreateTable ctHistTable = CreateHistTable(createTable, currentSchemaInfo);
                //String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);

                //String updateSchema = this.schemaManager.GetInsertSchemaStatement(x, createTable);


                String baseInsert = MetaManager.GetStartInsertFor(newCt.Schema, newCt.TableName); ;

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


            HybridSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
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
            columndefinitions.Add(HybridConstants.GetEndColumn());
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
