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

namespace QubaDC.Integrated.SMO
{
    public class IntegratedCreateTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedCreateTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableLastUpdateManager timeManager)
        {
            this.con = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.TimeManager = timeManager;
        }

        public DataConnection con { get; private set; }
        public SMORenderer SMORenderer { get; private set; }
        public TableLastUpdateManager TimeManager { get; private set; }

        internal void Handle(CreateTable createTable)
        {
            ColumnDefinition[] histColumns = IntegratedConstants.GetHistoryTableColumns();

            CreateTable newCt = JsonSerializer.CopyItem<CreateTable>(createTable);
            newCt.Columns = createTable.Columns.Union(histColumns).ToArray();
            String createBaseTable = SMORenderer.RenderCreateTable(newCt);

            String[] beforeLockStatements = new String[]
            {
                "SET autocommit=0;",
                "SELECT GET_LOCK('SMO UPDATES',10);",
                "SET @updateTime = NOW(3); "
            };

            String[] cleanup = new String[]
            {
                "COMMIT;",
                "SELECT RELEASE_LOCK('SMO UPDATES');"
            };
            con.AquiereOpenConnection(oc =>
            {
                foreach (var stmt in beforeLockStatements)
                    con.ExecuteSQLScript(stmt, oc);

                var res= con.ExecuteQuery("SELECT @updateTime;");
                //Aquiered Lock
                //Get Schema
                SchemaInfo currentSchemaInfo = this.schemaManager.GetCurrentSchema(oc);
                Schema currentSchema = currentSchemaInfo.Schema;
                if (currentSchemaInfo.ID == null)
                {
                    currentSchema = new Schema();
                    currentSchemaInfo.ID = 0;
                }


                //Actual CreateTableCode
                CreateTable ctHistTable = CreateHistTable(createTable, currentSchemaInfo);
                String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);
                String createMetaTable = TimeManager.GetCreateMetaTableFor(newCt.Schema, newCt.TableName);
                Table metaTable = TimeManager.GetMetaTableFor(newCt.Schema, newCt.TableName);
                //Manage Schema Statement
                currentSchema.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema(), metaTable);
                //String updateSchema = this.schemaManager.GetInsertSchemaStatement(x, createTable);

                //Add tables
                con.ExecuteNonQuerySQL(createBaseTable, oc);
                con.ExecuteNonQuerySQL(createHistTable, oc);
                con.ExecuteNonQuerySQL(createMetaTable, oc);

                this.schemaManager.StoreSchema(currentSchema, createTable, con, oc,true);

                String baseInsert= TimeManager.GetStartInsertFor(newCt.Schema, newCt.TableName);
                con.ExecuteNonQuerySQL(baseInsert, oc);

                foreach (var stmt in cleanup)
                    con.ExecuteSQLScript(stmt, oc);

            });  
        }

        private static CreateTable CreateHistTable(CreateTable createTable, SchemaInfo xy)
        {

            CreateTable ctHistTable = new CreateTable()
            {
                Columns = createTable.Columns.ToArray(),
                Schema = createTable.Schema,
                TableName = createTable.TableName + "_" + xy.ID
            };
            return ctHistTable;
        }
    }
}
