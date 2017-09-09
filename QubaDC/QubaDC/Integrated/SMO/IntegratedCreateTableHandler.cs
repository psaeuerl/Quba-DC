using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;

namespace QubaDC.Integrated.SMO
{
    public class IntegratedCreateTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedCreateTableHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer, GlobalUpdateTimeManager manager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.GlobalUpdateTimemanager = manager;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }
        public GlobalUpdateTimeManager GlobalUpdateTimemanager { get; private set; }

        internal void Handle(CreateTable createTable)
        {
            var con = (MySQLDataConnection)DataConnection;
            //Actual Code
            ColumnDefinition[] histColumns = IntegratedConstants.GetHistoryTableColumns();

            CreateTable newCt = JsonSerializer.CopyItem<CreateTable>(createTable);
            newCt.Columns = createTable.Columns.Union(histColumns).ToArray();
            String createBaseTable = SMORenderer.RenderCreateTable(newCt);

            SchemaInfo xy = this.schemaManager.GetCurrentSchema();
            Schema x = xy.Schema;
            if (xy.ID == null)
            {
                x = new Schema();
                xy.ID = 0;
            }

            ////Create History Table
            CreateTable ctHistTable = CreateHistTable(createTable, xy);
            String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);

            String ensureSchemaVersion = schemaManager.RenderEnsureSchema(xy);

            //Manage Schema Statement
            x.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema());


            String ensureSchema = schemaManager.RenderEnsureSchema(xy);
            String insertNewSchema = this.schemaManager.GetInsertSchemaStatement(x, createTable);

            String[] PreLockingStatements = new String[] { createBaseTable, createHistTable };
            String[] AfterLockingStatemnts = new String[] { ensureSchema, insertNewSchema };
            String[] tablesToLock = new String[] { this.schemaManager.GetTableName(), this.GlobalUpdateTimemanager.GetTableName() ,
                                                             this.SMORenderer.CRUDRenderer.PrepareTable(createTable.ToTable()),
                                                             this.SMORenderer.CRUDRenderer.PrepareTable(ctHistTable.ToTable()),
                                                          };
            IntegratedSMOExecuter.Execute(SMORenderer, con, PreLockingStatements, AfterLockingStatemnts, tablesToLock);

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
