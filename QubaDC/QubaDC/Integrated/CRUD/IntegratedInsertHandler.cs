using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    class IntegratedInsertHandler
    {
        public IntegratedInsertHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, GlobalUpdateTimeManager timeManager)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
            this.timeManager = timeManager;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public SchemaManager SchemaManager { get; private set; }
        public GlobalUpdateTimeManager timeManager { get; private set; }

        internal void HandleInsert(InsertOperation insertOperation)
        {
            Func<String[]> renderStaetement = () =>
       {
           //We now have our locks
           DateTime t = System.DateTime.Now;
           insertOperation.ColumnNames = insertOperation.ColumnNames.Concat(new String[] { IntegratedConstants.StartTS, IntegratedConstants.EndTS }).ToArray();
           insertOperation.ValueLiterals = insertOperation.ValueLiterals.Concat(new String[]
           {
                    this.CRUDRenderer.renderDateTime(t),
                     "null"
           }).ToArray();
           String insertToTable = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);
           String insertToGlobalUpdate = this.CRUDRenderer.RenderInsert(this.timeManager.GetTable(),
new String[] { "Operation", "Timestamp" },
new String[] { String.Format("'insert on {0}'", this.timeManager.GetTable().TableName), this.CRUDRenderer.renderDateTime(t) }
);
           return new String[]
           {
                insertToTable,
                insertToGlobalUpdate
           };
       };
            String[] lockTables = new string[]
                {
                   insertOperation.InsertTable.TableSchema+"."+insertOperation.InsertTable.TableName,
                   timeManager.GetTableName()
                };
            IntegratedCRUDExecuter.ExecuteStatementsOnLockedTables(renderStaetement, lockTables, this.DataConnection, this.CRUDRenderer);
        }

      

       
    }
}
