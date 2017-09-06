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
                     MySQLDialectHelper.RenderDateTime(t),
                     "null"
           }).ToArray();
           String insertToTable = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);
           String insertToGlobalUpdate = this.CRUDRenderer.RenderInsert(this.timeManager.GetTable(),
new String[] { "Operation", "Timestamp" },
new String[] { String.Format("'insert on {0}'", this.timeManager.GetTable().TableName), MySQLDialectHelper.RenderDateTime(t) }
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
            NewMethod(renderStaetement, lockTables);

        }

        private void NewMethod(Func<String[]> RenderStatements,String[] locktables)
        {
            this.DataConnection.AquiereOpenConnection(con =>
            {
                String[] lockTableStatements = this.CRUDRenderer.RenderLockTables(locktables);
                try
                {
                    foreach (var setupSql in lockTableStatements)
                        this.DataConnection.ExecuteNonQuerySQL(setupSql, con);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not aquire locks for:" + String.Join(",", locktables), e);
                }

            try
            {
                String[] statements = RenderStatements();
                String[] success = this.CRUDRenderer.RenderCommitAndUnlock();
                this.DataConnection.ExecuteNonQuerySQL(statements[0], con);
                this.DataConnection.ExecuteNonQuerySQL(statements[1], con);
                this.DataConnection.ExecuteNonQuerySQL(success[0], con);
                this.DataConnection.ExecuteNonQuerySQL(success[1], con);

                }
                catch (Exception e)
                {
                    String[] rollbackAndUnlock = this.CRUDRenderer.RenderRollBackAndUnlock();
                    this.DataConnection.ExecuteNonQuerySQL(rollbackAndUnlock[0]);
                    this.DataConnection.ExecuteNonQuerySQL(rollbackAndUnlock[1]);
                    throw new InvalidOperationException("Got exception after Table Locks, rolled back and unlocked", e);
                }

            });
        }

       
    }
}
