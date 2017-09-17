using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    class IntegratedDeleteHandler
    {
        public IntegratedDeleteHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, TableLastUpdateManager timeManager)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
            this.metaManager = timeManager;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public SchemaManager SchemaManager { get; private set; }
        public TableLastUpdateManager metaManager { get; private set; }

        internal void HandleDelete(DeleteOperation deleteOperation)
        {

            ////Actually, just insert the statement
            //String updateTableSetEndTs = this.CRUDRenderer.RenderUpdate(uo.Table, uo.ColumnNames, uo.ValueLiterals, uo.Restriction);
            //this.DataConnection.ExecuteQuery(updateTableSetEndTs);

            //    Func<String[]> renderStatement = () =>
            //    {
            //        DateTime t = System.DateTime.Now;
            //UpdateOperation uo = new UpdateOperation()
            //{
            //    ColumnNames = new String[] { IntegratedConstants.EndTS },
            //    ValueLiterals = new String[] { this.CRUDRenderer.renderDateTime(t) },
            //    Restriction = deleteOperation.Restriction,
            //    Table = deleteOperation.Table
            //};
            //        String updateOperation = this.CRUDRenderer.RenderUpdate(uo.Table, uo.ColumnNames, uo.ValueLiterals, uo.Restriction);

            //        String insertToGlobalUpdate = this.CRUDRenderer.RenderInsert(this.timeManager.GetTable(),
            //new String[] { "Operation", "Timestamp" },
            //new String[] { String.Format("'delete on {0}'", this.timeManager.GetTable().TableName), this.CRUDRenderer.renderDateTime(t) }
            //);

            //        return new String[]
            //        {
            //            updateOperation,
            //            insertToGlobalUpdate
            //        };
            //    };
            //    String[] lockTables = new string[]
            //    {
            //                   deleteOperation.Table.TableSchema+"."+deleteOperation.Table.TableName,
            //                   timeManager.GetTableName()
            //    };
            //        //IntegratedCRUDExecuter.ExecuteStatementsOnLockedTables(renderStatement, lockTables, this.DataConnection, this.CRUDRenderer);

            Func<String[]> renderStaetement = () =>
            {
                String insertTimeVariable = "insertTime";
                String setInsertTime = this.CRUDRenderer.RenderNowToVariable(insertTimeVariable);

                UpdateOperation uo = new UpdateOperation()
                {
                    ColumnNames = new String[] { IntegratedConstants.EndTS },
                    ValueLiterals = new String[] { this.CRUDRenderer.GetSQLVariable(insertTimeVariable) },
                    Restriction = deleteOperation.Restriction,
                    Table = deleteOperation.Table
                };


                String updateOperation = this.CRUDRenderer.RenderUpdate(uo.Table, uo.ColumnNames, uo.ValueLiterals, uo.Restriction);
                String updateLastUpdate = this.metaManager.GetSetLastUpdateStatement(deleteOperation.Table, this.CRUDRenderer.GetSQLVariable(insertTimeVariable));

                return new String[]
                {
                            setInsertTime,
                            updateOperation,
                            updateLastUpdate
                };
            };
            SchemaInfo currentSchemaInfo = this.SchemaManager.GetCurrentSchema();
            TableSchema hist = currentSchemaInfo.Schema.FindHistTable(deleteOperation.Table);

            String insertTable = this.CRUDRenderer.PrepareTable(deleteOperation.Table);
            Table metaTable = metaManager.GetMetaTableFor(deleteOperation.Table.TableSchema, deleteOperation.Table.TableName);
            String metaTableName = this.CRUDRenderer.PrepareTable(metaTable);
            String histTable = this.CRUDRenderer.PrepareTable(hist.ToTable());
            String[] lockTables = new string[]
                {
                   deleteOperation.Table.TableSchema+"."+deleteOperation.Table.TableName,
                   metaTableName,
                   histTable,
                   SchemaManager.GetTableName()
                };
            Boolean[] lockWrite = new bool[]
            {
                true,
                true,
                true,
                false
            };
            IntegratedCRUDExecuter.ExecuteStatementsOnLockedTables(renderStaetement, lockTables, lockWrite, this.DataConnection, this.CRUDRenderer, this.SchemaManager, currentSchemaInfo, deleteOperation.Table, metaManager
                , (s) => System.Diagnostics.Debug.WriteLine(s));

        }
    }
}
