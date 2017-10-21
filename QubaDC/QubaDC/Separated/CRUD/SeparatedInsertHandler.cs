using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Separated.CRUD
{
    class SeparatedInsertHandler
    {
        public SeparatedInsertHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, TableMetadataManager timeManager)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
            this.metaManager = timeManager;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager metaManager { get; private set; }
        public SchemaManager SchemaManager { get; private set; }

        internal void HandleInsert(InsertOperation insertOperation)
        {
            ////Actually, just insert the statement
            //String insertIntoBaseTable = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);
            //this.DataConnection.ExecuteQuery(insertIntoBaseTable);
            SchemaInfo currentSchemaInfo = this.SchemaManager.GetCurrentSchema();
            TableSchema hist = currentSchemaInfo.Schema.FindHistTable(insertOperation.InsertTable);

            String insertTable = this.CRUDRenderer.PrepareTable(insertOperation.InsertTable);
            Table metaTable = this.metaManager.GetMetaTableFor(insertOperation.InsertTable.TableSchema, insertOperation.InsertTable.TableName);
            String metaTableName = this.CRUDRenderer.PrepareTable(metaTable);
            String histTable = this.CRUDRenderer.PrepareTable(hist.ToTable());

            Func<String[]> renderStaetement = () =>
            {
                String insertTimeVariable = "insertTime";
                String setInsertTime = this.CRUDRenderer.RenderNowToVariable(insertTimeVariable);
                String insertVariable = this.CRUDRenderer.GetSQLVariable(insertTimeVariable);
                String insertToTable = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);


                String[] columnsOther = insertOperation.ColumnNames.Concat(SeparatedConstants.GetHistoryTableColumns().Select(y => y.ColumName)).ToArray();
                String[] literals = insertOperation.ValueLiterals.Concat(new String[] { insertVariable, "null" }).ToArray();
                String insertToSeparated = this.CRUDRenderer.RenderInsert(hist.ToTable(), columnsOther, literals);

                String updateLastUpdate = this.metaManager.GetSetLastUpdateStatement(insertOperation.InsertTable, this.CRUDRenderer.GetSQLVariable(insertTimeVariable));

                return new String[]
                {
                                setInsertTime,
                                insertToTable,
                                insertToSeparated,
                                updateLastUpdate
                };
            };      

            String[] lockTables = new string[]
                {
                   insertOperation.InsertTable.TableSchema+"."+insertOperation.InsertTable.TableName,
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
            Action<String> x = (y) => { System.Diagnostics.Debug.WriteLine(y); };
            SeparatedCRUDExecuter.ExecuteStatementsOnLockedTables(renderStaetement, lockTables, lockWrite, this.DataConnection, this.CRUDRenderer, this.SchemaManager, currentSchemaInfo, insertOperation.InsertTable, metaManager, x);
        }
    }
}
