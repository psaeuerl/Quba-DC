using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Hybrid.CRUD
{
    public class HybridInsertHandler
    {
        public HybridInsertHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, TableMetadataManager metaManager)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
            this.MetaManager = metaManager;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SchemaManager SchemaManager { get; private set; }

        internal void HandleInsert(InsertOperation insertOperation)
        {            
            //Actually, just insert the statement


            Func<String[]> renderStaetement = () =>
            {
                String insertTimeVariable = "insertTime";
                String setInsertTime = this.CRUDRenderer.RenderNowToVariable(insertTimeVariable);

                insertOperation.ColumnNames = insertOperation.ColumnNames.Concat(new String[] { HybridConstants.StartTS}).ToArray();
                insertOperation.ValueLiterals = insertOperation.ValueLiterals.Concat(new String[]
                {
                                    this.CRUDRenderer.GetSQLVariable(insertTimeVariable),
                }).ToArray();
                String insertToTable = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);
                String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(insertOperation.InsertTable, this.CRUDRenderer.GetSQLVariable(insertTimeVariable));

                return new String[]
                {
                                setInsertTime,
                                insertToTable,
                                updateLastUpdate
                };
            };
            SchemaInfo currentSchemaInfo = this.SchemaManager.GetCurrentSchema();
            TableSchema hist = currentSchemaInfo.Schema.FindHistTable(insertOperation.InsertTable);

            String insertTable = this.CRUDRenderer.PrepareTable(insertOperation.InsertTable);
            Table metaTable = this.MetaManager.GetMetaTableFor(insertOperation.InsertTable.TableSchema, insertOperation.InsertTable.TableName);


            String metaTableName = this.CRUDRenderer.PrepareTable(metaTable);
           // String histTable = this.CRUDRenderer.PrepareTable(hist.ToTable());
            String[] lockTables = new string[]
                {
                   insertOperation.InsertTable.TableSchema+"."+insertOperation.InsertTable.TableName,
                   metaTableName,
                   //histTable,
                   SchemaManager.GetTableName()
                };
            Boolean[] lockWrite = new bool[]
            {
                true,
                true,
                false
            };
            Action<String> x = (s) => System.Diagnostics.Debug.WriteLine(s);
            //x = HybridCRUDExecuter.DefLog;
            HybridCRUDExecuter.ExecuteStatementsOnLockedTables(renderStaetement, lockTables, lockWrite, this.DataConnection, this.CRUDRenderer, this.SchemaManager, currentSchemaInfo, insertOperation.InsertTable, this.MetaManager,x );
        }
    
    }
}
