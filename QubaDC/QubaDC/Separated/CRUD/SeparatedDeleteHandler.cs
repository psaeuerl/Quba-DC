using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using QubaDC.Restrictions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Separated.CRUD
{
    class SeparatedDeleteHandler
    {
        public SeparatedDeleteHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, TableMetadataManager timeManager)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
            this.metaManager = timeManager;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public SchemaManager SchemaManager { get; private set; }
        public TableMetadataManager metaManager { get; private set; }

        internal void HandleDelete(DeleteOperation deleteOperation)
        {
            //Actually, just insert the statement
            //String insertIntoBaseTable = this.CRUDRenderer.RenderDelete(deleteOperation.Table, deleteOperation.Restriction);
            //this.DataConnection.ExecuteQuery(insertIntoBaseTable);

            SchemaInfo currentSchemaInfo = this.SchemaManager.GetCurrentSchema();
            TableSchema hist = currentSchemaInfo.Schema.FindHistTable(deleteOperation.Table);

            String insertTable = this.CRUDRenderer.PrepareTable(deleteOperation.Table);
            Table metaTable = metaManager.GetMetaTableFor(deleteOperation.Table.TableSchema, deleteOperation.Table.TableName);
            String metaTableName = this.CRUDRenderer.PrepareTable(metaTable);
            String histTable = this.CRUDRenderer.PrepareTable(hist.ToTable());

            Func<String[]> renderStaetement = () =>
            {
                String nowVariable = "deleteTime";
                String setNowVariableStmt = this.CRUDRenderer.RenderNowToVariable(nowVariable);


                //BUILD Update at hist
                //Only Set ENDTS of those that have no ENDTS set == currently active
                OperatorRestriction endTSNull = new OperatorRestriction()
                {
                    LHS = new ColumnOperand()
                    {
                        Column = new ColumnReference()
                        {
                            ColumnName = SeparatedConstants.EndTS,
                            TableReference = deleteOperation.Table.TableName
                        }
                    },
                    Op = RestrictionOperator.IS
   ,
                    RHS = new LiteralOperand()
                    {
                        Literal = "NULL"
                    }
                };
                AndRestriction a = new QubaDC.AndRestriction() { Restrictions = new Restriction[] { endTSNull, deleteOperation.Restriction } };

                UpdateOperation uo = new UpdateOperation()
                {
                    ColumnNames = new String[] { SeparatedConstants.EndTS },
                    ValueLiterals = new String[] { this.CRUDRenderer.GetSQLVariable(nowVariable) },
                    Restriction = a,
                    Table = hist.ToTable()
                };
                String updateHistTableSetENDTS = this.CRUDRenderer.RenderUpdate(uo.Table, uo.ColumnNames, uo.ValueLiterals, uo.Restriction);
                updateHistTableSetENDTS = updateHistTableSetENDTS.Replace("`" + deleteOperation.Table.TableName + "`", "`" + hist.Name + "`");


                String deleteOriginal = this.CRUDRenderer.RenderDelete(deleteOperation.Table, deleteOperation.Restriction);


               
                String updateLastUpdate = this.metaManager.GetSetLastUpdateStatement(deleteOperation.Table, this.CRUDRenderer.GetSQLVariable(nowVariable));

                return new String[]
                {
                            setNowVariableStmt,
                            deleteOriginal,
                            updateHistTableSetENDTS,
                            updateLastUpdate
                };
            };
         
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
            SeparatedCRUDExecuter.ExecuteStatementsOnLockedTables(renderStaetement, lockTables, lockWrite, this.DataConnection, this.CRUDRenderer, this.SchemaManager, currentSchemaInfo, deleteOperation.Table, metaManager
                , (s) => System.Diagnostics.Debug.WriteLine(s));
        }
    }
}
