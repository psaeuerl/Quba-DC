using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using QubaDC.Restrictions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    class IntegratedDeleteHandler
    {
        public IntegratedDeleteHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, TableMetadataManager timeManager)
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
            Func<String[]> renderStaetement = () =>
            {
                String nowVariable = "deleteTime";
                String setNowVariableStmt = this.CRUDRenderer.RenderNowToVariable(nowVariable);

                //Only Set ENDTS of those that have no ENDTS set == currently active
                OperatorRestriction endTSNull = new OperatorRestriction()
                {
                    LHS = new ColumnOperand()
                    {
                        Column = new ColumnReference()
                        {
                            ColumnName = IntegratedConstants.EndTS,
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
                    ColumnNames = new String[] { IntegratedConstants.EndTS },
                    ValueLiterals = new String[] { this.CRUDRenderer.GetSQLVariable(nowVariable) },
                    Restriction = a,
                    Table = deleteOperation.Table
                };


                String updateOperation = this.CRUDRenderer.RenderUpdate(uo.Table, uo.ColumnNames, uo.ValueLiterals, uo.Restriction);
                String updateLastUpdate = this.metaManager.GetSetLastUpdateStatement(deleteOperation.Table, this.CRUDRenderer.GetSQLVariable(nowVariable));

                return new String[]
                {
                            setNowVariableStmt,
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
