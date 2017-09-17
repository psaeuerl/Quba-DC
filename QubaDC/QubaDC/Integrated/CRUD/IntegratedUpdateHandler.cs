using QubaDC.CRUD;
using QubaDC.Restrictions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    class IntegratedUpdateHandler
    {
        public IntegratedUpdateHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, TableLastUpdateManager timeManager)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
            this.GlobalUpdateTImeManager = timeManager;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public TableLastUpdateManager GlobalUpdateTImeManager { get; private set; }
        public SchemaManager SchemaManager { get; private set; }

        internal void HandleUpdate(UpdateOperation updateOperation)
        {
            Func<String[]> renderStaetement = () =>
            {
                OperatorRestriction startTsLower = new OperatorRestriction()
                {
                    LHS = new ColumnOperand()
                    {
                        Column = new ColumnReference()
                        {
                            ColumnName = IntegratedConstants.StartTS,
                            TableReference = updateOperation.Table.TableName
                        }
                    },
                    Op = RestrictionOperator.LT
         ,
                    RHS = new LiteralOperand()
                    {
                        Literal = this.CRUDRenderer.GetSQLVariable("ct")
                    }
                };

                OperatorRestriction endTSNull = new OperatorRestriction()
                {
                    LHS = new ColumnOperand()
                    {
                        Column = new ColumnReference()
                        {
                            ColumnName = IntegratedConstants.EndTS,
                            TableReference = updateOperation.Table.TableName
                        }
                    },
                    Op = RestrictionOperator.IS
,
                    RHS = new LiteralOperand()
                    {
                        Literal = "NULL"
                    }
                };


                var selectAndRestriciton = new AndRestriction();
                selectAndRestriciton.Restrictions = new Restriction[] { startTsLower, endTSNull, updateOperation.Restriction };


                SelectOperation selectCurrentFromBaseTable = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = updateOperation.Table.TableName } },
                    FromTable = new FromTable()
                    {
                        TableAlias = updateOperation.Table.TableName,
                        TableName = updateOperation.Table.TableName,
                        TableSchema = updateOperation.Table.TableSchema
                    },
                    Restriction = selectAndRestriciton
                };
                IntegratedSelectHandler selectHandler = new IntegratedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
                String select = selectHandler.HandleSelect(selectCurrentFromBaseTable, false);
                Table tmpTable = new Table()
                {
                    TableSchema = updateOperation.Table.TableSchema,
                    TableName = "tmpTable"
                };
                String CreateTmpTable = this.CRUDRenderer.RenderTmpTableFromSelect(tmpTable.TableSchema, tmpTable.TableName, select);


                String insertToGlobalUpdate = this.CRUDRenderer.RenderInsert(this.GlobalUpdateTImeManager.GetTable(),
                 new String[] { "Operation", "Timestamp" },
                 new String[] { String.Format("'update on {0}'", this.GlobalUpdateTImeManager.GetTable().TableName),
                 this.CRUDRenderer.GetSQLVariable("ct")}
                 );

                SelectOperation selectFromTempTable = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "tmp" } },
                    FromTable = new FromTable()
                    {
                        TableName = tmpTable.TableName,
                        TableAlias = "tmp",
                        TableSchema = tmpTable.TableSchema
                    }
                };
                String selectFromTempTableSQL = selectHandler.HandleSelect(selectFromTempTable, false);


                UpdateOperation setStartTs = new UpdateOperation()
                {
                    Table = updateOperation.Table,
                    ColumnNames = updateOperation.ColumnNames.Concat(new String[] { IntegratedConstants.StartTS }).ToArray(),
                    ValueLiterals = updateOperation.ValueLiterals.Concat(new String[] { this.CRUDRenderer.GetSQLVariable("ct") }).ToArray(),
                    Restriction = selectCurrentFromBaseTable.Restriction
                };

                UpdateOperation setEndTs = new UpdateOperation()
                {
                    Table = updateOperation.Table,
                    ColumnNames = new String[] { IntegratedConstants.EndTS },
                    ValueLiterals = new String[] { this.CRUDRenderer.GetSQLVariable("ct") },
                    Restriction = selectCurrentFromBaseTable.Restriction
                };
                String setEndtsBaseTable = this.CRUDRenderer.RenderUpdate(setEndTs.Table, setEndTs.ColumnNames, setEndTs.ValueLiterals, setEndTs.Restriction) + ";";
                String insertIntoBaseTableFromTmpTable = this.CRUDRenderer.RenderInsertSelect(new Table() { TableSchema = updateOperation.Table.TableSchema, TableName = updateOperation.Table.TableName }, null,
                    selectFromTempTableSQL);
                String setStartTsBaseTable = this.CRUDRenderer.RenderUpdate(setStartTs.Table, setStartTs.ColumnNames, setStartTs.ValueLiterals, setStartTs.Restriction) + ";";
                String dropTmpTable = this.CRUDRenderer.RenderDropTempTable(tmpTable);
                return new String[]
                {
                    this.CRUDRenderer.RenderNowToVariable("ct"),
                    CreateTmpTable,
                    setEndtsBaseTable,
                    insertIntoBaseTableFromTmpTable,
                    setStartTsBaseTable,
                    insertToGlobalUpdate,
                    dropTmpTable
                };
            };

            String[] lockTables = new string[]
                {
                   updateOperation.Table.TableSchema+"."+updateOperation.Table.TableName,
                   GlobalUpdateTImeManager.GetTableName()
                };
            //IntegratedCRUDExecuter.ExecuteStatementsOnLockedTables(renderStaetement, lockTables, this.DataConnection, this.CRUDRenderer,);
        }
    }
}

