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
    class IntegratedUpdateHandler
    {
        public IntegratedUpdateHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, TableMetadataManager timeManager)
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

        internal void HandleUpdate(UpdateOperation updateOperation)
        {

            Func<String[]> renderStaetement = () =>
            {
                String insertTimeVariable = "updateTime";
                String setInsertTime = this.CRUDRenderer.RenderNowToVariable(insertTimeVariable);
                OperatorRestriction startTsLower = GetStartTsLower(updateOperation.Table, insertTimeVariable);
                OperatorRestriction endTSNull = GetEndTsNull(updateOperation.Table);

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
                    ValueLiterals = updateOperation.ValueLiterals.Concat(new String[] { this.CRUDRenderer.GetSQLVariable(insertTimeVariable) }).ToArray(),
                    Restriction = selectCurrentFromBaseTable.Restriction
                };

                UpdateOperation setEndTs = new UpdateOperation()
                {
                    Table = updateOperation.Table,
                    ColumnNames = new String[] { IntegratedConstants.EndTS },
                    ValueLiterals = new String[] { this.CRUDRenderer.GetSQLVariable(insertTimeVariable) },
                    Restriction = selectCurrentFromBaseTable.Restriction
                };
                String setEndtsBaseTable = this.CRUDRenderer.RenderUpdate(setEndTs.Table, setEndTs.ColumnNames, setEndTs.ValueLiterals, setEndTs.Restriction) + ";";
                String insertIntoBaseTableFromTmpTable = this.CRUDRenderer.RenderInsertSelect(new Table() { TableSchema = updateOperation.Table.TableSchema, TableName = updateOperation.Table.TableName }, null,
                    selectFromTempTableSQL);
                String setStartTsBaseTable = this.CRUDRenderer.RenderUpdate(setStartTs.Table, setStartTs.ColumnNames, setStartTs.ValueLiterals, setStartTs.Restriction) + ";";
                String dropTmpTable = this.CRUDRenderer.RenderDropTempTable(tmpTable);

                String updateLastUpdate = this.metaManager.GetSetLastUpdateStatement(updateOperation.Table, this.CRUDRenderer.GetSQLVariable(insertTimeVariable));
                return new String[]
                {
                    setInsertTime,
                    CreateTmpTable,
                    setEndtsBaseTable,
                    insertIntoBaseTableFromTmpTable,
                    setStartTsBaseTable,                    
                    dropTmpTable,
                    updateLastUpdate
                };
            };
            SchemaInfo currentSchemaInfo = this.SchemaManager.GetCurrentSchema();
            TableSchema hist = currentSchemaInfo.Schema.FindHistTable(updateOperation.Table);

            String insertTable = this.CRUDRenderer.PrepareTable(updateOperation.Table);
            Table metaTable = metaManager.GetMetaTableFor(updateOperation.Table.TableSchema, updateOperation.Table.TableName);
            String metaTableName = this.CRUDRenderer.PrepareTable(metaTable);
            String histTable = this.CRUDRenderer.PrepareTable(hist.ToTable());
            String[] lockTables = new string[]
                {
                   updateOperation.Table.TableSchema+"."+updateOperation.Table.TableName,
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
            IntegratedCRUDExecuter.ExecuteStatementsOnLockedTables(renderStaetement, lockTables, lockWrite, this.DataConnection, this.CRUDRenderer, this.SchemaManager, currentSchemaInfo, updateOperation.Table, metaManager
                , (s) => System.Diagnostics.Debug.WriteLine(s));
        }

        //public void otherCode(UpdateOperation updateOperation)
        //{
        //    Func<String[]> renderStaetement = () =>
        //    {
        //        String insertTimeVariable = "updateTime";
        //        String setInsertTime = this.CRUDRenderer.RenderNowToVariable(insertTimeVariable);
        //        OperatorRestriction startTsLower = GetStartTsLower(updateOperation.Table, insertTimeVariable);
        //        OperatorRestriction endTSNull = GetEndTsNull(updateOperation.Table);

        //        var selectAndRestriciton = new AndRestriction();
        //        selectAndRestriciton.Restrictions = new Restriction[] { startTsLower, endTSNull, updateOperation.Restriction };


        //        SelectOperation selectCurrentFromBaseTable = new SelectOperation()
        //        {
        //            Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = updateOperation.Table.TableName } },
        //            FromTable = new FromTable()
        //            {
        //                TableAlias = updateOperation.Table.TableName,
        //                TableName = updateOperation.Table.TableName,
        //                TableSchema = updateOperation.Table.TableSchema
        //            },
        //            Restriction = selectAndRestriciton
        //        };
        //        IntegratedSelectHandler selectHandler = new IntegratedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
        //        String select = selectHandler.HandleSelect(selectCurrentFromBaseTable, false);


        //        Table tmpTable = new Table()
        //        {
        //            TableSchema = updateOperation.Table.TableSchema,
        //            TableName = "tmpTable"
        //        };
        //        String CreateTmpTable = this.CRUDRenderer.RenderTmpTableFromSelect(tmpTable.TableSchema, tmpTable.TableName, select);

        //        SelectOperation selectFromTempTable = new SelectOperation()
        //        {
        //            Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "tmp" } },
        //            FromTable = new FromTable()
        //            {
        //                TableName = tmpTable.TableName,
        //                TableAlias = "tmp",
        //                TableSchema = tmpTable.TableSchema
        //            }
        //        };
        //        String selectFromTempTableSQL = selectHandler.HandleSelect(selectFromTempTable, false);


        //        UpdateOperation setStartTs = new UpdateOperation()
        //        {
        //            Table = updateOperation.Table,
        //            ColumnNames = updateOperation.ColumnNames.Concat(new String[] { IntegratedConstants.StartTS }).ToArray(),
        //            ValueLiterals = updateOperation.ValueLiterals.Concat(new String[] { this.CRUDRenderer.GetSQLVariable("ct") }).ToArray(),
        //            Restriction = selectCurrentFromBaseTable.Restriction
        //        };

        //        UpdateOperation setEndTs = new UpdateOperation()
        //        {
        //            Table = updateOperation.Table,
        //            ColumnNames = new String[] { IntegratedConstants.EndTS },
        //            ValueLiterals = new String[] { this.CRUDRenderer.GetSQLVariable("ct") },
        //            Restriction = selectCurrentFromBaseTable.Restriction
        //        };
        //        String setEndtsBaseTable = this.CRUDRenderer.RenderUpdate(setEndTs.Table, setEndTs.ColumnNames, setEndTs.ValueLiterals, setEndTs.Restriction) + ";";
        //        String insertIntoBaseTableFromTmpTable = this.CRUDRenderer.RenderInsertSelect(new Table() { TableSchema = updateOperation.Table.TableSchema, TableName = updateOperation.Table.TableName }, null,
        //            selectFromTempTableSQL);
        //        String setStartTsBaseTable = this.CRUDRenderer.RenderUpdate(setStartTs.Table, setStartTs.ColumnNames, setStartTs.ValueLiterals, setStartTs.Restriction) + ";";
        //        String dropTmpTable = this.CRUDRenderer.RenderDropTempTable(tmpTable);

        //        String updateLastUpdate = this.metaManager.GetSetLastUpdateStatement(updateOperation.Table, this.CRUDRenderer.GetSQLVariable(nowVariable));
        //        return new String[]
        //        {
        //            setInsertTime,
        //            CreateTmpTable,
        //            setEndtsBaseTable,
        //            insertIntoBaseTableFromTmpTable,
        //            setStartTsBaseTable, 
        //            dropTmpTable
        //        };
        //    };

        //    String[] lockTables = new string[]
        //        {
        //           updateOperation.Table.TableSchema+"."+updateOperation.Table.TableName,
        //           metaManager.GetTableName()
        //        };
        //}

        private static OperatorRestriction GetEndTsNull(Table table)
        {
            return new OperatorRestriction()
            {
                LHS = new ColumnOperand()
                {
                    Column = new ColumnReference()
                    {
                        ColumnName = IntegratedConstants.EndTS,
                        TableReference = table.TableName
                    }
                },
                Op = RestrictionOperator.IS
,
                RHS = new LiteralOperand()
                {
                    Literal = "NULL"
                }
            };
        }

        private OperatorRestriction GetStartTsLower(Table Table,String variableName)
        {
            return new OperatorRestriction()
            {
                LHS = new ColumnOperand()
                {
                    Column = new ColumnReference()
                    {
                        ColumnName = IntegratedConstants.StartTS,
                        TableReference = Table.TableName
                    }
                },
                Op = RestrictionOperator.LT
     ,
                RHS = new LiteralOperand()
                {
                    Literal = this.CRUDRenderer.GetSQLVariable(variableName)
                }
            };
        }
    }
}

