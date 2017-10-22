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
    class SeparatedUpdateHandler
    {
        public SeparatedUpdateHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, TableMetadataManager timeManager)
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
            SchemaInfo currentSchemaInfo = this.SchemaManager.GetCurrentSchema();
            TableSchema hist = currentSchemaInfo.Schema.FindHistTable(updateOperation.Table);

            String insertTable = this.CRUDRenderer.PrepareTable(updateOperation.Table);
            Table metaTable = metaManager.GetMetaTableFor(updateOperation.Table.TableSchema, updateOperation.Table.TableName);
            String metaTableName = this.CRUDRenderer.PrepareTable(metaTable);
            String histTable = this.CRUDRenderer.PrepareTable(hist.ToTable());
            //Actually, just insert the statement
            //String insertIntoBaseTable = this.CRUDRenderer.RenderUpdate(updateOperation.Table, updateOperation.ColumnNames, updateOperation.ValueLiterals, updateOperation.Restriction);
            //this.DataConnection.ExecuteQuery(insertIntoBaseTable);
            Func<String[]> renderStaetement = () =>
            {
                String insertTimeVariable = "updateTime";
                String setInsertTime = this.CRUDRenderer.RenderNowToVariable(insertTimeVariable);
                OperatorRestriction endTSNull = GetEndTsNull(updateOperation.Table, updateOperation.Table.TableName);

                var selectAndRestriciton = new AndRestriction();
                selectAndRestriciton.Restrictions = new Restriction[] { endTSNull, updateOperation.Restriction };                

                SelectOperation selectCurrentFromHISTTable = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "updtTable" } },
                    FromTable = new FromTable()
                    {
                        TableAlias = "updtTable",
                        TableName = hist.Name,
                        TableSchema = hist.Schema
                    },
                    Restriction = selectAndRestriciton
                };
                SeparatedSelectHandler selectHandler = new SeparatedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
                String select = selectHandler.HandleSelect(selectCurrentFromHISTTable, false);
                select = select.Replace("`" + updateOperation.Table.TableName+"`", "`updtTable`");

                Table tmpTable = new Table()
                {
                    TableSchema = updateOperation.Table.TableSchema,
                    TableName = "tmpTable"
                };
                String CreateTmpTable = this.CRUDRenderer.RenderTmpTableFromSelect(tmpTable.TableSchema, tmpTable.TableName, select);

                UpdateOperation setEndTSNullHist = new UpdateOperation()
                {
                    ColumnNames = new String[] { SeparatedConstants.EndTS },
                    ValueLiterals = new String[] { this.CRUDRenderer.GetSQLVariable(insertTimeVariable) },
                    Restriction = selectAndRestriciton,
                    Table = hist.ToTable(),                    
                };
                //TODO => Hier die restriction auf grade bringen!!
                //Erweitern dss wir hier ein AS erlauben erlauben
                String setEndTSNullHistSQL = this.CRUDRenderer.RenderUpdate(setEndTSNullHist.Table, setEndTSNullHist.ColumnNames, setEndTSNullHist.ValueLiterals, setEndTSNullHist.Restriction) + ";";
                setEndTSNullHistSQL = setEndTSNullHistSQL.Replace("`" + updateOperation.Table.TableName+"`", "`" + setEndTSNullHist.Table.TableName + "`");

                UpdateOperation setStartTsInTMP = new UpdateOperation()
                {
                    Table = tmpTable,
                    ColumnNames = updateOperation.ColumnNames.Concat(new String[] { SeparatedConstants.StartTS }).ToArray(),
                    ValueLiterals = updateOperation.ValueLiterals.Concat(new String[] { this.CRUDRenderer.GetSQLVariable(insertTimeVariable) }).ToArray(),
               //     Restriction = selectCurrentFromBaseTable.Restriction
                };
                String setStartTsInTMPSQL = this.CRUDRenderer.RenderUpdate(setStartTsInTMP.Table, setStartTsInTMP.ColumnNames, setStartTsInTMP.ValueLiterals, setStartTsInTMP.Restriction) + ";";

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
                String insertIntoHistFromTempTableSQL = this.CRUDRenderer.RenderInsertSelect(new Table() { TableSchema = hist.Schema, TableName = hist.Name }, null,
                    selectFromTempTableSQL);



                //UpdateOperation setEndTs = new UpdateOperation()
                //{
                //    Table = updateOperation.Table,
                //    ColumnNames = new String[] { SeparatedConstants.EndTS },
                //    ValueLiterals = new String[] { this.CRUDRenderer.GetSQLVariable(insertTimeVariable) },
                //    Restriction = selectCurrentFromBaseTable.Restriction
                //};
                //

                //String setStartTsBaseTable = this.CRUDRenderer.RenderUpdate(setStartTs.Table, setStartTs.ColumnNames, setStartTs.ValueLiterals, setStartTs.Restriction) + ";";
                String dropTmpTable = this.CRUDRenderer.RenderDropTempTable(tmpTable);

                String updateLastUpdate = this.metaManager.GetSetLastUpdateStatement(updateOperation.Table, this.CRUDRenderer.GetSQLVariable(insertTimeVariable));
                String updateAcutalTable = this.CRUDRenderer.RenderUpdate(updateOperation.Table, updateOperation.ColumnNames, updateOperation.ValueLiterals, updateOperation.Restriction);

                return new String[]
                {
                    setInsertTime,
                    CreateTmpTable,
                    setEndTSNullHistSQL,
                    setStartTsInTMPSQL,
                    insertIntoHistFromTempTableSQL,
                    dropTmpTable,
                    updateAcutalTable,
                    updateLastUpdate
                };
            };
           
            String[] lockTables = new string[]
                {
                   updateOperation.Table.TableSchema+"."+updateOperation.Table.TableName,
                   metaTableName,
                   histTable,
                   histTable + " AS `updtTable`",
                   SchemaManager.GetTableName()
                };
            Boolean[] lockWrite = new bool[]
            {
                true,
                true,
                true,
                true,
                false
            };
            Action<String> x = (y) => { System.Diagnostics.Debug.WriteLine(y); };
            SeparatedCRUDExecuter.ExecuteStatementsOnLockedTables(renderStaetement, lockTables, lockWrite, this.DataConnection, this.CRUDRenderer, this.SchemaManager, currentSchemaInfo, updateOperation.Table, metaManager
                , (s) => System.Diagnostics.Debug.WriteLine(s));
        }

        private static OperatorRestriction GetEndTsNull(Table table,String refernce)
        {
            return new OperatorRestriction()
            {
                LHS = new ColumnOperand()
                {
                    Column = new ColumnReference()
                    {
                        ColumnName = SeparatedConstants.EndTS,
                        TableReference = refernce
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
    }
}
