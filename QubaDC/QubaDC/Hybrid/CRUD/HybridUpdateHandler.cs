using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using QubaDC.Restrictions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Hybrid.CRUD
{
    class HybridUpdateHandler
    {
        public HybridUpdateHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, TableMetadataManager metaManager)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
            this.MetaManager = metaManager;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public SchemaManager SchemaManager { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }

        internal void HandleUpdate(UpdateOperation updateOperation)
        {
            SchemaInfo currentSchemaInfo = this.SchemaManager.GetCurrentSchema();
            TableSchema hist = currentSchemaInfo.Schema.FindHistTable(updateOperation.Table);

            String insertTable = this.CRUDRenderer.PrepareTable(updateOperation.Table);
            Table metaTable = this.MetaManager.GetMetaTableFor(updateOperation.Table.TableSchema, updateOperation.Table.TableName);
            String metaTableName = this.CRUDRenderer.PrepareTable(metaTable);
            String histTable = this.CRUDRenderer.PrepareTable(hist.ToTable());

            Func<String[]> renderStaetement = () =>
            {
                String insertIntoBaseTable = this.CRUDRenderer.RenderUpdate(updateOperation.Table, updateOperation.ColumnNames, updateOperation.ValueLiterals, updateOperation.Restriction);


                String insertTimeVariable = "updateTime";
                String setInsertTime = this.CRUDRenderer.RenderNowToVariable(insertTimeVariable);
                String updateTimeVariable = this.CRUDRenderer.GetSQLVariable(insertTimeVariable);
                OperatorRestriction startTsLower = GetStartTsLower(updateOperation.Table, insertTimeVariable);


                var selectAndRestriciton = new AndRestriction();
                selectAndRestriciton.Restrictions = new Restriction[] { startTsLower,  updateOperation.Restriction };


                SelectOperation selectCurrentFromBaseTable = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = updateOperation.Table.TableName } },
                     LiteralColumns = new LiteralColumn[] { new LiteralColumn() {  ColumnLiteral = updateTimeVariable, ColumnName ="ut" } },
                    FromTable = new FromTable()
                    {
                        TableAlias = updateOperation.Table.TableName,
                        TableName = updateOperation.Table.TableName,
                        TableSchema = updateOperation.Table.TableSchema
                    },
                    Restriction = selectAndRestriciton
                };
                HybridSelectHandler selectHandler = new HybridSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
                String selectCurrentWithEndTime = selectHandler.HandleSelect(selectCurrentFromBaseTable, false);
                String isnertIntoHist = this.CRUDRenderer.RenderInsertSelect(new Table()
                { TableSchema = hist.Schema ,TableName = hist.Name },
                null,
             selectCurrentWithEndTime);

                UpdateOperation updateWithStartTs = new UpdateOperation()
                {
                    Table = updateOperation.Table,
                    ColumnNames = updateOperation.ColumnNames.Concat(new String[] { HybridConstants.StartTS }).ToArray(),
                    ValueLiterals = updateOperation.ValueLiterals.Concat(new String[] { updateTimeVariable }).ToArray(),
                    Restriction = selectCurrentFromBaseTable.Restriction
                };
                String updateWithStartTsSql = this.CRUDRenderer.RenderUpdate(updateWithStartTs.Table, updateWithStartTs.ColumnNames, updateWithStartTs.ValueLiterals, updateWithStartTs.Restriction) + ";";

                String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(updateOperation.Table, updateTimeVariable);

                return new String[]
                {
                    setInsertTime,
                    isnertIntoHist,
                    updateWithStartTsSql,
                    //CreateTmpTable,
                    //setEndtsBaseTable,
                    //insertIntoBaseTableFromTmpTable,
                    //setStartTsBaseTable,
                    //dropTmpTable,
                    updateLastUpdate
                };
            };
      
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
            HybridCRUDExecuter.ExecuteStatementsOnLockedTables(renderStaetement, lockTables, lockWrite, this.DataConnection, this.CRUDRenderer, this.SchemaManager, currentSchemaInfo, updateOperation.Table,  this.MetaManager
                , (s) => System.Diagnostics.Debug.WriteLine(s));
            
        }

        private static OperatorRestriction GetEndTsNull(Table table)
        {
            return new OperatorRestriction()
            {
                LHS = new ColumnOperand()
                {
                    Column = new ColumnReference()
                    {
                        ColumnName = HybridConstants.EndTS,
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

        private OperatorRestriction GetStartTsLower(Table Table, String variableName)
        {
            return new OperatorRestriction()
            {
                LHS = new ColumnOperand()
                {
                    Column = new ColumnReference()
                    {
                        ColumnName = HybridConstants.StartTS,
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
