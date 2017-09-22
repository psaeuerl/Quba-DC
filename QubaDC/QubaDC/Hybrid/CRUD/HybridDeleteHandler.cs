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
    class HybridDeleteHandler
    {
        public HybridDeleteHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, TableMetadataManager meta)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
            this.MetaManager = meta;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SchemaManager SchemaManager { get; private set; }

        internal void HandleDelete(DeleteOperation deleteOperation)
        {


            SchemaInfo currentSchemaInfo = this.SchemaManager.GetCurrentSchema();
            TableSchema hist = currentSchemaInfo.Schema.FindHistTable(deleteOperation.Table);

            String insertTable = this.CRUDRenderer.PrepareTable(deleteOperation.Table);
            Table metaTable = this.MetaManager.GetMetaTableFor(deleteOperation.Table.TableSchema, deleteOperation.Table.TableName);
            String metaTableName = this.CRUDRenderer.PrepareTable(metaTable);
            String histTable = this.CRUDRenderer.PrepareTable(hist.ToTable());

            Func<String[]> renderStaetement = () =>
            {
                //  String insertIntoBaseTable = this.CRUDRenderer.RenderUpdate(deleteOperation.Table, deleteOperation.ColumnNames, deleteOperation.ValueLiterals, deleteOperation.Restriction);


                String insertTimeVariable = "updateTime";
                String setInsertTime = this.CRUDRenderer.RenderNowToVariable(insertTimeVariable);
                String updateTimeVariable = this.CRUDRenderer.GetSQLVariable(insertTimeVariable);
                OperatorRestriction startTsLower = GetStartTsLower(deleteOperation.Table, insertTimeVariable);


                var selectAndRestriciton = new AndRestriction();
                selectAndRestriciton.Restrictions = new Restriction[] { startTsLower, deleteOperation.Restriction };


                SelectOperation selectCurrentFromBaseTable = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = deleteOperation.Table.TableName } },
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updateTimeVariable, ColumnName = "ut" } },
                    FromTable = new FromTable()
                    {
                        TableAlias = deleteOperation.Table.TableName,
                        TableName = deleteOperation.Table.TableName,
                        TableSchema = deleteOperation.Table.TableSchema
                    },
                    Restriction = selectAndRestriciton
                };
                HybridSelectHandler selectHandler = new HybridSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
                String selectCurrentWithEndTime = selectHandler.HandleSelect(selectCurrentFromBaseTable, false);
                String isnertIntoHist = this.CRUDRenderer.RenderInsertSelect(new Table()
                { TableSchema = hist.Schema, TableName = hist.Name },
                null,
             selectCurrentWithEndTime);

                DeleteOperation updateWithStartTs = new DeleteOperation()
                {
                    Table = deleteOperation.Table,
                    Restriction = selectCurrentFromBaseTable.Restriction
                };
                String delete = this.CRUDRenderer.RenderDelete(updateWithStartTs.Table, updateWithStartTs.Restriction);

                String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(deleteOperation.Table, updateTimeVariable);

                return new String[]
                {
                    setInsertTime,
                    isnertIntoHist,
                    delete,
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
            HybridCRUDExecuter.ExecuteStatementsOnLockedTables(renderStaetement, lockTables, lockWrite, this.DataConnection, this.CRUDRenderer, this.SchemaManager, currentSchemaInfo, deleteOperation.Table, this.MetaManager
                , (s) => System.Diagnostics.Debug.WriteLine(s));

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


