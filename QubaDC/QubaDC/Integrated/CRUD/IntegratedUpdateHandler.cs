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
        public IntegratedUpdateHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, GlobalUpdateTimeManager timeManager)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
            this.GlobalUpdateTImeManager = timeManager;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public GlobalUpdateTimeManager GlobalUpdateTImeManager { get; private set; }
        public SchemaManager SchemaManager { get; private set; }

        internal void HandleUpdate(UpdateOperation updateOperation)
        {
      

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







            //SelectOperation selectMaxTs = new SelectOperation()
            //{
            //    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "MAX(`startts`)" } },
            //    FromTable = s.FromTable,
            //    Restriction = s.Restriction
            //};
            //String query = selectHandler.HandleSelect(selectMaxTs, false);
            //String max = query.Replace("``.`MAX(`startts`)`", "MAX(`startts`)");

            //Acutal Idea:
            //a.) it is safe to delete => Update End TS
            //b.) it is safe to insert
            //do it in two steps


            DateTime currentTime = DateTime.Now;
            //Delete => set Endts
            this.DataConnection.DoTransaction((trans, con) =>
            {
                currentTime = this.GlobalUpdateTImeManager.GetLatestUpdate().DateTime;

                //  DateTime t = System.DateTime.Now;
              
                var updateAndRestriction = new AndRestriction();
                updateAndRestriction.Restrictions = new Restriction[] { endTSNull, updateOperation.Restriction };
                UpdateOperation setEndTs = new UpdateOperation()
                {
                    ColumnNames = new String[] { IntegratedConstants.EndTS },
                    Table = updateOperation.Table,
                    ValueLiterals = new String[] { "NOW(3)" },
                    Restriction = updateAndRestriction
                };
                String update = this.CRUDRenderer.RenderUpdate(setEndTs.Table, setEndTs.ColumnNames, setEndTs.ValueLiterals, setEndTs.Restriction);
                this.DataConnection.ExecuteQuery(update, con);
                trans.Commit();
            });
            //Insert New ones => set Endts
            this.DataConnection.DoTransaction((trans, con) =>
            {
                //Actually, just insert the statement
                //Insert new Rows that are like the update
                //Update the old ones
                InsertOperation op = new InsertOperation()
                {
                    ColumnNames = updateOperation.ColumnNames.Union(IntegratedConstants.GetHistoryTableColumns().Select(x => x.ColumName)).ToArray(),
                    ValueLiterals = updateOperation.ValueLiterals.Union(new String[] { "NOW(3)", "null" }).ToArray(),
                    InsertTable = updateOperation.Table
                };
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
                        Literal = MySQLDialectHelper.RenderDateTime(currentTime)
                    }
                };
                OperatorRestriction endTsBigger = new OperatorRestriction()
                {
                    LHS = new ColumnOperand()
                    {
                        Column = new ColumnReference()
                        {
                            ColumnName = IntegratedConstants.EndTS,
                            TableReference = updateOperation.Table.TableName
                        }
                    },
                    Op = RestrictionOperator.GET
        ,
                    RHS = new LiteralOperand()
                    {
                        Literal = MySQLDialectHelper.RenderDateTime(currentTime)
                    }
                };
                var selectAndRestriciton = new AndRestriction();
                selectAndRestriciton.Restrictions = new Restriction[] { startTsLower, endTsBigger, updateOperation.Restriction };
                SelectOperation s = new SelectOperation()
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
                String select = selectHandler.HandleSelect(s, false);
                String insertIntoBaseTable = this.CRUDRenderer.RenderInsertSelect(op.InsertTable, op.ColumnNames, select);
                this.DataConnection.ExecuteInsert(insertIntoBaseTable);
                trans.Commit();
            });
        }
    }
}
