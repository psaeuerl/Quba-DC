using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;
using QubaDC.CRUD;
using QubaDC.Restrictions;
using QubaDC.Hybrid;

namespace QubaDC.Separated.SMO
{
    class HybridJoinTableHandler
    {
        private SchemaManager schemaManager;

        public HybridJoinTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(JoinTable jointable)
        {

            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data twice!

            var con = (MySQLDataConnection)DataConnection;
            con.DoTransaction((transaction, c) =>
            {

                SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
                Schema currentSchema = xy.Schema;


                TableSchemaWithHistTable firstTable = xy.Schema.FindTable(jointable.FirstSchema, jointable.FirstTableName);
                TableSchema firstHistTable = xy.Schema.FindHistTable(firstTable.Table.ToTable());

                TableSchemaWithHistTable secondTable = xy.Schema.FindTable(jointable.FirstSchema, jointable.SecondTableName);
                TableSchema secondHistTable = xy.Schema.FindHistTable(secondTable.Table.ToTable());


                var joinedTableSchema = new TableSchema()
                {
                    Columns = firstTable.Table.Columns.Union(secondTable.Table.Columns).Distinct().ToArray(),
                    Name = jointable.ResultTableName,
                    Schema = jointable.ResultSchema,
                    ColumnDefinitions = firstTable.Table.ColumnDefinitions.Union(secondTable.Table.ColumnDefinitions).GroupBy(x=>x.ColumName).Select(x=>x.First()).ToArray()
                };
                var joinedTableHistSchema = new TableSchema()
                {
                    Columns = firstTable.Table.Columns.Union(secondHistTable.Columns).Distinct().ToArray(),
                    Name = jointable.ResultTableName + "_" + xy.ID,
                    Schema = jointable.ResultSchema,
                     ColumnDefinitions = firstTable.Table.ColumnDefinitions.Union(secondHistTable.ColumnDefinitions).GroupBy(x => x.ColumName).Select(x => x.First()).ToArray()
                };
                currentSchema.AddTable(joinedTableSchema, joinedTableHistSchema);
                currentSchema.RemoveTable(firstTable.Table.ToTable());
                currentSchema.RemoveTable(secondTable.Table.ToTable());

                CreateJoinedTable(c, con, firstTable.Table, secondTable.Table, joinedTableSchema,true);
                CreateJoinedTable(c, con, firstHistTable, secondHistTable, joinedTableHistSchema,false);



                ////Create Triggers on copiedTable

                //INsert Trigger 
                String trigger = SMORenderer.RenderCreateInsertTrigger(joinedTableSchema, joinedTableHistSchema);
                //Delete Trigger
                String deleteTrigger = SMORenderer.RenderCreateDeleteTrigger(joinedTableSchema, joinedTableHistSchema);
                //Update Trigger
                String UpdateTrigger = SMORenderer.RenderCreateUpdateTrigger(joinedTableSchema, joinedTableHistSchema);

                //Add Trigger
                con.ExecuteSQLScript(trigger, c);
                con.ExecuteSQLScript(deleteTrigger, c);
                con.ExecuteSQLScript(UpdateTrigger, c);




                ////Insert data from old to new
                String select = CreateSelectForTables(firstTable.Table, secondTable.Table, jointable.FirstTableAlias, jointable.SecondTableAlias, jointable.JoinRestriction,false);
                String insertFromFirstTable = SMORenderer.RenderInsertToTableFromSelect(joinedTableSchema,select);
                con.ExecuteNonQuerySQL(insertFromFirstTable);

                String deleteFromFirstTable = this.SMORenderer.CRUDRenderer.RenderDelete(firstTable.Table.ToTable(), null);
                con.ExecuteNonQuerySQL(deleteFromFirstTable);
                String DropFirstTable = SMORenderer.RenderDropTable(firstTable.Table.Schema, firstTable.Table.Name);
                con.ExecuteNonQuerySQL(DropFirstTable);

                String deleteFromSecondTable = this.SMORenderer.CRUDRenderer.RenderDelete(secondTable.Table.ToTable(), null);
                con.ExecuteNonQuerySQL(deleteFromSecondTable);
                String DropSecondTable = SMORenderer.RenderDropTable(secondTable.Table.Schema, secondTable.Table.Name);
                con.ExecuteNonQuerySQL(DropSecondTable);
                //String updateSchema = this.schemaManager.GetInsertSchemaStatement(currentSchema, jointable);

                //con.ExecuteNonQuerySQL(updateSchema, c);
                this.schemaManager.StoreSchema(currentSchema, jointable, con, c);

                transaction.Commit();
            });


        }

        private void CreateJoinedTable(System.Data.Common.DbConnection c, MySQLDataConnection con, TableSchema firstTable, TableSchema secondTable, TableSchema joinedTableSchema, Boolean IncludeTSColumn)
        {
            string select = CreateSelectForTables(firstTable, secondTable,"t1","t2", new OperatorRestriction()
            {
                LHS = new LiteralOperand() { Literal = "1" },
                Op = RestrictionOperator.Equals,
                RHS = new LiteralOperand() { Literal = "2" }
            },IncludeTSColumn);

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(joinedTableSchema.Schema, joinedTableSchema.Name, select);
            con.ExecuteNonQuerySQL(copyTableSQL, c);
        }

        private string CreateSelectForTables(TableSchema firstTable, TableSchema secondTable, String firstTableRename, String SecondTableRename, Restriction r, Boolean IncludeTSColumn)
        {
            String[] columnsFromTable1 = firstTable.Columns.ToArray();
            String[] columnsFromTable2 = secondTable.Columns.Except(columnsFromTable1).ToArray();

            ColumnReference[] refTable1 = columnsFromTable1.Select(x => new ColumnReference() { ColumnName = x, TableReference = firstTableRename }).ToArray();
            ColumnReference[] refTable2 = columnsFromTable2.Select(x => new ColumnReference() { ColumnName = x, TableReference = SecondTableRename }).ToArray();
            ColumnDefinition cf = HybridConstants.GetStartColumn();
            ColumnReference[] tsColumns = new ColumnReference[] { };
            if (IncludeTSColumn)
                tsColumns = new ColumnReference[] { new ColumnReference() { ColumnName = cf.ColumName, TableReference = firstTableRename } };
            SelectOperation s = new SelectOperation()
            {
                Columns = refTable1.Union(refTable2)
                .Union(tsColumns)
                .ToArray(),
                FromTable = new FromTable() { TableName = firstTable.Name, TableSchema = firstTable.Schema, TableAlias = firstTableRename },
                JoinedTables = new JoinedTable[]
                   {
                            new JoinedTable()
                            {
                                 Join = JoinType.NoJoin,
                                  JoinCondition = null,
                                   TableName = secondTable.Name,
                                    TableSchema = secondTable.Schema,
                                     TableAlias = SecondTableRename
                            }
                   },
                Restriction = r
            };

            String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);
            return select;
        }
    }
}
