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

namespace QubaDC.Separated.SMO
{
    class SeparatedJoinTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedJoinTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
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
                    Schema = jointable.ResultSchema
                };
                var joinedTableHistSchema = new TableSchema()
                {
                    Columns = firstTable.Table.Columns.Union(secondHistTable.Columns).Distinct().ToArray(),
                    Name = jointable.ResultTableName + "_" + xy.ID,
                    Schema = jointable.ResultSchema
                };
                currentSchema.AddTable(joinedTableSchema, joinedTableHistSchema);
                currentSchema.RemoveTable(firstTable.Table.ToTable());
                currentSchema.RemoveTable(secondTable.Table.ToTable());

                CreateJoinedTable(c, con, firstTable.Table, secondTable.Table, joinedTableSchema);
                CreateJoinedTable(c, con, firstHistTable, secondHistTable, joinedTableHistSchema);



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
                String select = CreateSelectForTables(firstTable.Table, secondTable.Table, jointable.FirstTableAlias, jointable.SecondTableAlias, jointable.JoinRestriction);
                String insertFromFirstTable = SMORenderer.RenderInsertToTableFromSelect(joinedTableSchema,select);
                con.ExecuteNonQuerySQL(insertFromFirstTable);

                String DropFirstTable = SMORenderer.RenderDropTable(firstTable.Table.Schema, firstTable.Table.Name);
                con.ExecuteNonQuerySQL(DropFirstTable);
                String DropSecondTable = SMORenderer.RenderDropTable(secondTable.Table.Schema, secondTable.Table.Name);
                con.ExecuteNonQuerySQL(DropSecondTable);
                String updateSchema = this.schemaManager.GetInsertSchemaStatement(currentSchema, jointable);

                con.ExecuteNonQuerySQL(updateSchema, c);
                transaction.Commit();
            });


        }

        private void CreateJoinedTable(System.Data.Common.DbConnection c, MySQLDataConnection con, TableSchema firstTable, TableSchema secondTable, TableSchema joinedTableSchema)
        {
            string select = CreateSelectForTables(firstTable, secondTable,"t1","t2", new OperatorRestriction()
            {
                LHS = new LiteralOperand() { Literal = "1" },
                Op = RestrictionOperator.Equals,
                RHS = new LiteralOperand() { Literal = "2" }
            });

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(joinedTableSchema.Schema, joinedTableSchema.Name, select);
            con.ExecuteNonQuerySQL(copyTableSQL, c);
        }

        private string CreateSelectForTables(TableSchema firstTable, TableSchema secondTable, String firstTableRename, String SecondTableRename, Restriction r)
        {
            String[] columnsFromTable1 = firstTable.Columns.ToArray();
            String[] columnsFromTable2 = secondTable.Columns.Except(columnsFromTable1).ToArray();

            ColumnReference[] refTable1 = columnsFromTable1.Select(x => new ColumnReference() { ColumnName = x, TableReference = firstTableRename }).ToArray();
            ColumnReference[] refTable2 = columnsFromTable2.Select(x => new ColumnReference() { ColumnName = x, TableReference = SecondTableRename }).ToArray();

            SelectOperation s = new SelectOperation()
            {
                Columns = refTable1.Union(refTable2).ToArray(),
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
