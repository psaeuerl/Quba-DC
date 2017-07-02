using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;
using QubaDC.Restrictions;

namespace QubaDC.Separated.SMO
{
    class SeparatedDecomposeTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedDecomposeTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(DecomposeTable partitionTable)
        {
            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data

            var con = (MySQLDataConnection)DataConnection;
            con.DoTransaction((transaction, c) =>
            {

                SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
                Schema currentSchema = xy.Schema;


                TableSchemaWithHistTable originalTable = xy.Schema.FindTable(partitionTable.BaseSchema, partitionTable.BaseTableName);
                TableSchema originalHistTable = xy.Schema.FindHistTable(originalTable.Table.ToTable());

                var firstTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Where(x=>partitionTable.FirstColumns.Contains(x) || partitionTable.SharedColumns.Contains(x)).ToArray(),
                    Name = partitionTable.FirstTableName,
                    Schema = partitionTable.FirstSchema
                };
                var firstTableHistSchema = new TableSchema()
                {
                    Columns = firstTableSchema.Columns.Union(originalHistTable.Columns.Except(originalTable.Table.Columns)).ToArray(),
                    Name = partitionTable.FirstTableName + "_" + xy.ID,
                    Schema = partitionTable.FirstSchema
                };
                currentSchema.AddTable(firstTableSchema, firstTableHistSchema);

                var secondTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Where(x => partitionTable.SecondColumns.Contains(x) || partitionTable.SharedColumns.Contains(x)).ToArray(),
                    Name = partitionTable.FirstTableName,
                    Schema = partitionTable.FirstSchema
                };
                var secondTableHistSchema = new TableSchema()
                {
                    Columns = secondTableSchema.Columns.Union(originalHistTable.Columns.Except(originalTable.Table.Columns)).ToArray(),
                    Name = partitionTable.FirstTableName + "_" + xy.ID,
                    Schema = partitionTable.FirstSchema
                };
                currentSchema.AddTable(secondTableSchema, secondTableHistSchema);

                ////Copy Tables without Triggers
                String copyTrueTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, firstTableSchema.Schema, firstTableSchema.Name);
                con.ExecuteNonQuerySQL(copyTrueTable, c);

                String[] firstTableDropColumns = originalTable.Table.Columns.Except(firstTableSchema.Columns).ToArray();
                String firstTableDropColumnsSQL = SMORenderer.RenderDropColumns(firstTableSchema.Schema, firstTableSchema.Name, firstTableDropColumns);
                con.ExecuteNonQuerySQL(firstTableDropColumnsSQL, c);


                ////Copy Hist Table without Triggers
                String copyTrueHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, firstTableHistSchema.Schema, firstTableHistSchema.Name);
                con.ExecuteNonQuerySQL(copyTrueHistTableSQL, c);

                String[] secondTableDropColumns = originalTable.Table.Columns.Except(secondTableSchema.Columns).ToArray();
                String secondTableDropColumnsSQL = SMORenderer.RenderDropColumns(secondTableSchema.Schema, secondTableSchema.Name, secondTableDropColumns);
                con.ExecuteNonQuerySQL(secondTableDropColumnsSQL, c);





                CreateTriggers( c, con, currentSchema, firstTableSchema, firstTableHistSchema);
                CreateTriggers( c, con, currentSchema, secondTableSchema, secondTableHistSchema);


                //Insert data from old to true
                String insertTrueFromTable = SMORenderer.RenderInsertToTableFromSelect(originalTable.Table, firstTableSchema, null);
                con.ExecuteNonQuerySQL(insertTrueFromTable);

                ////Insert data from old to false
                //Restriction falseRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "FALSE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                //String insertFalseFromTable = SMORenderer.RenderInsertToTableFromSelect(originalTable.Table, falseTableSchema, falseRestriction);
                //con.ExecuteNonQuerySQL(insertFalseFromTable);



                String DropFirstTable = SMORenderer.RenderDropTable(originalTable.Table.Schema, originalTable.Table.Name);
                con.ExecuteNonQuerySQL(DropFirstTable);
                currentSchema.RemoveTable(originalTable.Table.ToTable());

                String updateSchema = this.schemaManager.GetInsertSchemaStatement(currentSchema, partitionTable);
                con.ExecuteNonQuerySQL(updateSchema, c);
                transaction.Commit();
            });
        

        }

        private void CreateTriggers(System.Data.Common.DbConnection c, MySQLDataConnection con, Schema currentSchema, TableSchema trueTableSchema, TableSchema trueTableHist)
        {
            //Create Triggers on copiedTable

            //INsert Trigger 
            String trigger = SMORenderer.RenderCreateInsertTrigger(trueTableSchema, trueTableHist);
            //Delete Trigger
            String deleteTrigger = SMORenderer.RenderCreateDeleteTrigger(trueTableSchema, trueTableHist);
            //Update Trigger
            String UpdateTrigger = SMORenderer.RenderCreateUpdateTrigger(trueTableSchema, trueTableHist);

            //Add Trigger
            con.ExecuteSQLScript(trigger, c);
            con.ExecuteSQLScript(deleteTrigger, c);
            con.ExecuteSQLScript(UpdateTrigger, c);
        }     
    }
}
