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
    class SeperatedPartitionTableHandler
    {
        private SchemaManager schemaManager;

        public SeperatedPartitionTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(PartitionTable partitionTable)
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

                var trueTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns,
                    Name = partitionTable.TrueConditionTableName,
                    Schema = partitionTable.TrueConditionSchema
                };
                var trueTableHist = new TableSchema()
                {
                    Columns = originalHistTable.Columns,
                    Name = partitionTable.TrueConditionTableName + "_" + xy.ID,
                    Schema = partitionTable.TrueConditionSchema
                };
                currentSchema.AddTable(trueTableSchema, trueTableHist);

                var falseTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns,
                    Name = partitionTable.FalseConditionTableName,
                    Schema = partitionTable.FalseConditionSchema
                };
                var falseTableSchemaHist = new TableSchema()
                {
                    Columns = originalHistTable.Columns,
                    Name = partitionTable.FalseConditionTableName + "_" + xy.ID,
                    Schema = partitionTable.FalseConditionSchema
                };
                currentSchema.AddTable(falseTableSchema, falseTableSchemaHist);

                ////Copy Tables without Triggers
                String copyTrueTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, trueTableSchema.Schema, trueTableSchema.Name);
                con.ExecuteNonQuerySQL(copyTrueTable, c);

                //Copy Hist Table without Triggers
                String copyTrueHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, trueTableHist.Schema, trueTableHist.Name);
                con.ExecuteNonQuerySQL(copyTrueHistTableSQL, c);


                String copyFalseTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, falseTableSchema.Schema, falseTableSchema.Name);
                con.ExecuteNonQuerySQL(copyFalseTable, c);

                //Copy Hist Table without Triggers
                String copyFalseHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, falseTableSchemaHist.Schema, falseTableSchemaHist.Name);
                con.ExecuteNonQuerySQL(copyFalseHistTableSQL, c);

                CreateTriggers( c, con, currentSchema, trueTableSchema, trueTableHist);
                CreateTriggers( c, con, currentSchema, falseTableSchema, falseTableSchemaHist);


                //Insert data from old to true
                Restriction trueRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "TRUE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                String insertTrueFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, trueTableSchema, trueRestriction,null);
                con.ExecuteNonQuerySQL(insertTrueFromTable);

                //Insert data from old to false
                Restriction falseRestriction = new OperatorRestriction() { LHS = new LiteralOperand() { Literal = "FALSE" }, Op = RestrictionOperator.Equals, RHS = new RestrictionRestrictionOperand() { Restriciton = partitionTable.Restriction } };
                String insertFalseFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, falseTableSchema, falseRestriction,null);
                con.ExecuteNonQuerySQL(insertFalseFromTable);



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
