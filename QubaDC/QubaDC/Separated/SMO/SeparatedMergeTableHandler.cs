using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;

namespace QubaDC.Separated.SMO
{
    class SeparatedMergeTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedMergeTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(MergeTable mergeTable)
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


                TableSchemaWithHistTable firstTable = xy.Schema.FindTable(mergeTable.FirstSchema, mergeTable.FirstTableName);
                TableSchema firstHistTable = xy.Schema.FindHistTable(firstTable.Table.ToTable());

                TableSchemaWithHistTable secondTable = xy.Schema.FindTable(mergeTable.FirstSchema, mergeTable.SecondTableName);
                TableSchema secondHistTable = xy.Schema.FindHistTable(secondTable.Table.ToTable());

                Guard.StateTrue(firstTable.Table.Columns.SequenceEqual<String>(secondTable.Table.Columns),"Firsttable and Secondtable do not have the same columns");

                var mergedTableSchema = new TableSchema()
                {
                    Columns = firstTable.Table.Columns,
                    Name = mergeTable.ResultTableName,
                    Schema = mergeTable.ResultSchema
                };
                var mergedHistTableSchema = new TableSchema()
                {
                    Columns = firstHistTable.Columns,
                    Name = mergeTable.ResultTableName + "_" + xy.ID,
                    Schema = mergeTable.ResultSchema
                };
                currentSchema.AddTable(mergedTableSchema, mergedHistTableSchema);
                currentSchema.RemoveTable(firstTable.Table.ToTable());
                currentSchema.RemoveTable(secondTable.Table.ToTable());
                //Copy Table without Triggers
                String copyTableSQL = SMORenderer.RenderCopyTable(firstTable.Table.Schema, firstTable.Table.Name, mergedTableSchema.Schema, mergedTableSchema.Name);
                con.ExecuteNonQuerySQL(copyTableSQL, c);

                //Copy Hist Table without Triggers
                String copyHistTableSQL = SMORenderer.RenderCopyTable(firstHistTable.Schema, firstHistTable.Name, mergedHistTableSchema.Schema, mergedHistTableSchema.Name);
                con.ExecuteNonQuerySQL(copyHistTableSQL, c);

                //Create Triggers on copiedTable

                //INsert Trigger 
                String trigger = SMORenderer.RenderCreateInsertTrigger(mergedTableSchema, mergedHistTableSchema);
                //Delete Trigger
                String deleteTrigger = SMORenderer.RenderCreateDeleteTrigger(mergedTableSchema, mergedHistTableSchema);
                //Update Trigger
                String UpdateTrigger = SMORenderer.RenderCreateUpdateTrigger(mergedTableSchema, mergedHistTableSchema);

                //Add Trigger
                con.ExecuteSQLScript(trigger, c);
                con.ExecuteSQLScript(deleteTrigger, c);
                con.ExecuteSQLScript(UpdateTrigger, c);

                String updateSchema = this.schemaManager.GetInsertSchemaStatement(currentSchema, mergeTable);

                con.ExecuteNonQuerySQL(updateSchema, c);


                //Insert data from old to new
                String insertFromFirstTable = SMORenderer.RenderInsertToTableFromSelect(firstTable.Table, mergedTableSchema,null,null);
                con.ExecuteNonQuerySQL(insertFromFirstTable);

                String insertFromSecondTable = SMORenderer.RenderInsertToTableFromSelect(secondTable.Table, mergedTableSchema,null,null);
                con.ExecuteNonQuerySQL(insertFromSecondTable);


                String DropFirstTable = SMORenderer.RenderDropTable(firstTable.Table.Schema, firstTable.Table.Name);
                con.ExecuteNonQuerySQL(DropFirstTable);
                String DropSecondTable = SMORenderer.RenderDropTable(secondTable.Table.Schema, secondTable.Table.Name);
                con.ExecuteNonQuerySQL(DropSecondTable);
                transaction.Commit();
            });


        }

    }
}
