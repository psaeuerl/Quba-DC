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
                    Columns = originalTable.Table.Columns.Where(x => partitionTable.FirstColumns.Contains(x) || partitionTable.SharedColumns.Contains(x)).ToArray(),
                    Name = partitionTable.FirstTableName,
                    Schema = partitionTable.FirstSchema,                     
                };
                firstTableSchema.ColumnDefinitions = originalTable.Table.ColumnDefinitions.Where(x => firstTableSchema.Columns.Contains(x.ColumName)).ToArray();
                var firstTableHistSchema = new TableSchema()
                {
                    Columns = firstTableSchema.Columns.Union(originalHistTable.Columns.Except(originalTable.Table.Columns)).ToArray(),
                    Name = partitionTable.FirstTableName + "_" + xy.ID,
                    Schema = partitionTable.FirstSchema
                };
                firstTableHistSchema.ColumnDefinitions = originalHistTable.ColumnDefinitions.Where(x => firstTableHistSchema.Columns.Contains(x.ColumName)).ToArray();
                currentSchema.AddTable(firstTableSchema, firstTableHistSchema);

                var secondTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Where(x => partitionTable.SecondColumns.Contains(x) || partitionTable.SharedColumns.Contains(x)).ToArray(),
                    Name = partitionTable.SecondTableName,
                    Schema = partitionTable.SecondSchema
                };
                secondTableSchema.ColumnDefinitions = originalTable.Table.ColumnDefinitions.Where(x => secondTableSchema.Columns.Contains(x.ColumName)).ToArray();
                var secondTableHistSchema = new TableSchema()
                {
                    Columns = secondTableSchema.Columns.Union(originalHistTable.Columns.Except(originalTable.Table.Columns)).ToArray(),
                    Name = partitionTable.SecondTableName + "_" + xy.ID,
                    Schema = partitionTable.SecondSchema
                };
                secondTableHistSchema.ColumnDefinitions = originalHistTable.ColumnDefinitions.Where(x => secondTableHistSchema.Columns.Contains(x.ColumName)).ToArray();
                currentSchema.AddTable(secondTableSchema, secondTableHistSchema);

                ////Copy Tables without Triggers
                CreateCopiedTables(c, con, originalTable, originalHistTable, firstTableSchema, firstTableHistSchema);

                CreateCopiedTables(c, con, originalTable, originalHistTable, secondTableSchema, secondTableHistSchema);





                CreateTriggers(c, con, currentSchema, firstTableSchema, firstTableHistSchema);
                CreateTriggers(c, con, currentSchema, secondTableSchema, secondTableHistSchema);


                //Insert data from old to true                
                String insertTrueFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, firstTableSchema, null, firstTableSchema.Columns);
                con.ExecuteNonQuerySQL(insertTrueFromTable);

                //Insert data from old to true                
                String insertFromSecondTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, secondTableSchema, null, secondTableSchema.Columns);
                con.ExecuteNonQuerySQL(insertFromSecondTable);



                String DropFirstTable = SMORenderer.RenderDropTable(originalTable.Table.Schema, originalTable.Table.Name);
                con.ExecuteNonQuerySQL(DropFirstTable);
                currentSchema.RemoveTable(originalTable.Table.ToTable());


                this.schemaManager.StoreSchema(currentSchema, partitionTable, con, c);

                transaction.Commit();
            });
        

        }

        private void CreateCopiedTables(System.Data.Common.DbConnection c, MySQLDataConnection con, TableSchemaWithHistTable originalTable, TableSchema originalHistTable, TableSchema normalschema, TableSchema nomralHistSchema)
        {
            String copyTrueTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, normalschema.Schema, normalschema.Name);
            con.ExecuteNonQuerySQL(copyTrueTable, c);

            String[] firstTableDropColumns = originalTable.Table.Columns.Except(normalschema.Columns).ToArray();
            String firstTableDropColumnsSQL = SMORenderer.RenderDropColumns(normalschema.Schema, normalschema.Name, firstTableDropColumns);
            con.ExecuteNonQuerySQL(firstTableDropColumnsSQL, c);

            String copyTrueHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, nomralHistSchema.Schema, nomralHistSchema.Name);
            con.ExecuteNonQuerySQL(copyTrueHistTableSQL, c);

            String[] firstHistTableDropColumns = originalHistTable.Columns.Except(nomralHistSchema.Columns).ToArray();
            String firstHistTableDropColumnsSQL = SMORenderer.RenderDropColumns(nomralHistSchema.Schema, nomralHistSchema.Name, firstTableDropColumns);
            con.ExecuteNonQuerySQL(firstHistTableDropColumnsSQL, c);
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
