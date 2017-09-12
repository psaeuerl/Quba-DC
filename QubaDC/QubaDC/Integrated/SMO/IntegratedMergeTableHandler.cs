using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;
using QubaDC.Integrated;

namespace QubaDC.Separated.SMO
{
    class IntegratedMergeTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedMergeTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
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

                Guard.StateTrue(firstTable.Table.Columns.SequenceEqual<String>(secondTable.Table.Columns), "Firsttable and Secondtable do not have the same columns");

                var mergedTableSchema = new TableSchema()
                {
                    Columns = firstTable.Table.Columns,
                    Name = mergeTable.ResultTableName,
                    Schema = mergeTable.ResultSchema,
                    ColumnDefinitions = firstTable.Table.ColumnDefinitions
                };
                var mergedHistTableSchema = new TableSchema()
                {
                    Columns = firstHistTable.Columns,
                    Name = mergeTable.ResultTableName + "_" + xy.ID,
                    Schema = mergeTable.ResultSchema,
                    ColumnDefinitions = firstHistTable.ColumnDefinitions
                };
                currentSchema.AddTable(mergedTableSchema, mergedHistTableSchema);
                currentSchema.RemoveTable(firstTable.Table.ToTable());
                currentSchema.RemoveTable(secondTable.Table.ToTable());

                TableSchema isnertWithStartts = new TableSchema()
                {
                    Columns = firstTable.Table.Columns.Concat(new String[] { IntegratedConstants.StartTS, IntegratedConstants.EndTS }).ToArray(),
                    Name = firstTable.Table.Name,
                    Schema = firstTable.Table.Schema,
                };
                //Copy Table without Triggers
                String copyTableSQL = SMORenderer.RenderCopyTable(firstTable.Table.Schema, firstTable.Table.Name, mergedTableSchema.Schema, mergedTableSchema.Name);
                con.ExecuteNonQuerySQL(copyTableSQL, c);

                //Copy Hist Table without Triggers
                String copyHistTableSQL = SMORenderer.RenderCopyTable(firstHistTable.Schema, firstHistTable.Name, mergedHistTableSchema.Schema, mergedHistTableSchema.Name);
                con.ExecuteNonQuerySQL(copyHistTableSQL, c);

                //con.ExecuteNonQuerySQL(updateSchema, c);
                this.schemaManager.StoreSchema(currentSchema, mergeTable, con, c);


                String[] allColumns = firstTable.Table.Columns;
                var StartEndTs = new String[] { "NOW(3)", "NULL" };
                var RestrictionT1 = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(firstTable.Table.Name, "NOW(3)");

                //Insert data from old to new
                String insertFromFirstTable = SMORenderer.RenderInsertFromOneTableToOther(firstTable.Table, mergedTableSchema, RestrictionT1, allColumns, null, StartEndTs);
                con.ExecuteNonQuerySQL(insertFromFirstTable);

                var RestrictionT2 = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(secondTable.Table.Name, "NOW(3)");
                String insertFromSecondTable = SMORenderer.RenderInsertFromOneTableToOther(secondTable.Table, mergedTableSchema, RestrictionT2, allColumns, null, StartEndTs);
                con.ExecuteNonQuerySQL(insertFromSecondTable);

                DropHistTableRenameCurrentToHist(con, firstTable);
                DropHistTableRenameCurrentToHist(con, secondTable);

                //String DropFirstTable = SMORenderer.RenderDropTable(firstTable.Table.Schema, firstTable.Table.Name);
                //con.ExecuteNonQuerySQL(DropFirstTable);
                //String DropSecondTable = SMORenderer.RenderDropTable(secondTable.Table.Schema, secondTable.Table.Name);
                //con.ExecuteNonQuerySQL(DropSecondTable);

                //;
                transaction.Commit();
            });


        }

        private void DropHistTableRenameCurrentToHist(MySQLDataConnection con, TableSchemaWithHistTable firstTable)
        {
            String dropOriginalHistTable = SMORenderer.RenderDropTable(firstTable.HistTableSchema, firstTable.HistTableName);
            con.ExecuteNonQuerySQL(dropOriginalHistTable);
            String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
            {
                NewSchema = firstTable.HistTableSchema,
                NewTableName = firstTable.HistTableName,
                OldSchema = firstTable.Table.Schema,
                OldTableName = firstTable.Table.Name
            });
            con.ExecuteNonQuerySQL(renameTableSQL);
        }
    }
}
