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
using QubaDC.Integrated;

namespace QubaDC.Separated.SMO
{
    class IntegratedCopyTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedCopyTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(CopyTable copyTable)
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


                TableSchemaWithHistTable originalTable = xy.Schema.FindTable(copyTable.Schema, copyTable.TableName);
                TableSchema originalHistTable = xy.Schema.FindHistTable(originalTable.Table.ToTable());

                var copiedTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns,
                    Name = copyTable.CopiedTableName,
                    Schema = copyTable.CopiedSchema,
                     ColumnDefinitions = originalTable.Table.ColumnDefinitions,
                };
                var copiedHistSchema = new TableSchema()
                {
                    Columns = originalHistTable.Columns,
                    Name = copyTable.CopiedTableName + "_" + xy.ID,
                    Schema = copyTable.CopiedSchema,
                     ColumnDefinitions = originalHistTable.ColumnDefinitions
                };
                currentSchema.AddTable(copiedTableSchema, copiedHistSchema);

                //Copy Table without Triggers
                String copyTableSQL = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, copiedTableSchema.Schema, copiedTableSchema.Name);
                con.ExecuteNonQuerySQL(copyTableSQL, c);

                //Copy Hist Table without Triggers
                String copyHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, copiedHistSchema.Schema, copiedHistSchema.Name);

                con.ExecuteNonQuerySQL(copyHistTableSQL, c);

                //Insert data from old to new                
                String[]allColumns = originalTable.Table.Columns;
                var StartEndTs = new String[] { "NOW(3)", "NULL" };
                var Restriction = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(originalTable.Table.Name, "NOW(3)");

                String insertFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, copiedTableSchema, Restriction, allColumns,null, StartEndTs);
                con.ExecuteNonQuerySQL(insertFromTable);

                this.schemaManager.StoreSchema(currentSchema, copyTable, con, c);
                transaction.Commit();
            });
        

        }

    }
}
