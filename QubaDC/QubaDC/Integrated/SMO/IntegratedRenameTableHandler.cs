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

namespace QubaDC.Separated.SMO
{
    public class IntegratedRenameTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedRenameTableHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(RenameTable renameTable)
        {

            //What to do here?
            //a.) Execute Rename Table
            //b.) Schemamanager => Add that new table points to old table
            //c.) Triggers can stay

            var con = (MySQLDataConnection)DataConnection;
            con.DoTransaction((transaction, c) =>
            {



                String renameTableSQL = SMORenderer.RenderRenameTable(renameTable);

                //Change Shchema    
                //take old table, remove it, add it with new names
                SchemaInfo xy = this.schemaManager.GetCurrentSchema();
                Schema x = xy.Schema;
                Table oldTable = new Table() { TableSchema = renameTable.OldSchema, TableName = renameTable.OldTableName };
                TableSchemaWithHistTable table = x.FindTable(oldTable);
                TableSchema tableHist = x.FindHistTable(oldTable);
                var actualTable = x.FindTable(oldTable);
                actualTable.Table.Name = renameTable.NewTableName;
                actualTable.Table.Schema = renameTable.NewSchema;

                //Renameing Table
                con.ExecuteQuery(renameTableSQL, c);

                this.schemaManager.StoreSchema(x, renameTable, con, c);

                transaction.Commit();

            });
        }

    }
}
