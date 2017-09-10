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
    public class IntegratedDropTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedDropTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(DropTable dropTable)
        {
            //What to do here?
            //a.) Drop Table
            //b.) Schemamanager => RemoveTable            

            var con = (MySQLDataConnection)DataConnection;
            con.DoTransaction((transaction, c) =>
            {
                SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);


                TableSchemaWithHistTable originalTable = xy.Schema.FindTable(dropTable.Schema, dropTable.TableName);
                TableSchema originalHistTable = xy.Schema.FindHistTable(originalTable.Table.ToTable());

                String dropOriginalHistTable = SMORenderer.RenderDropTable(originalHistTable.Schema, originalHistTable.Name);
                con.ExecuteNonQuerySQL(dropOriginalHistTable);

                String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
                {
                    NewSchema = originalTable.HistTableSchema,
                    NewTableName = originalTable.HistTableName,
                    OldSchema = originalTable.Table.Schema,
                    OldTableName = originalTable.Table.Name
                });
                con.ExecuteNonQuerySQL(renameTableSQL);

                Schema x = xy.Schema;
                Table oldTable = new Table() { TableSchema = dropTable.Schema, TableName = dropTable.TableName };
                x.RemoveTable(oldTable);

                //Storing Schema
                this.schemaManager.StoreSchema(x, dropTable, con, c);
                transaction.Commit();

            });
        }

    }
}
