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
    public class SeparatedDropTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedDropTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
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

                String dropTableSql = SMORenderer.RenderDropTable(dropTable.Schema,dropTable.TableName);


                SchemaInfo xy = this.schemaManager.GetCurrentSchema();
                Schema x = xy.Schema;
                Table oldTable = new Table() { TableSchema = dropTable.Schema, TableName = dropTable.TableName };

                x.RemoveTable(oldTable);

                String updateSchema = this.schemaManager.GetInsertSchemaStatement(x, dropTable);

                //Renameing Table
                con.ExecuteQuery(dropTableSql, c);

                //Storing Schema
                con.ExecuteNonQuerySQL(updateSchema, c);
                transaction.Commit();

            });
        }

    }
}
