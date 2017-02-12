using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;

namespace QubaDC.Separated.SMO
{
    class SeparatedCreateTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedCreateTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(CreateTable createTable)
        {
            //1st start transaction
            var con = (MySQLDataConnection)DataConnection;
            con.DoTransaction((transaction) =>
            {
                //Create Table
                String normalesCreateTable = SMORenderer.RenderCreateTable(createTable);
                //Create History Table
                //Query Schema, add tables to Schema
                //Commit everything
            });
        }
    }
}
