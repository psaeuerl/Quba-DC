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
                List<String> columns = new List<string>();
                columns.AddRange(createTable.Columns);
                columns.Add("startts");
                columns.Add("endts");
                columns.Add("guid");

                List<String> columndefinitions = new List<string>();
                columndefinitions.AddRange(createTable.ColumnDefinitions);
                columndefinitions.AddRange(SMORenderer.GetHistoryTableColumns());
                //Create History Table
                CreateTable ctHistTable = new CreateTable()
                {
                    ColumnDefinitions = columndefinitions.ToArray(),
                    Columns = columns.ToArray(),
                    Schema = createTable.Schema,
                    TableName = createTable.TableName + "_hist"
                };
                String histCreateTable = SMORenderer.RenderCreateTable(ctHistTable,true);
                //Query Schema, add tables to Schema
                //Commit everything
            });
        }
    }
}
