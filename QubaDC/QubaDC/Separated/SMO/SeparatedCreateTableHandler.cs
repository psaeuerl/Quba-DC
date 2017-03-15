using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;

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
            ////1st start transaction
            //var con = (MySQLDataConnection)DataConnection;
            //con.DoTransaction((transaction,c) =>
            //{
            //    //Create Table
            //    String normalCreateTable = SMORenderer.RenderCreateTable(createTable);

            //    ////Create History Table
            //    List<ColumnDefinition> columndefinitions = new List<ColumnDefinition>();
            //    columndefinitions.AddRange(createTable.Columns);
            //    columndefinitions.AddRange(SeparatedConstants.GetHistoryTableColumns());
            //    CreateTable ctHistTable = new CreateTable()
            //    { 
            //        Columns = columndefinitions.ToArray(),
            //        Schema = createTable.Schema,
            //        TableName = createTable.TableName + "_hist"
            //    };
            //    String histCreateTable = SMORenderer.RenderCreateTable(ctHistTable,true);
            //    //Query Schema, add tables to Schema
            //    SchemaInfo xy =  this.schemaManager.GetCurrentSchema(c);
            //    Schema x = xy.Schema;
            //    if (xy.ID == null)
            //    {
            //        x = new Schema();
            //    }
  
            //    x.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema());
            //    String Statement = this.schemaManager.GetInsertSchemaStatement(x,createTable);
            //    //Commit everything
            //    this.DataConnection.ExecuteNonQuerySQL(normalCreateTable,c);
            //    this.DataConnection.ExecuteNonQuerySQL(histCreateTable,c);
            //    this.DataConnection.ExecuteInsert(Statement,c);
            //    transaction.Commit();                
            //});
        }
    }
}
