﻿using System;
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
    class SeparatedCopyTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedCopyTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(CopyTable createTable)
        {
            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data
            ;
            //var con = (MySQLDataConnection)DataConnection;
            //con.DoTransaction((transaction, c) =>
            //{
            //    //What to do?
            //    //Create Table normal
            //    //Create Table Hist
            //    //Create Trigger on normal
            //    String createBaseTable = SMORenderer.RenderCreateTable(createTable);
            //    ////Create History Table
            //    SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
            //    Schema x = xy.Schema;
            //    if (xy.ID == null)
            //    {
            //        x = new Schema();
            //        xy.ID = 0;
            //     }


            //    List<ColumnDefinition> columndefinitions = new List<ColumnDefinition>();
            //    columndefinitions.AddRange(createTable.Columns);
            //    columndefinitions.AddRange(SeparatedConstants.GetHistoryTableColumns());
            //    CreateTable ctHistTable = new CreateTable()
            //    {
            //        Columns = columndefinitions.ToArray(),
            //        Schema = createTable.Schema,
            //        TableName = createTable.TableName + "_" + xy.ID
            //    };
            //    String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);

            //    //INsert Trigger 
            //    String trigger = SMORenderer.RenderCreateInsertTrigger(createTable, ctHistTable);
            //    //Delete Trigger
            //    String deleteTrigger = SMORenderer.RenderCreateDeleteTrigger(createTable, ctHistTable);
            //    //Update Trigger
            //    String UpdateTrigger = SMORenderer.RenderCreateUpdateTrigger(createTable, ctHistTable);


            //    //Manage Schema Statement
            //    x.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema());
            //    String updateSchema = this.schemaManager.GetInsertSchemaStatement(x, createTable);
             
            //    //Add tables
            //    con.ExecuteNonQuerySQL(createBaseTable, c);
            //    con.ExecuteNonQuerySQL(createHistTable, c);
            //    //Add Trigger
            //    con.ExecuteSQLScript(trigger, c);
            //    con.ExecuteSQLScript(deleteTrigger, c);
            //    con.ExecuteSQLScript(UpdateTrigger, c);

            //    //Store Schema
            //    con.ExecuteNonQuerySQL(updateSchema, c);
            //    transaction.Commit();
            //});
        }

    }
}
