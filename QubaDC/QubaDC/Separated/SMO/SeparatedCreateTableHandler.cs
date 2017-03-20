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
            Guard.StateTrue(createTable.PrimaryKey.Length > 0, "Primary Key Requiered");

            var con = (MySQLDataConnection)DataConnection;
            con.DoTransaction((transaction, c) =>
            {
                //What to do?
                //Create Table normal
                //Create Table Hist
                //Create Trigger on normal
                String createBaseTable = SMORenderer.RenderCreateTable(createTable);
                ////Create History Table
                SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
                Schema x = xy.Schema;
                if (xy.ID == null)
                {
                    x = new Schema();
                    xy.ID = 0;
                 }


                List<ColumnDefinition> columndefinitions = new List<ColumnDefinition>();
                columndefinitions.AddRange(createTable.Columns);
                columndefinitions.AddRange(SeparatedConstants.GetHistoryTableColumns());
                CreateTable ctHistTable = new CreateTable()
                {
                    Columns = columndefinitions.ToArray(),
                    Schema = createTable.Schema,
                    TableName = createTable.TableName + "_" + xy.ID
                };
                String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);

                //INsert Trigger 
                String trigger = SMORenderer.RenderCreateInsertTrigger(createTable, ctHistTable);

                //Manage Schema Statement
                x.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema());
                String updateSchema = this.schemaManager.GetInsertSchemaStatement(x, createTable);
             
                //Add tables
                con.ExecuteNonQuerySQL(createBaseTable, c);
                con.ExecuteNonQuerySQL(createHistTable, c);
                //Add Trigger
                con.ExecuteSQLScript(trigger, c);
                //Store Schema
                con.ExecuteNonQuerySQL(updateSchema, c);
                transaction.Commit();
            });
        }

    }
}
