﻿using System;
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
using QubaDC.Integrated.SMO;

namespace QubaDC.Integrated.SMO
{
    class IntegratedCopyTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedCopyTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager manager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = manager;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(CopyTable copyTable)
        {
            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data

            //var con = (MySQLDataConnection)DataConnection;
            //con.DoTransaction((transaction, c) =>
            //{

            //    SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
            //    Schema currentSchema = xy.Schema;


            //    TableSchemaWithHistTable originalTable = xy.Schema.FindTable(copyTable.Schema, copyTable.TableName);
            //    TableSchema originalHistTable = xy.Schema.FindHistTable(originalTable.Table.ToTable());

            //    var copiedTableSchema = new TableSchema()
            //    {
            //        Columns = originalTable.Table.Columns,
            //        Name = copyTable.CopiedTableName,
            //        Schema = copyTable.CopiedSchema,
            //         ColumnDefinitions = originalTable.Table.ColumnDefinitions,
            //    };
            //    var copiedHistSchema = new TableSchema()
            //    {
            //        Columns = originalHistTable.Columns,
            //        Name = copyTable.CopiedTableName + "_" + xy.ID,
            //        Schema = copyTable.CopiedSchema,
            //         ColumnDefinitions = originalHistTable.ColumnDefinitions
            //    };
            //    currentSchema.AddTable(copiedTableSchema, copiedHistSchema);

            //    //Copy Table without Triggers
            //    String copyTableSQL = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, copiedTableSchema.Schema, copiedTableSchema.Name);
            //    con.ExecuteNonQuerySQL(copyTableSQL, c);

            //    //Copy Hist Table without Triggers
            //    String copyHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, copiedHistSchema.Schema, copiedHistSchema.Name);

            //    con.ExecuteNonQuerySQL(copyHistTableSQL, c);

            //    //Insert data from old to new                
            //    String[]allColumns = originalTable.Table.Columns;
            //    var StartEndTs = new String[] { "NOW(3)", "NULL" };
            //    var Restriction = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(originalTable.Table.Name, "NOW(3)");

            //    String insertFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, copiedTableSchema, Restriction, allColumns,null, StartEndTs);
            //    con.ExecuteNonQuerySQL(insertFromTable);

            //    this.schemaManager.StoreSchema(currentSchema, copyTable, con, c);
            //    transaction.Commit();
            //});

            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                String updateTime = this.SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");
                Schema currentSchema = currentSchemaInfo.Schema;


                TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(copyTable.Schema, copyTable.TableName);
                TableSchema originalHistTable = currentSchemaInfo.Schema.FindHistTable(originalTable.Table.ToTable());

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
                    Name = copyTable.CopiedTableName + "_" + currentSchemaInfo.ID,
                    Schema = copyTable.CopiedSchema,
                    ColumnDefinitions = originalHistTable.ColumnDefinitions
                };
                Table firstTableMeta = this.MetaManager.GetMetaTableFor(copiedTableSchema);
                currentSchema.AddTable(copiedTableSchema, copiedHistSchema, firstTableMeta);

                //Copy Table without Triggers
                String copyTableSQL = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, copiedTableSchema.Schema, copiedTableSchema.Name);

                //Copy Hist Table without Triggers
                String copyHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, copiedHistSchema.Schema, copiedHistSchema.Name);


                //Insert data from old to new                
                String[] allColumns = originalTable.Table.Columns;                                             
                var StartEndTs = new String[] { updateTime, "NULL" };
                var Restriction = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(originalTable.Table.Name, updateTime);

                String insertFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, copiedTableSchema, Restriction, allColumns, null, StartEndTs);

                String createFirstMetaTable = this.MetaManager.GetCreateMetaTableFor(copiedTableSchema.Schema, copiedTableSchema.Name);

                String insertMetadataFirstTable = this.MetaManager.GetStartInsertFor(copiedTableSchema.Schema, copiedTableSchema.Name);

                String[] Statements = new String[]
                {
                    copyTableSQL,
                    copyHistTableSQL,
                    insertFromTable,
                    createFirstMetaTable,
                    insertMetadataFirstTable

                };


                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements,
                     MetaTablesToLock = new Table[] { originalTable.ToTable() },
                     TablesToUnlock = new Table[] { originalTable.ToTable() }
                };
            };


            IntegratedSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 copyTable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 ,this.MetaManager);

        }

    }
}
