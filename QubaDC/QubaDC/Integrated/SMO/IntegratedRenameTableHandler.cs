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
using QubaDC.Integrated.SMO;
using QubaDC.Integrated;

namespace QubaDC.Integrated.SMO
{
    public class IntegratedRenameTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedRenameTableHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer, TableMetadataManager metaManager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = metaManager;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(RenameTable renameTable)
        {

            //What to do here?
            //a.) Execute Rename Table
            //b.) Schemamanager => Add that new table points to old table
            //c.) Triggers can stay

            //var con = (MySQLDataConnection)DataConnection;
            //con.DoTransaction((transaction, c) =>
            //{




            //    //Renameing Table
            //    con.ExecuteQuery(renameTableSQL, c);

            //    this.schemaManager.StoreSchema(x, renameTable, con, c);

            //    transaction.Commit();

            //});

            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                String renameTableSQL = SMORenderer.RenderRenameTable(renameTable);

                //Change Shchema    
                //take old table, remove it, add it with new names
                SchemaInfo xy = currentSchemaInfo;
                Schema updatedSchema = xy.Schema;
                Table oldTable = new Table() { TableSchema = renameTable.OldSchema, TableName = renameTable.OldTableName };
                TableSchemaWithHistTable table = updatedSchema.FindTable(oldTable);
                TableSchema tableHist = updatedSchema.FindHistTable(oldTable);
                var actualTable = updatedSchema.FindTable(oldTable);
                actualTable.Table.Name = renameTable.NewTableName;
                actualTable.Table.Schema = renameTable.NewSchema;

                actualTable.MetaTableName = this.MetaManager.GetMetaTableFor(actualTable.Table).TableName;
                actualTable.MetaTableSchema = actualTable.Table.Schema;

                String updateTime = this.SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");

                String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(actualTable.ToTable(), updateTime);
                String renameUpdateTable = SMORenderer.RenderRenameTable(new RenameTable()
                {
                     NewSchema = actualTable.MetaTableSchema,
                     NewTableName = actualTable.MetaTableName,
                      OldSchema = actualTable.MetaTableSchema,
                        OldTableName = this.MetaManager.GetMetaTableFor(oldTable.TableSchema,oldTable.TableName).TableName
                });

                String[] Statements = new String[]
                {
                    renameTableSQL,
                    renameUpdateTable,
                    updateLastUpdate
                };

                return new UpdateSchema()
                {
                    newSchema = currentSchemaInfo.Schema,
                    UpdateStatements = Statements,
                     MetaTablesToLock = new Table[] { oldTable },
                      TablesToUnlock = new Table[] { actualTable.ToTable() }
                };
            };


            IntegratedSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 renameTable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , MetaManager
                 );
        }

    }
}
