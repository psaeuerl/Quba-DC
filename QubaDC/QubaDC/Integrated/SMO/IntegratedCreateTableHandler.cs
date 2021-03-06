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
using static QubaDC.Integrated.SMO.IntegratedSMOExecuter;

namespace QubaDC.Integrated.SMO
{
    public class IntegratedCreateTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedCreateTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager timeManager)
        {
            this.con = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = timeManager;
        }

        public DataConnection con { get; private set; }
        public SMORenderer SMORenderer { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }

        internal void Handle(CreateTable createTable)
        {



            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                ColumnDefinition[] histColumns = IntegratedConstants.GetHistoryTableColumns();
                CreateTable newCt = JsonSerializer.CopyItem<CreateTable>(createTable);
                newCt.Columns = createTable.Columns.Union(histColumns).ToArray();
                String createBaseTable = SMORenderer.RenderCreateTable(newCt);

                CreateTable ctHistTable = CreateHistTable(createTable, currentSchemaInfo);
                String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);
                String createMetaTable = MetaManager.GetCreateMetaTableFor(newCt.Schema, newCt.TableName);
                Table metaTable = MetaManager.GetMetaTableFor(newCt.Schema, newCt.TableName);
                //Manage Schema Statement
                currentSchemaInfo.Schema.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema(), metaTable);
                //String updateSchema = this.schemaManager.GetInsertSchemaStatement(x, createTable);


                String baseInsert = MetaManager.GetStartInsertFor(newCt.Schema, newCt.TableName); ;

                String[] Statements = new String[]
                {
                    createBaseTable,
                    createHistTable,
                    createMetaTable,
                    baseInsert
                };

                return new UpdateSchema()
                {
                    newSchema = currentSchemaInfo.Schema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { },
                    TablesToUnlock = new Table[] { }
                };
            };


            IntegratedSMOExecuter.Execute(
                this.SMORenderer,
                this.con,
                 this.schemaManager,
                 createTable,
                 f,
                 (s) =>  System.Diagnostics.Debug.WriteLine(s)
                 , MetaManager);
        }



        private static CreateTable CreateHistTable(CreateTable createTable, SchemaInfo xy)
        {

            CreateTable ctHistTable = new CreateTable()
            {
                Columns = createTable.Columns.ToArray(),
                Schema = createTable.Schema,
                TableName = createTable.TableName + "_" + xy.ID
            };
            return ctHistTable;
        }
    }
}
