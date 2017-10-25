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
using QubaDC.Restrictions;

namespace QubaDC.Separated.SMO
{
    class SeparatedJoinTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedJoinTableHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer, TableMetadataManager metaManager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = metaManager;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }

        internal void Handle(JoinTable jointable)
        {

            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data twice!



            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                String updateTime = this.SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");            
                Schema currentSchema = currentSchemaInfo.Schema;


                TableSchemaWithHistTable firstTable = currentSchemaInfo.Schema.FindTable(jointable.FirstSchema, jointable.FirstTableName);
                TableSchema firstHistTable = currentSchemaInfo.Schema.FindHistTable(firstTable.Table.ToTable());

                TableSchemaWithHistTable secondTable = currentSchemaInfo.Schema.FindTable(jointable.FirstSchema, jointable.SecondTableName);
                TableSchema secondHistTable = currentSchemaInfo.Schema.FindHistTable(secondTable.Table.ToTable());


                var joinedTableSchema = new TableSchema()
                {
                    Columns = firstTable.Table.Columns.Union(secondTable.Table.Columns).Distinct().ToArray(),
                    Name = jointable.ResultTableName,
                    Schema = jointable.ResultSchema,
                    ColumnDefinitions = firstTable.Table.ColumnDefinitions.Union(secondTable.Table.ColumnDefinitions).GroupBy(x => x.ColumName).Select(x => x.First()).ToArray()
                };
                var joinedTableHistSchema = new TableSchema()
                {
                    Columns = firstTable.Table.Columns.Union(secondHistTable.Columns).Distinct().ToArray(),
                    Name = jointable.ResultTableName + "_" + currentSchemaInfo.ID,
                    Schema = jointable.ResultSchema,
                    ColumnDefinitions = firstTable.Table.ColumnDefinitions.Union(secondHistTable.ColumnDefinitions).GroupBy(x => x.ColumName).Select(x => x.First()).ToArray()
                };
                Table firstTableMeta = this.MetaManager.GetMetaTableFor(joinedTableSchema);
                currentSchema.AddTable(joinedTableSchema, joinedTableHistSchema, firstTableMeta);
                currentSchema.RemoveTable(firstTable.Table.ToTable());
                currentSchema.RemoveTable(secondTable.Table.ToTable());

                String createJoinedTable =  CreateJoinedTable(firstTable.Table, secondTable.Table, joinedTableSchema);
                CreateTable ctHist = CreateHistTable(joinedTableSchema, currentSchemaInfo);
                String createJoinedHistTable = SMORenderer.RenderCreateTable(ctHist, true);


                ////Insert data from old to new
                String select = CreateSelectForTables(firstTable.Table, secondTable.Table, jointable.FirstTableAlias, jointable.SecondTableAlias, jointable.JoinRestriction);
                String insertJoinedData = SMORenderer.RenderInsertToTableFromSelect(joinedTableSchema, select);

                String DropFirstTable = SMORenderer.RenderDropTable(firstTable.Table.Schema, firstTable.Table.Name);
                String DropSecondTable = SMORenderer.RenderDropTable(secondTable.Table.Schema, secondTable.Table.Name);


                //Table firstTableMeta = this.MetaManager.GetMetaTableFor(joinedTableSchema);
                //currentSchema.AddTable(joinedTableSchema, joinedTableHistSchema, firstTableMeta);
                //currentSchema.RemoveTable(firstTable.Table.ToTable());
                //currentSchema.RemoveTable(secondTable.Table.ToTable());

                //String createJoinTable = CreateJoinedTable(firstTable.Table, secondTable.Table, joinedTableSchema);
                //String createHistJoinedTable = CreateJoinedTable(firstTable.Table, secondTable.Table, joinedTableHistSchema);

                //////Insert data from old to new
                //var RestrictionT1 = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(jointable.FirstTableAlias, updateTime);
                //var RestrictionT2 = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(jointable.SecondTableAlias, updateTime);
                //var Restriction = new AndRestriction() { Restrictions = new QubaDC.Restriction[] { RestrictionT1, RestrictionT1, jointable.JoinRestriction } };

                //var StartEndTs = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updateTime, ColumnName = IntegratedConstants.StartTS },
                //                        new LiteralColumn() { ColumnLiteral = "NULL", ColumnName = IntegratedConstants.EndTS } };

                //TableSchema isnertWithStartts = new TableSchema()
                //{
                //    Columns = joinedTableSchema.Columns.Concat(new String[] { IntegratedConstants.StartTS, IntegratedConstants.EndTS }).ToArray(),
                //    Name = joinedTableSchema.Name,
                //    Schema = joinedTableSchema.Schema
                //};
                //String select = CreateSelectForTables(firstTable.Table, secondTable.Table, jointable.FirstTableAlias, jointable.SecondTableAlias, Restriction, StartEndTs);
                //String insertFromFirstTable = SMORenderer.RenderInsertToTableFromSelect(isnertWithStartts, select);

                //String[] renameFirstToHist = DropHistTableRenameCurrentToHist(firstTable);
                //String[] renameSecondToHist = DropHistTableRenameCurrentToHist(secondTable);




                ////con.ExecuteNonQuerySQL(insertFromTable);
                //// String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(new Table() { TableName = addColumn.TableName, TableSchema = addColumn.Schema }, updateTime);

                String createJoinedMeta = this.MetaManager.GetCreateMetaTableFor(joinedTableSchema.Schema, joinedTableSchema.Name);
                String InsertJoinedMeta = this.MetaManager.GetStartInsertFor(joinedTableSchema.Schema, joinedTableSchema.Name);

                String dropFirstMetaTable = SMORenderer.RenderDropTable(firstTable.MetaTableSchema, firstTable.MetaTableName);
                String dropSecondlMetaTable = SMORenderer.RenderDropTable(secondTable.MetaTableSchema, secondTable.MetaTableName);

                var StartEndTs = new String[] { updateTime, "NULL" };
                String insertHistJoinedTable = SMORenderer.RenderInsertFromOneTableToOther(joinedTableSchema, joinedTableHistSchema, null, joinedTableSchema.Columns, null, StartEndTs);

                //String[] Statements = new String[]
                //{
                //    createJoinTable,
                //    createHistJoinedTable,
                //    insertFromFirstTable
                //}
                //.Concat(renameFirstToHist)
                //.Concat(renameSecondToHist)
                //.Concat(new String[]
                //{
                //    createJoinedMeta,
                //    InsertJoinedMeta,
                //    dropFirstMetaTable,
                //    dropSecondlMetaTable
                //}).ToArray();
                ////.Concat(dropOriginalHist)

                String[] Statements = new String[]
                {
                    createJoinedTable,
                    createJoinedHistTable,
                    insertJoinedData,
                    DropFirstTable,
                    DropSecondTable,
                    createJoinedMeta,
                    InsertJoinedMeta,
                    dropFirstMetaTable,
                    dropSecondlMetaTable,
                    insertHistJoinedTable
                };

                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { firstTable.ToTable(), secondTable.ToTable() },
                    TablesToUnlock = new Table[] { }
                };
            };


            SeparatedSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 jointable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , this.MetaManager);
        }

        private String CreateJoinedTable(TableSchema firstTable, TableSchema secondTable, TableSchema joinedTableSchema)
        {
            string select = CreateSelectForTables(firstTable, secondTable,"t1","t2", new OperatorRestriction()
            {
                LHS = new LiteralOperand() { Literal = "1" },
                Op = RestrictionOperator.Equals,
                RHS = new LiteralOperand() { Literal = "2" }
            });

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(joinedTableSchema.Schema, joinedTableSchema.Name, select);
            return copyTableSQL;
        }

        private string CreateSelectForTables(TableSchema firstTable, TableSchema secondTable, String firstTableRename, String SecondTableRename, Restriction r)
        {
            String[] columnsFromTable1 = firstTable.Columns.ToArray();
            String[] columnsFromTable2 = secondTable.Columns.Except(columnsFromTable1).ToArray();

            ColumnReference[] refTable1 = columnsFromTable1.Select(x => new ColumnReference() { ColumnName = x, TableReference = firstTableRename }).ToArray();
            ColumnReference[] refTable2 = columnsFromTable2.Select(x => new ColumnReference() { ColumnName = x, TableReference = SecondTableRename }).ToArray();

            SelectOperation s = new SelectOperation()
            {
                Columns = refTable1.Union(refTable2).ToArray(),
                FromTable = new FromTable() { TableName = firstTable.Name, TableSchema = firstTable.Schema, TableAlias = firstTableRename },
                JoinedTables = new JoinedTable[]
                   {
                            new JoinedTable()
                            {
                                 Join = JoinType.NoJoin,
                                  JoinCondition = null,
                                   TableName = secondTable.Name,
                                    TableSchema = secondTable.Schema,
                                     TableAlias = SecondTableRename
                            }
                   },
                Restriction = r
            };

            String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);
            return select;
        }
        private static CreateTable CreateHistTable(TableSchema createTable, SchemaInfo xy)
        {            
            List<ColumnDefinition> columndefinitions = new List<ColumnDefinition>();
            columndefinitions.AddRange(createTable.ColumnDefinitions);
            columndefinitions.AddRange(SeparatedConstants.GetHistoryTableColumns());
            CreateTable ctHistTable = new CreateTable()
            {
                Columns = columndefinitions.ToArray(),
                Schema = createTable.Schema,
                TableName = createTable.Name + "_" + xy.ID
            };
            return ctHistTable;
        }
    }
}
