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
using QubaDC.Restrictions;
using QubaDC.Hybrid;
using QubaDC.Hybrid.SMO;

namespace QubaDC.Hybrid.SMO
{
    class HybridJoinTableHandler
    {
        private SchemaManager schemaManager;

        public HybridJoinTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager meta)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = meta;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

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

               String createJoinedTable =   CreateJoinedTable( firstTable.Table, secondTable.Table, joinedTableSchema, true, null);
                String createJoinHistTable =  CreateJoinedTable(firstTable.Table, secondHistTable, joinedTableHistSchema, false, null);
                String createJoinedMetaTable = this.MetaManager.GetCreateMetaTableFor(joinedTableSchema.Schema, joinedTableSchema.Name);

                ////Insert data from old to new
                String select = CreateSelectForTables(firstTable.Table, secondTable.Table, jointable.FirstTableAlias, jointable.SecondTableAlias, jointable.JoinRestriction, false, updateTime);
                TableSchema copiedWithStartTS = new TableSchema()
                {
                    Columns = joinedTableSchema.Columns.Concat(new String[] { HybridConstants.StartTS, }).ToArray(),
                    Name = joinedTableSchema.Name,
                    Schema = joinedTableSchema.Schema
                };
                String insertFromFirstTable = SMORenderer.RenderInsertToTableFromSelect(copiedWithStartTS, select);

                String DropFirstTable = SMORenderer.RenderDropTable(firstTable.Table.Schema, firstTable.Table.Name);
                String DropSecondTable = SMORenderer.RenderDropTable(secondTable.Table.Schema, secondTable.Table.Name);



                //////Insert data to hist
                SelectOperation selectFirst = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "t1" } },
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updateTime, ColumnName = "ut" } },
                    FromTable = new FromTable() { TableAlias = "t1", TableName = firstTable.Table.Name, TableSchema = firstTable.Table.Schema },
                };
                String selectFirstCurrent = this.SMORenderer.CRUDHandler.RenderSelectOperation(selectFirst);
                String isnertIntoHistFirstTable = this.SMORenderer.CRUDRenderer.RenderInsertSelect(new Table()
                { TableSchema = firstHistTable.Schema, TableName = firstHistTable.Name },
                    null,
                    selectFirstCurrent);

                SelectOperation selectSecond = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "t1" } },
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updateTime, ColumnName = "ut" } },
                    FromTable = new FromTable() { TableAlias = "t1", TableName = secondTable.Table.Name, TableSchema = secondTable.Table.Schema },
                };
                String selectSecondCurrent = this.SMORenderer.CRUDHandler.RenderSelectOperation(selectSecond);
                String insertIntoSecondHist = this.SMORenderer.CRUDRenderer.RenderInsertSelect(new Table()
                { TableSchema = secondHistTable.Schema, TableName = secondHistTable.Name },
                    null,
                    selectSecondCurrent);

                String DropFirstMeta = SMORenderer.RenderDropTable(firstTable.MetaTableSchema, firstTable.MetaTableName);
                String DropSecondMeta = SMORenderer.RenderDropTable(secondTable.MetaTableSchema, secondTable.MetaTableName);

                String isnertMetadataJoinedMeta = this.MetaManager.GetStartInsertFor(joinedTableSchema.Schema, joinedTableSchema.Name);


                String[] Statements =
                                    new String[]
                                        {
                                            createJoinedTable,
                                            createJoinHistTable,
                                            createJoinedMetaTable,
                                            insertFromFirstTable,
                                            isnertIntoHistFirstTable,
                                            insertIntoSecondHist,
                                            DropFirstTable,
                                            DropSecondTable,
                                            DropFirstMeta,
                                            DropSecondMeta,
                                            isnertMetadataJoinedMeta,
                                        };


                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { firstTable.ToTable(), secondTable.ToTable() },
                    TablesToUnlock = new Table[] { }
                };
            };


            HybridSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 jointable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , this.MetaManager);


        }

        private String CreateJoinedTable( TableSchema firstTable, TableSchema secondTable, TableSchema joinedTableSchema, Boolean IncludeTSColumn, String updateTime)
        {
            string select = CreateSelectForTables(firstTable, secondTable,"t1","t2", new OperatorRestriction()
            {
                LHS = new LiteralOperand() { Literal = "1" },
                Op = RestrictionOperator.Equals,
                RHS = new LiteralOperand() { Literal = "2" }
            },IncludeTSColumn, updateTime
            );

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(joinedTableSchema.Schema, joinedTableSchema.Name, select);
            return copyTableSQL;            
        }

        private string CreateSelectForTables(TableSchema firstTable, TableSchema secondTable, String firstTableRename, String SecondTableRename, Restriction r, Boolean IncludeTSColumn,String updatetime)
        {
            String[] columnsFromTable1 = firstTable.Columns.ToArray();
            String[] columnsFromTable2 = secondTable.Columns.Except(columnsFromTable1).ToArray();

            ColumnReference[] refTable1 = columnsFromTable1.Select(x => new ColumnReference() { ColumnName = x, TableReference = firstTableRename }).ToArray();
            ColumnReference[] refTable2 = columnsFromTable2.Select(x => new ColumnReference() { ColumnName = x, TableReference = SecondTableRename }).ToArray();
            ColumnDefinition cf = HybridConstants.GetStartColumn();
            ColumnReference[] tsColumns = new ColumnReference[] { };
            if (IncludeTSColumn)
            {
                tsColumns = new ColumnReference[] { new ColumnReference() { ColumnName = cf.ColumName, TableReference = firstTableRename } };
            }

            LiteralColumn[] cols = new LiteralColumn[] { };
            if(!String.IsNullOrWhiteSpace(updatetime))
            {
                cols = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updatetime, ColumnName = "startts" } };
            }
 
            SelectOperation s = new SelectOperation()
            {
                Columns = refTable1.Union(refTable2)
                .Union(tsColumns)
                .ToArray(),
                LiteralColumns = cols,
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
    }
}
