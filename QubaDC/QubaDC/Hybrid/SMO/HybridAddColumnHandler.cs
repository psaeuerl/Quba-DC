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
using QubaDC.Hybrid.SMO;
using QubaDC.Hybrid;

namespace QubaDC.Hybrid.SMO
{
    class HybridAddColumnHandler
    {
        private SchemaManager schemaManager;

        public HybridAddColumnHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager m)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = m;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(AddColum addColumn)
        {
            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data

            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                String insertTimeVariable = "updateTime";
                String updateTimeVariable = this.SMORenderer.CRUDRenderer.GetSQLVariable(insertTimeVariable);

                String updateTime = this.SMORenderer.CRUDRenderer.GetSQLVariable("updateTime");
                Schema currentSchema = currentSchemaInfo.Schema;

                TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(addColumn.Schema, addColumn.TableName);
                TableSchema originalHistTable = currentSchemaInfo.Schema.FindHistTable(originalTable.Table.ToTable());

                var copiedTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Union(new String[] { addColumn.Column.ColumName }).ToArray(),
                    Name = originalTable.Table.Name,
                    Schema = originalTable.Table.Schema,
                    ColumnDefinitions = originalTable.Table.ColumnDefinitions.Union(new ColumnDefinition[] { addColumn.Column }).ToArray(),
                };
                var copiedHistSchema = new TableSchema()
                {
                    Columns = copiedTableSchema.Columns.Union(originalHistTable.Columns.Except(copiedTableSchema.Columns)).ToArray(),
                    Name = originalTable.Table.Name + "_" + currentSchemaInfo.ID,
                    Schema = originalTable.Table.Schema,
                    ColumnDefinitions = copiedTableSchema.ColumnDefinitions.Union(originalHistTable.ColumnDefinitions
                                            .Where(x => copiedTableSchema.Columns.Contains(x.ColumName) == false)).ToArray()
                };

                Guard.StateTrue(copiedTableSchema.Columns.Count() == originalTable.Table.Columns.Count() + 1, "Could add new column: " + addColumn.Column);
                Guard.StateTrue(copiedHistSchema.Columns.Count() == originalHistTable.Columns.Count() + 1, "Could add new column: " + addColumn.Column);
                Guard.StateTrue(copiedTableSchema.ColumnDefinitions.Count() == originalTable.Table.ColumnDefinitions.Count() + 1, "Could add new column: " + addColumn.Column);
                Guard.StateTrue(copiedHistSchema.ColumnDefinitions.Count() == originalHistTable.ColumnDefinitions.Count() + 1, "Could add new column: " + addColumn.Column);

                currentSchema.RemoveTable(originalTable.Table.ToTable());
                Table metaTable = this.MetaManager.GetMetaTableFor(copiedTableSchema);
                currentSchema.AddTable(copiedTableSchema, copiedHistSchema, metaTable);

                String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
                {
                    NewSchema = originalTable.Table.Schema,
                    NewTableName = originalTable.Table.Name + "_old",
                    OldSchema = originalTable.Table.Schema,
                    OldTableName = originalTable.Table.Name
                });

                String copyBaseTable = CopyTable( originalTable.Table, copiedTableSchema, true, false);
                String addColumnBase = AlterTable( copiedTableSchema, addColumn.Column);
                String addStartTs = AlterTable(copiedTableSchema, HybridConstants.GetStartColumn()); //STARTTS

                String copyHistTable =  CopyTable(originalTable.Table, copiedHistSchema, false, false);
                String addColumnHistTable =  AlterTable(copiedHistSchema, addColumn.Column);
                String addStartTsHist = AlterTable(copiedHistSchema, HybridConstants.GetStartColumn()); //STARTTS
                String addEndTSHist = AlterTable(copiedHistSchema, HybridConstants.GetEndColumn()); //ENDTS

                ////Insert data from old to new
                SelectOperation s = new SelectOperation()
                {
                    Columns = originalTable.Table.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = addColumn.InitalValue, ColumnName = addColumn.Column.ColumName }
                                                          ,new LiteralColumn() {ColumnLiteral =  updateTime, ColumnName = "ut" } },
                    FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Table.Name + "_old", TableSchema = originalTable.Table.Schema }
                };
                String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);
                TableSchema copiedWithStartTS = new TableSchema()
                {
                    Columns = copiedTableSchema.Columns.Concat(new String[] { HybridConstants.StartTS, }).ToArray(),
                    Name = copiedTableSchema.Name,
                    Schema = copiedTableSchema.Schema
                };
                String insertFromTableToNew = SMORenderer.RenderInsertToTableFromSelect(copiedWithStartTS, select);


                //Insert data to hist
                SelectOperation selectCurrentFromBaseTable = new SelectOperation()
                {
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "t1"} },
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updateTimeVariable, ColumnName = "ut" } },
                    FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Table.Name + "_old", TableSchema = originalTable.Table.Schema },
                };
                String selectCurrentWithUT = this.SMORenderer.CRUDHandler.RenderSelectOperation(selectCurrentFromBaseTable);
                String isnertIntoHist = this.SMORenderer.CRUDRenderer.RenderInsertSelect(new Table()
                { TableSchema = originalHistTable.Schema, TableName = originalHistTable.Name },
                    null,
                    selectCurrentWithUT);
                String insertFromTableToHist = SMORenderer.RenderInsertToTableFromSelect(originalHistTable, selectCurrentWithUT);


                String dropTableSql = SMORenderer.RenderDropTable(originalTable.Table.Schema, originalTable.Table.Name + "_old");
                String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(new Table() { TableName = addColumn.TableName, TableSchema = addColumn.Schema }, updateTime);

                String[] Statements = new String[]
                {
                    //insert data into hist table
                   renameTableSQL,
                   copyBaseTable,
                   insertFromTableToHist,
                   addColumnBase,
                   addStartTs,
                   insertFromTableToNew,

                   copyHistTable,
                   addColumnHistTable,
                   addStartTsHist,
                   addEndTSHist,



                   dropTableSql,
                   updateLastUpdate
                };

                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { originalTable.ToTable() },
                    TablesToUnlock = new Table[] { originalTable.ToTable() }
                };
            };


            HybridSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 addColumn,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , this.MetaManager);

        }

        private String AlterTable( TableSchema copiedTableSchema, ColumnDefinition column)
        {
            String statement = this.SMORenderer.RenderAddColumn(copiedTableSchema, column);
            return statement;
        }

        private String CopyTable( TableSchema originalTable, TableSchema copiedTableSchema, Boolean includeOldTable, Boolean includeTsColumn)
        {
            SelectOperation s = new SelectOperation()
            {
                Restriction = new OperatorRestriction()
                {
                    LHS = new LiteralOperand() { Literal = "1" },
                    Op = RestrictionOperator.Equals,
                    RHS = new LiteralOperand() { Literal = "2" }
                },
                Columns = originalTable.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Name + (includeOldTable ? "_old": ""), TableSchema = originalTable.Schema }
            };
            if (includeTsColumn)
                s.Columns = s.Columns.Union(new ColumnReference[] { new ColumnReference() { ColumnName = Hybrid.HybridConstants.StartTS, TableReference = s.FromTable.TableAlias } }).ToArray();
            String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(copiedTableSchema.Schema, copiedTableSchema.Name, select);
            return copyTableSQL;
        }
    }
}
