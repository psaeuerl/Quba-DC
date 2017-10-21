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
    class HybridDropColumnHandler
    {
        private SchemaManager schemaManager;

        public HybridDropColumnHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager meta)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = meta;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(DropColumn dropColumn)
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

                TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(dropColumn.Schema, dropColumn.TableName);
                TableSchema originalHistTable = currentSchemaInfo.Schema.FindHistTable(originalTable.Table.ToTable());

                var copiedTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Where(x => x != dropColumn.Column).ToArray(),
                    Name = originalTable.Table.Name,
                    Schema = originalTable.Table.Schema,
                    ColumnDefinitions = originalTable.Table.ColumnDefinitions.Where(x => x.ColumName != dropColumn.Column).ToArray(),
                };
                var copiedHistSchema = new TableSchema()
                {
                    Columns = originalHistTable.Columns.Where(x => x != dropColumn.Column).ToArray(),
                    Name = originalTable.Table.Name + "_" + currentSchemaInfo.ID,
                    Schema = originalTable.Table.Schema,
                    ColumnDefinitions = originalHistTable.ColumnDefinitions.Where(x => x.ColumName != dropColumn.Column).ToArray(),
                };

                Guard.StateTrue(copiedTableSchema.Columns.Count() + 1 == originalTable.Table.Columns.Count(), "Could not find column: " + dropColumn.Column);
                Guard.StateTrue(copiedHistSchema.Columns.Count() + 1 == originalHistTable.Columns.Count(), "Could not find column: " + dropColumn.Column);
                Guard.StateTrue(copiedTableSchema.ColumnDefinitions.Count() + 1 == originalTable.Table.ColumnDefinitions.Count(), "Could not find column: " + dropColumn.Column);
                Guard.StateTrue(copiedHistSchema.ColumnDefinitions.Count() + 1 == originalHistTable.ColumnDefinitions.Count(), "Could not find column: " + dropColumn.Column);

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

                String copyBaseTable = CopyTable(originalTable.Table, copiedTableSchema, true, true);
                //String addColumnBase = AlterTable(copiedTableSchema, addColumn.Column);
                //String addStartTs = AlterTable(copiedTableSchema, HybridConstants.GetStartColumn()); //STARTTS

                String copyHistTable = CopyTable(originalTable.Table, copiedHistSchema, false, false);
                //String addColumnHistTable = AlterTable(copiedHistSchema, addColumn.Column);
                String addStartTsHist = AlterTable(copiedHistSchema, HybridConstants.GetStartColumn()); //STARTTS
                String addEndTSHist = AlterTable(copiedHistSchema, HybridConstants.GetEndColumn()); //ENDTS

                ////Insert data from old to new
                SelectOperation s = new SelectOperation()
                {
                    Columns = copiedTableSchema.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = updateTimeVariable, ColumnName = "ut" } },
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
                    Columns = new ColumnReference[] { new ColumnReference() { ColumnName = "*", TableReference = "t1" } },
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
                String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(new Table() { TableName = dropColumn.TableName, TableSchema = dropColumn.Schema }, updateTime);

                String[] Statements = new String[]
                {
                    //insert data into hist table
                   renameTableSQL,
                   copyBaseTable,
                   insertFromTableToHist,

                   insertFromTableToNew,

                   copyHistTable,
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
                 dropColumn,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , this.MetaManager);
       
        }

        private String AlterTable(TableSchema copiedTableSchema, ColumnDefinition column)
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
