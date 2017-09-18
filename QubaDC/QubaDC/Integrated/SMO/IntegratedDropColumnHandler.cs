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
using QubaDC.Integrated;
using QubaDC.Integrated.SMO;

namespace QubaDC.Separated.SMO
{
    class IntegratedDropColumnHandler
    {
        private SchemaManager schemaManager;

        public IntegratedDropColumnHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager mm)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = mm;
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
                Table metaTable = this.MetaManager.GetMetaTableFor(copiedTableSchema);
                currentSchema.RemoveTable(originalTable.Table.ToTable());
                currentSchema.AddTable(copiedTableSchema, copiedHistSchema,metaTable);

                String dropOriginalHistTable = SMORenderer.RenderDropTable(originalHistTable.Schema, originalHistTable.Name);
                //con.ExecuteNonQuerySQL(dropOriginalHistTable);

                String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
                {
                    NewSchema = originalTable.HistTableSchema,
                    NewTableName = originalTable.HistTableName,
                    OldSchema = originalTable.Table.Schema,
                    OldTableName = originalTable.Table.Name
                });
                //con.ExecuteNonQuerySQL(renameTableSQL);

                String copyBaseTable =  CopyTable(originalHistTable, copiedTableSchema, false);
                String copyHistTable = CopyTable( copiedTableSchema, copiedHistSchema, false);


                //String updateSchema = this.schemaManager.GetInsertSchemaStatement(currentSchema, dropColumn);




                //Insert data from old to new
                SelectOperation s = new SelectOperation()
                {
                    Columns = copiedTableSchema.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                    LiteralColumns = new LiteralColumn[] {
                        new LiteralColumn() { ColumnLiteral = updateTime, ColumnName = IntegratedConstants.StartTS },
                        new LiteralColumn() { ColumnLiteral = "NULL", ColumnName = IntegratedConstants.EndTS }},
                    FromTable = new FromTable() { TableAlias = "t1", TableName = originalHistTable.Name, TableSchema = originalTable.Table.Schema },
                    Restriction = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction("t1", updateTime)

                };
                String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);
                TableSchema isnertWithStartts = new TableSchema()
                {
                    Columns = copiedTableSchema.Columns.Concat(new String[] { IntegratedConstants.StartTS, IntegratedConstants.EndTS }).ToArray(),
                    Name = copiedTableSchema.Name,
                    Schema = copiedTableSchema.Schema
                };
                String insertFromTable = SMORenderer.RenderInsertToTableFromSelect(isnertWithStartts, select);
                //con.ExecuteNonQuerySQL(insertFromTable);
                String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(new Table() { TableName = dropColumn.TableName, TableSchema = dropColumn.Schema }, updateTime);

                String[] Statements = new String[]
                {
                   dropOriginalHistTable,
                   renameTableSQL,
                   copyBaseTable,
                   copyHistTable,
                   insertFromTable,
                   updateLastUpdate
                };

                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements
                };
            };


            IntegratedSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 dropColumn,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s));


        }


        private String CopyTable( TableSchema originalTable, TableSchema copiedTableSchema, Boolean includeold)
        {
            SelectOperation s = new SelectOperation()
            {
                Restriction = new OperatorRestriction()
                {
                    LHS = new LiteralOperand() { Literal = "1" },
                    Op = RestrictionOperator.Equals,
                    RHS = new LiteralOperand() { Literal = "2" }
                },
                Columns = copiedTableSchema.Columns.Concat(new String[] { IntegratedConstants.StartTS, IntegratedConstants.EndTS }).Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Name + (includeold ? "_old": ""), TableSchema = originalTable.Schema }
            };
            String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(copiedTableSchema.Schema, copiedTableSchema.Name, select);
            return copyTableSQL;
        }
    }
}
