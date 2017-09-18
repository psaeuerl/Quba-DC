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
using QubaDC.Integrated.SMO;
using QubaDC.Integrated;

namespace QubaDC.Separated.SMO
{
    class IntegratedAddColumnHandler
    {
        private readonly TableMetadataManager MetaManager;
        private SchemaManager schemaManager;

        public IntegratedAddColumnHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer, TableMetadataManager manager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = manager;
        }

        public DataConnection DataConnection { get; private set; }
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

                Table metaTable = this.MetaManager.GetMetaTableFor(copiedTableSchema);
                currentSchema.RemoveTable(originalTable.Table.ToTable());
                currentSchema.AddTable(copiedTableSchema, copiedHistSchema, metaTable);



                String dropOriginalHistTable = SMORenderer.RenderDropTable(originalHistTable.Schema, originalHistTable.Name);
               // con.ExecuteNonQuerySQL(dropOriginalHistTable);

                String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
                {
                    NewSchema = originalTable.HistTableSchema,
                    NewTableName = originalTable.HistTableName,
                    OldSchema = originalTable.Table.Schema,
                    OldTableName = originalTable.Table.Name
                });
               // con.ExecuteNonQuerySQL(renameTableSQL);



                String copyBaseTable =  CopyTable(originalHistTable, copiedTableSchema, false);
                String addColumnBase =  AlterTable(copiedTableSchema, addColumn.Column);
                String addStartTs =  AlterTable(copiedTableSchema, IntegratedConstants.GetHistoryTableColumns()[0]); //STARTTS
                String addEndTS = AlterTable(copiedTableSchema, IntegratedConstants.GetHistoryTableColumns()[1]); //ENDTS
                String copyHistTable = CopyTable(copiedTableSchema, copiedHistSchema, false);





                ////Insert data from old to new
                SelectOperation s = new SelectOperation()
                {
                    Columns = originalTable.Table.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = addColumn.InitalValue, ColumnName = addColumn.Column.ColumName },
                        new LiteralColumn() { ColumnLiteral = updateTime, ColumnName = IntegratedConstants.StartTS },
                        new LiteralColumn() { ColumnLiteral = "NULL", ColumnName = IntegratedConstants.EndTS }},
                    FromTable = new FromTable() { TableAlias = "t1", TableName = originalHistTable.Name, TableSchema = originalHistTable.Schema },
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
                String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(new Table() { TableName = addColumn.TableName, TableSchema = addColumn.Schema }, updateTime);

                String[] Statements = new String[]
                {
                   dropOriginalHistTable,
                   renameTableSQL,
                   copyBaseTable,
                   addColumnBase,
                   addStartTs,
                   addEndTS,
                   copyHistTable,
                   insertFromTable,
                   updateLastUpdate
                };

                return new UpdateSchema()
                {
                    newSchema = currentSchema,
                    UpdateStatements = Statements,
                     MetaTablesToLock = new Table[] { originalTable.ToTable()},
                     TablesToUnlock = new Table[] { originalTable.ToTable() }
                };
            };


            IntegratedSMOExecuter.Execute(
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
                Columns = originalTable.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                FromTable = new FromTable() { TableAlias = "t1", TableName = originalTable.Name + (includeold ? "_old" : ""), TableSchema = originalTable.Schema }
            };
            String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(copiedTableSchema.Schema, copiedTableSchema.Name, select);
            return copyTableSQL;
            //con.ExecuteNonQuerySQL(copyTableSQL, c);
        }
    }
}
