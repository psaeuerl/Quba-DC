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
        private readonly TableLastUpdateManager GlobalUpdateTimeManager;
        private SchemaManager schemaManager;

        public IntegratedAddColumnHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer, TableLastUpdateManager manager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.GlobalUpdateTimeManager = manager;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(AddColum alterColumn)
        {
            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data
            var con = (MySQLDataConnection)DataConnection;
            con.DoTransaction((transaction, c) =>
            {

                SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
                Schema currentSchema = xy.Schema;


                TableSchemaWithHistTable originalTable = xy.Schema.FindTable(alterColumn.Schema, alterColumn.TableName);
                TableSchema originalHistTable = xy.Schema.FindHistTable(originalTable.Table.ToTable());

                var copiedTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Union(new String[] { alterColumn.Column.ColumName }).ToArray(),
                    Name = originalTable.Table.Name,
                    Schema = originalTable.Table.Schema,
                    ColumnDefinitions = originalTable.Table.ColumnDefinitions.Union(new ColumnDefinition[] { alterColumn.Column }).ToArray(),
                };
                var copiedHistSchema = new TableSchema()
                {
                    Columns = copiedTableSchema.Columns.Union(originalHistTable.Columns.Except(copiedTableSchema.Columns)).ToArray(),
                    Name = originalTable.Table.Name + "_" + xy.ID,
                    Schema = originalTable.Table.Schema,
                    ColumnDefinitions = copiedTableSchema.ColumnDefinitions.Union(originalHistTable.ColumnDefinitions
                                            .Where(x => copiedTableSchema.Columns.Contains(x.ColumName) == false)).ToArray()
                };


                Guard.StateTrue(copiedTableSchema.Columns.Count() == originalTable.Table.Columns.Count() + 1, "Could add new column: " + alterColumn.Column);
                Guard.StateTrue(copiedHistSchema.Columns.Count() == originalHistTable.Columns.Count() + 1, "Could add new column: " + alterColumn.Column);
                Guard.StateTrue(copiedTableSchema.ColumnDefinitions.Count() == originalTable.Table.ColumnDefinitions.Count() + 1, "Could add new column: " + alterColumn.Column);
                Guard.StateTrue(copiedHistSchema.ColumnDefinitions.Count() == originalHistTable.ColumnDefinitions.Count() + 1, "Could add new column: " + alterColumn.Column);

                currentSchema.RemoveTable(originalTable.Table.ToTable());
                currentSchema.AddTable(copiedTableSchema, copiedHistSchema);



                String dropOriginalHistTable = SMORenderer.RenderDropTable(originalHistTable.Schema, originalHistTable.Name);
                con.ExecuteNonQuerySQL(dropOriginalHistTable);

                String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
                {
                    NewSchema = originalTable.HistTableSchema,
                    NewTableName = originalTable.HistTableName,
                    OldSchema = originalTable.Table.Schema,
                    OldTableName = originalTable.Table.Name
                });
                con.ExecuteNonQuerySQL(renameTableSQL);



                CopyTable(c, con, originalHistTable, copiedTableSchema, false);
                AlterTable(c, con, copiedTableSchema, alterColumn.Column);
                AlterTable(c, con, copiedTableSchema, IntegratedConstants.GetHistoryTableColumns()[0]); //STARTTS
                AlterTable(c, con, copiedTableSchema, IntegratedConstants.GetHistoryTableColumns()[1]); //ENDTS
                CopyTable(c, con, copiedTableSchema, copiedHistSchema, false);
                




                ////Insert data from old to new
                SelectOperation s = new SelectOperation()
                {
                    Columns = originalTable.Table.Columns.Select(x => new ColumnReference() { ColumnName = x, TableReference = "t1" }).ToArray(),
                    LiteralColumns = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = alterColumn.InitalValue, ColumnName = alterColumn.Column.ColumName },
                        new LiteralColumn() { ColumnLiteral = "NOW(3)", ColumnName = IntegratedConstants.StartTS },
                        new LiteralColumn() { ColumnLiteral = "NULL", ColumnName = IntegratedConstants.EndTS }},
                    FromTable = new FromTable() { TableAlias = "t1", TableName = originalHistTable.Name , TableSchema = originalHistTable.Schema },
                    Restriction = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction("t1", "NOW(3)")

                };
                String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);

                TableSchema isnertWithStartts = new TableSchema()
                {
                    Columns = copiedTableSchema.Columns.Concat(new String[] {IntegratedConstants.StartTS,IntegratedConstants.EndTS}).ToArray(),
                    Name = copiedTableSchema.Name,
                    Schema = copiedTableSchema.Schema
                };
                String insertFromTable = SMORenderer.RenderInsertToTableFromSelect(isnertWithStartts, select);
                con.ExecuteNonQuerySQL(insertFromTable);
                this.schemaManager.StoreSchema(currentSchema, alterColumn, con, c);


                //Make Global Update

                transaction.Commit();
            });

        }

        private void AlterTable(System.Data.Common.DbConnection c, MySQLDataConnection con, TableSchema copiedTableSchema, ColumnDefinition column)
        {
            String statement = this.SMORenderer.RenderAddColumn(copiedTableSchema, column);
            con.ExecuteNonQuerySQL(statement, c);
        }

        private void CopyTable(System.Data.Common.DbConnection c, MySQLDataConnection con, TableSchema originalTable, TableSchema copiedTableSchema, Boolean includeold)
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
            con.ExecuteNonQuerySQL(copyTableSQL, c);
        }
    }
}
