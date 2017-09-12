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

namespace QubaDC.Separated.SMO
{
    class IntegratedJoinTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedJoinTableHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(JoinTable jointable)
        {

            //What to do here?
            //a.) Copy table
            //b.) Add table to the Schemamanager
            //c.) Delete Trigger to the table
            //d.) Recreate Trigger on the table with correct hist table
            //e.) Copy Data twice!

            var con = (MySQLDataConnection)DataConnection;
            con.DoTransaction((transaction, c) =>
            {

                SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
                Schema currentSchema = xy.Schema;


                TableSchemaWithHistTable firstTable = xy.Schema.FindTable(jointable.FirstSchema, jointable.FirstTableName);
                TableSchema firstHistTable = xy.Schema.FindHistTable(firstTable.Table.ToTable());

                TableSchemaWithHistTable secondTable = xy.Schema.FindTable(jointable.FirstSchema, jointable.SecondTableName);
                TableSchema secondHistTable = xy.Schema.FindHistTable(secondTable.Table.ToTable());


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
                    Name = jointable.ResultTableName + "_" + xy.ID,
                    Schema = jointable.ResultSchema,
                    ColumnDefinitions = firstTable.Table.ColumnDefinitions.Union(secondHistTable.ColumnDefinitions).GroupBy(x => x.ColumName).Select(x => x.First()).ToArray()
                };
                currentSchema.AddTable(joinedTableSchema, joinedTableHistSchema);
                currentSchema.RemoveTable(firstTable.Table.ToTable());
                currentSchema.RemoveTable(secondTable.Table.ToTable());

                CreateJoinedTable(c, con, firstTable.Table, secondTable.Table, joinedTableSchema);
                CreateJoinedTable(c, con, firstTable.Table, secondTable.Table, joinedTableHistSchema);

                ////Insert data from old to new
                var RestrictionT1 = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(jointable.FirstTableAlias, "NOW(3)");
                var RestrictionT2 = Integrated.SMO.IntegratedSMOHelper.GetBasiRestriction(jointable.SecondTableAlias, "NOW(3)");
                var Restriction = new AndRestriction() { Restrictions = new QubaDC.Restriction[] { RestrictionT1, RestrictionT1, jointable.JoinRestriction } };

                var StartEndTs = new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = "NOW(3)", ColumnName = IntegratedConstants.StartTS },
                                        new LiteralColumn() { ColumnLiteral = "NULL", ColumnName = IntegratedConstants.EndTS } };

                TableSchema isnertWithStartts = new TableSchema()
                {
                    Columns = joinedTableSchema.Columns.Concat(new String[] { IntegratedConstants.StartTS, IntegratedConstants.EndTS }).ToArray(),
                    Name = joinedTableSchema.Name,
                    Schema = joinedTableSchema.Schema
                };
                String select = CreateSelectForTables(firstTable.Table, secondTable.Table, jointable.FirstTableAlias, jointable.SecondTableAlias, Restriction, StartEndTs);
                String insertFromFirstTable = SMORenderer.RenderInsertToTableFromSelect(isnertWithStartts, select);
                con.ExecuteNonQuerySQL(insertFromFirstTable);

                DropHistTableRenameCurrentToHist(con, firstTable);
                DropHistTableRenameCurrentToHist(con, secondTable);
               

                this.schemaManager.StoreSchema(currentSchema, jointable, con, c);

                transaction.Commit();
            });


        }

        private void DropHistTableRenameCurrentToHist(MySQLDataConnection con, TableSchemaWithHistTable firstTable)
        {
            String dropOriginalHistTable = SMORenderer.RenderDropTable(firstTable.HistTableSchema, firstTable.HistTableName);
            con.ExecuteNonQuerySQL(dropOriginalHistTable);
            String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
            {
                NewSchema = firstTable.HistTableSchema,
                NewTableName = firstTable.HistTableName,
                OldSchema = firstTable.Table.Schema,
                OldTableName = firstTable.Table.Name
            });
            con.ExecuteNonQuerySQL(renameTableSQL);
        }

        private void CreateJoinedTable(System.Data.Common.DbConnection c, MySQLDataConnection con, TableSchema firstTable, TableSchema secondTable, TableSchema joinedTableSchema)
        {
            string select = CreateSelectForTables(firstTable, secondTable, "t1", "t2", new OperatorRestriction()
            {
                LHS = new LiteralOperand() { Literal = "1" },
                Op = RestrictionOperator.Equals,
                RHS = new LiteralOperand() { Literal = "2" }
            }, new LiteralColumn[] { new LiteralColumn() { ColumnLiteral = "t1." + IntegratedConstants.StartTS, ColumnName = IntegratedConstants.StartTS },
                                        new LiteralColumn() { ColumnLiteral = "t1."+IntegratedConstants.EndTS, ColumnName = IntegratedConstants.EndTS } });

            ////Copy Table without Triggers
            String copyTableSQL = SMORenderer.RenderCopyTable(joinedTableSchema.Schema, joinedTableSchema.Name, select);
            con.ExecuteNonQuerySQL(copyTableSQL, c);
        }

        private string CreateSelectForTables(TableSchema firstTable, TableSchema secondTable, String firstTableRename, String SecondTableRename, Restriction r, LiteralColumn[] literalColumns)
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
                LiteralColumns = literalColumns,
                Restriction = r
            };

            String select = this.SMORenderer.CRUDHandler.RenderSelectOperation(s);
            return select;
        }
    }
}
