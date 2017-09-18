using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;
using QubaDC.Restrictions;
using QubaDC.CRUD;

namespace QubaDC.Integrated.SMO
{
    class IntegratedDecomposeTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedDecomposeTableHandler(DataConnection c, SchemaManager schemaManager, SMORenderer renderer, TableMetadataManager metaManager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = metaManager;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(DecomposeTable partitionTable)
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


                TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(partitionTable.BaseSchema, partitionTable.BaseTableName);
                TableSchema originalHistTable = currentSchemaInfo.Schema.FindHistTable(originalTable.Table.ToTable());

                var firstTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Where(x => partitionTable.FirstColumns.Contains(x) || partitionTable.SharedColumns.Contains(x)).ToArray(),
                    Name = partitionTable.FirstTableName,
                    Schema = partitionTable.FirstSchema,
                };
                firstTableSchema.ColumnDefinitions = originalTable.Table.ColumnDefinitions.Where(x => firstTableSchema.Columns.Contains(x.ColumName)).ToArray();
                var firstTableHistSchema = new TableSchema()
                {
                    Columns = firstTableSchema.Columns.Union(originalHistTable.Columns.Except(originalTable.Table.Columns)).ToArray(),
                    Name = partitionTable.FirstTableName + "_" + currentSchemaInfo.ID,
                    Schema = partitionTable.FirstSchema
                };
                firstTableHistSchema.ColumnDefinitions = originalHistTable.ColumnDefinitions.Where(x => firstTableHistSchema.Columns.Contains(x.ColumName)).ToArray();

                Table firstTableMeta = this.MetaManager.GetMetaTableFor(firstTableSchema);
                currentSchema.AddTable(firstTableSchema, firstTableHistSchema, firstTableMeta);

                var secondTableSchema = new TableSchema()
                {
                    Columns = originalTable.Table.Columns.Where(x => partitionTable.SecondColumns.Contains(x) || partitionTable.SharedColumns.Contains(x)).ToArray(),
                    Name = partitionTable.SecondTableName,
                    Schema = partitionTable.SecondSchema
                };
                secondTableSchema.ColumnDefinitions = originalTable.Table.ColumnDefinitions.Where(x => secondTableSchema.Columns.Contains(x.ColumName)).ToArray();
                var secondTableHistSchema = new TableSchema()
                {
                    Columns = secondTableSchema.Columns.Union(originalHistTable.Columns.Except(originalTable.Table.Columns)).ToArray(),
                    Name = partitionTable.SecondTableName + "_" + currentSchemaInfo.ID,
                    Schema = partitionTable.SecondSchema
                };
                secondTableHistSchema.ColumnDefinitions = originalHistTable.ColumnDefinitions.Where(x => secondTableHistSchema.Columns.Contains(x.ColumName)).ToArray();


                Table secondMetaTable = this.MetaManager.GetMetaTableFor(secondTableSchema);
                currentSchema.AddTable(secondTableSchema, secondTableHistSchema, secondMetaTable);

                ////Copy Tables without Triggers
               String[] copyTrueTable =  CreateCopiedTables(originalTable, originalHistTable, firstTableSchema, firstTableHistSchema);

                String[] copyFalseTable = CreateCopiedTables(originalTable, originalHistTable, secondTableSchema, secondTableHistSchema);





                //TODO => ADD RESTRICITON THAT ENDTS IS NULL!!!
                
                var StartEndTs = new String[] { updateTime, "NULL" };
                var restrictToActive = IntegratedSMOHelper.GetBasiRestriction(originalTable.Table.Name, updateTime);

                //Insert data from old to true                
                String insertTrueFromTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, firstTableSchema, restrictToActive, firstTableSchema.Columns, null, StartEndTs);

                //Insert data from old to true                
                String insertFromSecondTable = SMORenderer.RenderInsertFromOneTableToOther(originalTable.Table, secondTableSchema, restrictToActive, secondTableSchema.Columns, null, StartEndTs);



                String dropOriginalHistTable = SMORenderer.RenderDropTable(originalHistTable.Schema, originalHistTable.Name);

                String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
                {
                    NewSchema = originalTable.HistTableSchema,
                    NewTableName = originalTable.HistTableName,
                    OldSchema = originalTable.Table.Schema,
                    OldTableName = originalTable.Table.Name
                });

                currentSchema.RemoveTable(originalTable.Table.ToTable());

                //con.ExecuteNonQuerySQL(insertFromTable);
                // String updateLastUpdate = this.MetaManager.GetSetLastUpdateStatement(new Table() { TableName = addColumn.TableName, TableSchema = addColumn.Schema }, updateTime);

                String createFirstMetaTable = this.MetaManager.GetCreateMetaTableFor(firstTableSchema.Schema, firstTableSchema.Name);
                String createSecondMetaTable = this.MetaManager.GetCreateMetaTableFor(secondTableSchema.Schema, secondTableSchema.Name);

                String insertMetadataFirstTable =  this.MetaManager.GetStartInsertFor(firstTableSchema.Schema, firstTableSchema.Name);
                String insertMetadataSecondTable = this.MetaManager.GetStartInsertFor(secondTableSchema.Schema, secondTableSchema.Name);

                String[] Statements = copyTrueTable
                                        .Concat(copyFalseTable)
                                        .Concat(new String[]
                {
                    insertTrueFromTable,
                    insertFromSecondTable,
                    dropOriginalHistTable,
                    renameTableSQL,
                    createFirstMetaTable,
                    createSecondMetaTable,
                    insertMetadataFirstTable,
                    insertMetadataSecondTable


                }).ToArray();
                

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
                 partitionTable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , MetaManager);

        }

        private String[] CreateCopiedTables(TableSchemaWithHistTable originalTable, TableSchema originalHistTable, TableSchema normalschema, TableSchema nomralHistSchema)
        {
            List<String> stmts = new List<string>();
            String copyTrueTable = SMORenderer.RenderCopyTable(originalTable.Table.Schema, originalTable.Table.Name, normalschema.Schema, normalschema.Name);
            stmts.Add(copyTrueTable);

            String[] firstTableDropColumns = originalTable.Table.Columns.Except(normalschema.Columns).ToArray();
            String firstTableDropColumnsSQL = SMORenderer.RenderDropColumns(normalschema.Schema, normalschema.Name, firstTableDropColumns);
            stmts.Add(firstTableDropColumnsSQL);

            String copyTrueHistTableSQL = SMORenderer.RenderCopyTable(originalHistTable.Schema, originalHistTable.Name, nomralHistSchema.Schema, nomralHistSchema.Name);
            stmts.Add(copyTrueHistTableSQL);


            String[] firstHistTableDropColumns = originalHistTable.Columns.Except(nomralHistSchema.Columns).ToArray();
            String firstHistTableDropColumnsSQL = SMORenderer.RenderDropColumns(nomralHistSchema.Schema, nomralHistSchema.Name, firstTableDropColumns);
            stmts.Add(firstHistTableDropColumnsSQL);

            return stmts.ToArray();
        }
    }
}
