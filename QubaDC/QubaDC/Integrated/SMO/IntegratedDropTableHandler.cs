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
using QubaDC.Integrated.SMO;

namespace QubaDC.Separated.SMO
{
    public class IntegratedDropTableHandler
    {
        private SchemaManager schemaManager;

        public IntegratedDropTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager MetaManager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = MetaManager;
        }

        public DataConnection DataConnection { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(DropTable dropTable)
        {
            //What to do here?
            //a.) Drop Table
            //b.) Schemamanager => RemoveTable            



            Func<SchemaInfo, UpdateSchema> f = (currentSchemainfo) =>
            {




                TableSchemaWithHistTable originalTable = currentSchemainfo.Schema.FindTable(dropTable.Schema, dropTable.TableName);
                TableSchema originalHistTable = currentSchemainfo.Schema.FindHistTable(originalTable.Table.ToTable());

                String dropOriginalHistTable = SMORenderer.RenderDropTable(originalHistTable.Schema, originalHistTable.Name);

                String renameTableSQL = SMORenderer.RenderRenameTable(new RenameTable()
                {
                    NewSchema = originalTable.HistTableSchema,
                    NewTableName = originalTable.HistTableName,
                    OldSchema = originalTable.Table.Schema,
                    OldTableName = originalTable.Table.Name
                });

                String dropOriginalMetaTable = SMORenderer.RenderDropTable(originalTable.MetaTableSchema, originalTable.MetaTableName);

                Schema x = currentSchemainfo.Schema;
                Table oldTable = new Table() { TableSchema = dropTable.Schema, TableName = dropTable.TableName };
                x.RemoveTable(oldTable);

                String[] Statements = new String[]
                {
                    dropOriginalHistTable,
                    renameTableSQL
                    ,dropOriginalMetaTable

                };

                return new UpdateSchema()
                {
                    newSchema = currentSchemainfo.Schema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { originalTable.ToTable() },
                    TablesToUnlock = new Table[] { }
                };
            };


            IntegratedSMOExecuter.Execute(
                this.SMORenderer,
                this.DataConnection,
                 this.schemaManager,
                 dropTable,
                 f,
                 (s) => System.Diagnostics.Debug.WriteLine(s)
                 , MetaManager);
        }

    }
}
