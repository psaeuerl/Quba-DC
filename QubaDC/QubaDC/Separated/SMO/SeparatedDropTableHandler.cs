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

namespace QubaDC.Separated.SMO
{
    public class SeparatedDropTableHandler
    {
        private SchemaManager schemaManager;

        public SeparatedDropTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer, TableMetadataManager metaManager)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
            this.MetaManager = metaManager;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }
        public TableMetadataManager MetaManager { get; private set; }


        internal void Handle(DropTable dropTable)
        {
            //What to do here?
            //a.) Drop Table
            //b.) Schemamanager => RemoveTable            

            Func<SchemaInfo, UpdateSchema> f = (currentSchemaInfo) =>
            {
                TableSchemaWithHistTable originalTable = currentSchemaInfo.Schema.FindTable(dropTable.Schema, dropTable.TableName);

                String dropTableSql = SMORenderer.RenderDropTable(dropTable.Schema, dropTable.TableName);
                String dropOriginalMetaTable = SMORenderer.RenderDropTable(originalTable.MetaTableSchema, originalTable.MetaTableName);

                Schema x = currentSchemaInfo.Schema;
                Table oldTable = new Table() { TableSchema = dropTable.Schema, TableName = dropTable.TableName };

                x.RemoveTable(oldTable);

                String[] Statements = new String[]
                {
                    dropTableSql,
                    dropOriginalMetaTable
                };

                return new UpdateSchema()
                {
                    newSchema = currentSchemaInfo.Schema,
                    UpdateStatements = Statements,
                    MetaTablesToLock = new Table[] { originalTable.ToTable()  },
                    TablesToUnlock = new Table[] { }
                };
            };


            SeparatedSMOExecuter.Execute(
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
