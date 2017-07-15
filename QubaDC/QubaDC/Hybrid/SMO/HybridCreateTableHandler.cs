using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC;
using QubaDC.DatabaseObjects;
using QubaDC.Utility;

namespace QubaDC.Hybrid.SMO
{
    public class HybridCreateTableHandler
    {
        private SchemaManager schemaManager;

        public HybridCreateTableHandler(DataConnection c, SchemaManager schemaManager,SMORenderer renderer)
        {
            this.DataConnection = c;
            this.schemaManager = schemaManager;
            this.SMORenderer = renderer;
        }

        public DataConnection DataConnection { get; private set; }
        public SMORenderer SMORenderer { get; private set; }

        internal void Handle(CreateTable createTable)
        {
            Guard.StateTrue(createTable.PrimaryKey.Length > 0, "Primary Key Requiered");

            var con = (MySQLDataConnection)DataConnection;
            ;
            con.DoTransaction((transaction, c) =>
            {
                ////What to do?
                ////Create Table normal
                ////Create Table Hist
                ////Create Trigger on normal
                ColumnDefinition startTs = HybridConstants.GetStartColumn();

                CreateTable newCt = JsonSerializer.CopyItem<CreateTable>(createTable);
                newCt.Columns = createTable.Columns.Union(new ColumnDefinition[] { HybridConstants.GetStartColumn() }).ToArray();
                String createBaseTable = SMORenderer.RenderCreateTable(newCt);
                ;
                ////Create History Table
                SchemaInfo xy = this.schemaManager.GetCurrentSchema(c);
                Schema x = xy.Schema;
                if (xy.ID == null)
                {
                    x = new Schema();
                    xy.ID = 0;
                }

                CreateTable ctHistTable = CreateHistTable(newCt, xy);

                String createHistTable = SMORenderer.RenderCreateTable(ctHistTable, true);

                //Manage Schema Statement
                x.AddTable(createTable.ToTableSchema(), ctHistTable.ToTableSchema());
                String updateSchema = this.schemaManager.GetInsertSchemaStatement(x, createTable);

                //Add tables
                con.ExecuteNonQuerySQL(createBaseTable, c);
                con.ExecuteNonQuerySQL(createHistTable, c);

                ////INsert Trigger 
                String trigger = SMORenderer.RenderCreateInsertTrigger(createTable.ToTableSchema(), ctHistTable.ToTableSchema());
                ////Delete Trigger
                String deleteTrigger = SMORenderer.RenderCreateDeleteTrigger(createTable.ToTableSchema(), ctHistTable.ToTableSchema());
                ////Update Trigger
                String UpdateTrigger = SMORenderer.RenderCreateUpdateTrigger(createTable.ToTableSchema(), ctHistTable.ToTableSchema());

                ////Add Trigger
                con.ExecuteSQLScript(trigger, c);
                con.ExecuteSQLScript(deleteTrigger, c);
                con.ExecuteSQLScript(UpdateTrigger, c);

                ////Store Schema
                con.ExecuteNonQuerySQL(updateSchema, c);
                transaction.Commit();
            });
        }

        private static CreateTable CreateHistTable(CreateTable createTable, SchemaInfo xy)
        {
            List<ColumnDefinition> columndefinitions = new List<ColumnDefinition>();
            columndefinitions.AddRange(createTable.Columns);
            columndefinitions.Add(HybridConstants.GetEndColumn());
            CreateTable ctHistTable = new CreateTable()
            {
                Columns = columndefinitions.ToArray(),
                Schema = createTable.Schema,
                TableName = createTable.TableName + "_" + xy.ID
            };
            return ctHistTable;
        }
    }
}
