using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Separated.CRUD
{
    class SeparatedInsertHandler
    {
        public SeparatedInsertHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public SchemaManager SchemaManager { get; private set; }

        internal void HandleInsert(InsertOperation insertOperation)
        {

            //Actually, just insert the statement
            String insertIntoBaseTable = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);
            this.DataConnection.ExecuteQuery(insertIntoBaseTable);
            //What happes in the Insert Operation?

            //a => get the current schema
            //b => build insert statement for the history table
            //Insert both
            //this.DataConnection.DoTransaction((trans, con) =>
            //{
            //    String insertIntoBaseTable = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);

            //    SchemaInfo s = this.SchemaManager.GetCurrentSchema(con);
            //    Table histTable = s.Schema.FindHistTable(insertOperation.InsertTable);

            //    List<String> histcolumns = new List<string>();
            //    histcolumns.AddRange(insertOperation.ColumnNames);
            //    histcolumns.AddRange(SeparatedConstants.GetHistoryTableColumns().Select(x => x.ColumName));
            //    String[] histColumnsFinal = histcolumns.ToArray();

            //    List<String> histValues = new List<string>();
            //    histValues.AddRange(insertOperation.ValueLiterals);
            //    histValues.AddRange(new String[] {
            //    CRUDRenderer.SerializeDateTime(DateTime.Now),
            //       "NULL",
            //       CRUDRenderer.SerializeString(Guid.NewGuid().ToString())
            //    });
            //    String[] histValuesFinal = histValues.ToArray();

            //    String insertIntoHistTable = this.CRUDRenderer.RenderInsert(histTable, histColumnsFinal, histValuesFinal);
            //    this.DataConnection.ExecuteQuery(insertIntoBaseTable, con);
            //    this.DataConnection.ExecuteQuery(insertIntoHistTable, con);
            //    trans.Commit();
            //});
        }
    }
}
