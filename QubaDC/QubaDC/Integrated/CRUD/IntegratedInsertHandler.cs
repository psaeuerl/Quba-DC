using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    class IntegratedInsertHandler
    {
        public IntegratedInsertHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender)
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
            
        }
    }
}
