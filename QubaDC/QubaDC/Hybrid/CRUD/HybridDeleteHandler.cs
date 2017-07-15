using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Separated.CRUD
{
    class HybridDeleteHandler
    {
        public HybridDeleteHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public SchemaManager SchemaManager { get; private set; }

        internal void HandleDelete(DeleteOperation deleteOperation)
        {
            //Actually, just insert the statement
            String insertIntoBaseTable = this.CRUDRenderer.RenderDelete(deleteOperation.Table, deleteOperation.Restriction);
            this.DataConnection.ExecuteQuery(insertIntoBaseTable);
            
        }
    }
}
