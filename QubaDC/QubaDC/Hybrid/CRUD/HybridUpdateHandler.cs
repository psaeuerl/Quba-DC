using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Hybrid.CRUD
{
    class HybridUpdateHandler
    {
        public HybridUpdateHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public SchemaManager SchemaManager { get; private set; }

        internal void HandleUpdate(UpdateOperation updateOperation)
        {
            throw new NotImplementedException("Not Implemented");
            //Actually, just insert the statement
            String insertIntoBaseTable = this.CRUDRenderer.RenderUpdate(updateOperation.Table, updateOperation.ColumnNames, updateOperation.ValueLiterals, updateOperation.Restriction);
            this.DataConnection.ExecuteQuery(insertIntoBaseTable);
            
        }
    }
}
