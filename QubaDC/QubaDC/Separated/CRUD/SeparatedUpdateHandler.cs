using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Separated.CRUD
{
    class SeparatedUpdateHandler
    {
        public SeparatedUpdateHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender)
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
            //Actually, just insert the statement
            String insertIntoBaseTable = this.CRUDRenderer.RenderUpdate(updateOperation.Table, updateOperation.ColumnNames, updateOperation.ValueLiterals, updateOperation.Restriction);
            this.DataConnection.ExecuteQuery(insertIntoBaseTable);
            
        }
    }
}
