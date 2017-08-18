using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    class IntegratedDeleteHandler
    {
        public IntegratedDeleteHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender)
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
            UpdateOperation uo = new UpdateOperation()
            {
                ColumnNames = new String[] { IntegratedConstants.EndTS },
                ValueLiterals = new String[] { "NOW(3)" },
                Restriction = deleteOperation.Restriction,
                Table = deleteOperation.Table
            };
            //Actually, just insert the statement
            String updateTableSetEndTs = this.CRUDRenderer.RenderUpdate(uo.Table, uo.ColumnNames, uo.ValueLiterals, uo.Restriction);
            this.DataConnection.ExecuteQuery(updateTableSetEndTs);
        }
    }
}
