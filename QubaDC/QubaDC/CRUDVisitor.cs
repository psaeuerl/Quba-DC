using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using System.Data;

namespace QubaDC
{
    public abstract class CRUDVisitor
    {
        public CRUDRenderer CRUDRenderer { get; set; }
        public DataConnection DataConnection { get; set; }
        public SchemaManager SchemaManager { get; set; }

        public abstract void HandleDeletOperation(DeleteOperation deleteOperation);

        public abstract void HandleInsert(InsertOperation insertOperation);

        public abstract String RenderSelectOperation(SelectOperation selectOperation);

        internal abstract string RenderHashSelect(SelectOperation newOperation);
        public abstract void HandleUpdateOperation(UpdateOperation c2);
    }
}
