using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;

namespace QubaDC
{
    public abstract class QueryStoreSelectHandler
    {
        public abstract QueryStoreSelectResult HandleSelect(SelectOperation s, SchemaManager schemaManager, DataConnection dataConnection, GlobalUpdateTimeManager timeManager, CRUDVisitor cRUDHandler, QueryStore qs);
        internal abstract QueryStoreReexecuteResult ReExecuteSelectFor(Guid gUID, QueryStore qs, DataConnection con, CRUDVisitor cRUDHandler, SchemaManager schemaManager);
    }
}
