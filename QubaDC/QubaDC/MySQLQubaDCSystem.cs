using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySQLQubaDCSystem : QubaDCSystem
    {
        public MySQLQubaDCSystem(MySQLDataConnection con, SMOVisitor separatedSMOHandler
            , CRUDVisitor separatedCRUDHandler, QueryStoreSelectHandler selecthandler
            , SMORenderer renderer
            )
            : base(con, separatedSMOHandler, 
                  separatedCRUDHandler, 
                  new MySqlQueryStore(con, selecthandler),
                  new MySqlSchemaManager(con),
                  renderer,
                  new MySQLCrudRenderer(),
                  new MySQLGlobalUpdateTimeManager(con))
        {
            this.TypedConnection = con; ;
        }

        public MySQLDataConnection TypedConnection { get; private set; }

    }
}
