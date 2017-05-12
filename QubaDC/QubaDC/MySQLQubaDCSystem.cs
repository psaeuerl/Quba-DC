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
            , CRUDVisitor separatedCRUDHandler , QueryStoreSelectHandler selecthandler
            )
            : base(con, separatedSMOHandler, 
                  separatedCRUDHandler, 
                  new MySqlQueryStore(con, selecthandler),
                  new MySqlSchemaManager(con),
                  new MySqlSMORenderer(),
                  new MySQLCrudRenderer())
        {
            this.TypedConnection = con; ;
        }

        public MySQLDataConnection TypedConnection { get; private set; }

    }
}
