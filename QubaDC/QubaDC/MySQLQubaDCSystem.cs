using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySQLQubaDCSystem : QubaDCSystem
    {
        public MySQLQubaDCSystem(MySQLDataConnection con, SMOVisitor separatedSMOHandler, CRUDVisitor separatedCRUDHandler)
            : base(con, separatedSMOHandler, separatedCRUDHandler, 
                  new MySqlQueryStore(con),
                  new MySqlSchemaManager(con),
                  new MySqlSMORenderer(),
                  new MySQLCrudRenderer())
        {
            this.TypedConnection = con; ;
        }

        public MySQLDataConnection TypedConnection { get; private set; }

    }
}
