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
            : base(con, separatedSMOHandler, separatedCRUDHandler)
        {
            ;
        }
    }
}
