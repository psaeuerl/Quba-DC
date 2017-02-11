using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.Separated;

namespace QubaDC
{
    public class QubaDCSystem
    {

        public QubaDCSystem(DataConnection connection, SMOVisitor SMOHandler)
        {
            this.DataConnection = connection;
            this.SMOHandler = SMOHandler;
        }

        public QubaDCSystem(DataConnection mySQLDataConnection, SMOVisitor separatedSMOHandler, CRUDVisitor separatedCRUDHandler)
        {
            this.DataConnection = mySQLDataConnection;
            this.SMOHandler = separatedSMOHandler;
            this.CRUDHandler = separatedCRUDHandler;
        }
    

        public SMOVisitor SMOHandler { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public CRUDVisitor CRUDHandler { get; private set; }
    }
}
