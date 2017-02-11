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
        private MySQLDataConnection mySQLDataConnection;
        private SeparatedSMOHandler separatedSMOHandler;
        private SeparatedCRUDHandler separatedCRUDHandler;

        public QubaDCSystem(DataConnection connection, SMOVisitor SMOHandler)
        {
            this.DataConnection = connection;
            this.SMOHandler = SMOHandler;
        }

        public QubaDCSystem(MySQLDataConnection mySQLDataConnection, SeparatedSMOHandler separatedSMOHandler, SeparatedCRUDHandler separatedCRUDHandler)
        {
            this.mySQLDataConnection = mySQLDataConnection;
            this.separatedSMOHandler = separatedSMOHandler;
            this.separatedCRUDHandler = separatedCRUDHandler;
        }
    

        public SMOVisitor SMOHandler { get; private set; }
        public DataConnection DataConnection { get; private set; }
    }
}
