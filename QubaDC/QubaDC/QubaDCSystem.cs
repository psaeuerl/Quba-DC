using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.Separated;

namespace QubaDC
{
    public abstract class QubaDCSystem
    {

        public QubaDCSystem(DataConnection mySQLDataConnection, SMOVisitor separatedSMOHandler, CRUDVisitor separatedCRUDHandler)
        {
            this.DataConnection = mySQLDataConnection;
            this.SMOHandler = separatedSMOHandler;
            this.CRUDHandler = separatedCRUDHandler;
        }

        public void Init()
        {
            //Which tables do we need?
            //
            throw new NotImplementedException();
        }

        public SMOVisitor SMOHandler { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public CRUDVisitor CRUDHandler { get; private set; }
    }
}
