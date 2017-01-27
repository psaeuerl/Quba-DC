using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class QubaDCSystem
    {
        public QubaDCSystem(DataConnection connection, SMOVisitor SMOHandler)
        {
            this.DataConnection = connection;
            this.SMOHandler = SMOHandler;
        }

        public SMOVisitor SMOHandler { get; private set; }
        public DataConnection DataConnection { get; private set; }
    }
}
