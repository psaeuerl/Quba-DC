using QubaDC.Hybrid;
using QubaDC.Separated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Tests
{
    public class HybridQBDCFixture : MySqlDBFixture
    {
        public HybridQBDCFixture() : base()
        {
            QubaDCSystem c = new MySQLQubaDCSystem(
                this.DataConnection,
                new HybridSMOHandler()
                , new HybridCRUDHandler()
                , new HybridQSSelectHandler()
                );
            this.QBDCSystem = c;
        }


        public QubaDCSystem QBDCSystem { get; private set; }

        public override void Dispose()
        {
            base.Dispose();
        }

    }
}
