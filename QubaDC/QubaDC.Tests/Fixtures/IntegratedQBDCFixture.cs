using QubaDC.Integrated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Tests
{
    public class IntegratedQBDCFixture : MySqlDBFixture
    {
        public IntegratedQBDCFixture() : base()
        {
            QubaDCSystem c = new MySQLQubaDCSystem(
                this.DataConnection,
                new IntegratedSMOHandler()
                , new IntegratedCRUDHandler()
                , new IntegratedQSSelectHandler()
                , new IntegratedMySqlSMORenderer()
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
