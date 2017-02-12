using QubaDC.Separated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Tests
{
    public class SeparatedQBDCFixture : MysqlDBFixture
    {
        public SeparatedQBDCFixture() : base()
        {
            QubaDCSystem c = new QubaDCSystem(
                this.DataConnection,
                  new SeparatedSMOHandler(),
                  new SeparatedCRUDHandler()
                );
            this.QBDCSystem = c;
            //Create Empty Schema
            this.CreateEmptyDatabase("SeperatedTests");
            this.DataConnection.UseDatabase("SeperatedTests");
            //Init the System
            c.Init();
        }


        public QubaDCSystem QBDCSystem { get; private set; }

        public override void Dispose()
        {
            base.Dispose();
        }

    }
}
