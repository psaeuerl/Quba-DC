using QubaDC.Separated;
using QubaDC.SMO;
using QubaDC.Tests.DataBuilder;
using QubaDC.Tests.xUnitExtension;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.Separated
{
    public class IntegratedBasicTests : SystemTests.SystemBasicTests
    {

        public IntegratedBasicTests() : base()
        {
            IntegratedQBDCFixture f = new IntegratedQBDCFixture();
            this.IntegratedFixture = f;
            this.Fixture = f;
            base.Init();
        }

        public IntegratedQBDCFixture IntegratedFixture { get; private set; }

        public override string BuildDataBaseName()
        {
            //Create Empty Schema
            return "Integrated_" + Guid.NewGuid().ToString().Replace("-", "");
        }

        public override QubaDCSystem BuildSystem()
        {
            return IntegratedFixture.QBDCSystem;
        }
    }
}
