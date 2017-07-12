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
    public class HybridBasicTests : SystemTests.SystemBasicTests
    {

        public HybridBasicTests() : base()
        {
            HybridQBDCFixture f = new HybridQBDCFixture();
            this.HybridFixture = f;
            this.Fixture = f;
            base.Init();
        }

        public HybridQBDCFixture HybridFixture { get; private set; }

        public override string BuildDataBaseName()
        {
            //Create Empty Schema
            return "HybridTests_" + Guid.NewGuid().ToString().Replace("-", "");
        }

        public override QubaDCSystem BuildSystem()
        {
            return HybridFixture.QBDCSystem;
        }
    }
}
