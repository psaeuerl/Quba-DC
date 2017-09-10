using QubaDC.CRUD;
using QubaDC.Restrictions;
using QubaDC.Separated;
using QubaDC.SMO;
using QubaDC.Tests.CustomAsserts;
using QubaDC.Tests.DataBuilder;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.Separated
{
    public class IntegratedQueryStoreSMOTests :  SystemQueryStoreSMOTests
    {

        public IntegratedQueryStoreSMOTests() : base()
        {
            IntegratedQBDCFixture f = new IntegratedQBDCFixture();
            this.SeparatedFixture = f;
            this.Fixture = f;
            base.Init();
            base.CheckTriggersCopied = false;
        }

        public IntegratedQBDCFixture SeparatedFixture { get; private set; }

        public override string BuildDataBaseName()
        {
            //Create Empty Schema
            return "Integrated_" + Guid.NewGuid().ToString().Replace("-", "");
        }

        public override QubaDCSystem BuildSystem()
        {
            return SeparatedFixture.QBDCSystem;
        }
    }
}
