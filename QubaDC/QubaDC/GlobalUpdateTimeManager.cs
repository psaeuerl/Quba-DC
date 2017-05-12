using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public abstract class GlobalUpdateTimeManager
    {
        public abstract String GetCreateUpdateTimeTableStatement();
        public abstract GlobalUpdate GetLatestUpdate();
    }
}
