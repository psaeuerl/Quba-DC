using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;

namespace QubaDC
{
    public abstract class TableLastUpdateManager
    {
        public abstract String GetCreateUpdateTimeTableStatement();
        public abstract TableLastUpdate GetLatestUpdate();
        public abstract string GetTableName();
        public abstract Table GetTable();
    }
}
