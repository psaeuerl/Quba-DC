using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public abstract class DataConnection
    {
        /// <summary>
        /// Checks if a Connection can be opened
        /// </summary>
        public abstract void CheckConnection();

        public abstract void ExecuteNonQuerySQL(string SQL);

        public abstract Table[] GetAllTables();

        public abstract DataTable ExecuteQuery(String SQL);
    }
}
