using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
        public abstract void ExecuteNonQuerySQL(string SQL, DbConnection openconnection);


        public abstract Table[] GetAllTables();

        public abstract DataTable ExecuteQuery(String SQL);

        public abstract void DoTransaction(Action<DbTransaction,DbConnection> p);

        /// <summary>
        /// Returns ID of last inserted Row or null
        /// </summary>
        public abstract long? ExecuteInsert(string statement);

        public abstract long? ExecuteInsert(string statement, DbConnection c);

    }
}
