using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using QubaDC.Utility;
using QubaDC.DatabaseObjects;
using System.Data;

namespace QubaDC
{
    public class MySQLDataConnection : DataConnection
    {
        public NetworkCredential Credentials { get; set; } 
        public String Server { get; set; }

        public MySQLDataConnection Clone()
        {
            return new MySQLDataConnection()
            {
                Credentials = new NetworkCredential(this.Credentials.UserName, this.Credentials.Password),
                DataBase = this.DataBase,
                Server = this.Server
            };
        }

        public override void DoTransaction(Action<DbTransaction, DbConnection> p)
        {
            this.AquireOpenConnection((con) =>
            {
                using (MySqlTransaction trans = con.BeginTransaction(IsolationLevel.Serializable))
                {
                    p(trans, con);
                }
            });
        }

        public String DataBase { get; set; }

        public override void CheckConnection()
        {
            AquireOpenConnection();
        }

        private void AquireOpenConnection()
        {
            AquireOpenConnection((t) => {; });
        }

        private void AquireOpenConnection(Action<MySqlConnection> action)
        {
            string connStr = CreateConnectionString();
            MySqlConnection con = null;
            try
            {
                con = new MySqlConnection(connStr);
                con.Open();
                con.ChangeDatabase(DataBase);
                action.Invoke(con);

            }catch(MySqlException ex)
            {
                throw new InvalidOperationException(ex.Message, ex);
            }
            finally
            {
                if (con != null)
                    con.Close();
            }
        }

        public void UseDatabase(string database)
        {
            this.DataBase = database;
            this.CheckConnection();
        }

        private string CreateConnectionString()
        {
            Guard.ArgumentNotNullOrWhiteSpace(Server, nameof(Server));
            Guard.ArgumentNotNull(Credentials, nameof(Credentials));
            Guard.ArgumentNotNullOrWhiteSpace(Credentials.UserName, nameof(Credentials.UserName));
            Guard.ArgumentNotNullOrWhiteSpace(Credentials.Password, nameof(Credentials.Password));
            Guard.ArgumentNotNullOrWhiteSpace(DataBase, nameof(DataBase));
            string connStr = String.Format("server={0};user id={1}; password={2}; database=mysql; pooling=false",
            Server, Credentials.UserName, Credentials.Password);
            return connStr;
        }

        public override void ExecuteNonQuerySQL(string SQL)
        {
            this.AquireOpenConnection(con => {
                ExecuteNonQuerySQL(SQL, con);
            });
        }

        public override void ExecuteNonQuerySQL(string SQL, DbConnection openconnection)
        {
                MySqlCommand com = (MySqlCommand)openconnection.CreateCommand();
                com.CommandType = System.Data.CommandType.Text;
                com.CommandText = SQL;            
                com.ExecuteNonQuery();
        }

        public override void ExecuteSQLScript(string SQL, DbConnection openconnection)
        {
            //Otherwise, Delimiter is not support @ MySQL and we need it for Triggers
            MySqlScript m = new MySqlScript((MySql.Data.MySqlClient.MySqlConnection) openconnection, SQL);
            m.Execute();
        }

        public override TableSchema[] GetAllTables()
        {
            String query = String.Format("select TABLE_SCHEMA, TABLE_NAME from information_schema.tables WHERE TABLE_SCHEMA = '{0}'", this.DataBase);
            var result = this.ExecuteQuery(query)
                .Select()
                .Select(x => 
                new TableSchema()
                    {
                    Name = x["TABLE_NAME"].ToString(),
                    Schema = x["TABLE_SCHEMA"].ToString() }
                ).ToArray();
            return result;
        }

        public override long? ExecuteInsert(string statement)
        {
            long? result = null;
            this.AquireOpenConnection(con =>
            {
                result =  this.ExecuteInsert(statement, con);
            });
            return result;
        }

        public override long? ExecuteInsert(string statement, DbConnection c)
        {
            MySqlCommand cmd = (MySqlCommand)c.CreateCommand();
            cmd.CommandText = statement;
            cmd.CommandType = CommandType.Text;
            long lastinsertedBefore = cmd.LastInsertedId;
            long inserted = cmd.ExecuteNonQuery();
            long newId = cmd.LastInsertedId;
            var yx = cmd.UpdatedRowSource;
            return lastinsertedBefore == newId? new long?(newId) : null;
        }

        public override DataTable ExecuteQuery(string SQL, DbConnection openconnection)
        {
            DataTable result = new DataTable();

                MySqlCommand com = (MySqlCommand)openconnection.CreateCommand();
                com.CommandType = System.Data.CommandType.Text;
                com.CommandText = SQL;
                using (MySqlDataReader reader = com.ExecuteReader())
                {
                    result.Load(reader);
                }

            return result;
        }

        public override DataTable ExecuteQuery(string SQL)
        {
            DataTable result = null;
            this.AquireOpenConnection(con =>
            {
                result = this.ExecuteQuery(SQL, con);
            });
            return result;
        }
    }
}
