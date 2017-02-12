﻿using MySql.Data.MySqlClient;
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
            this.AquireOpenConnection(con => { MySqlCommand com = con.CreateCommand();
                com.CommandType = System.Data.CommandType.Text;
                com.CommandText = SQL;
                com.ExecuteNonQuery();
            });
        }

        public override Table[] GetAllTables()
        {
            String query = String.Format("select TABLE_SCHEMA, TABLE_NAME from information_schema.tables WHERE TABLE_SCHEMA = '{0}'", this.DataBase);
            var result = this.ExecuteQuery(query)
                .Select()
                .Select(x => 
                new Table
                    {
                    Name = x["TABLE_NAME"].ToString(),
                    Schema = x["TABLE_SCHEMA"].ToString() }
                ).ToArray();
            return result;
        }

        public override DataTable ExecuteQuery(string SQL)
        {
            DataTable result = new DataTable();
            this.AquireOpenConnection(con => {
                MySqlCommand com = con.CreateCommand();
                com.CommandType = System.Data.CommandType.Text;
                com.CommandText = SQL;
                using (MySqlDataReader reader = com.ExecuteReader())
                {
                    result.Load(reader);
                }
            });
            return result;
        }
    }
}
