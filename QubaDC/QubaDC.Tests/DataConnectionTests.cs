using QubaDC;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Quba_DC.Tests
{
    public class DataConnectionTests
    {

        [Fact]
        public void CheckConnectionIsWorking()
        {
            MySQLDataConnection c = GetConnection();
            c.CheckConnection();
        }

        private static MySQLDataConnection GetConnection()
        {
            return new MySQLDataConnection()
            {
                Credentials = new System.Net.NetworkCredential("root", "rootpw"),
                Server = "localhost",
                DataBase = "mysql"
            };
        }

        [Fact]
        public void NonExistingDatabaseThrowsException()
        {
            var con = GetConnection();
            con.DataBase = "Non_Existing";
            Assert.ThrowsAny<InvalidOperationException>(() => con.CheckConnection());
        }

        [Fact]      
        public void WrongServerAtCheckConnectionThrowsException()
        {
            MySQLDataConnection c = GetConnection();
            c.Server = "Localhost2";
            //Why SystemException? MySQL Driver is bad implemented and does not inherit from DBException
            Assert.ThrowsAny<InvalidOperationException>(() => c.CheckConnection());
        }

        [Fact]
        public void WrongDatabaseAtCheckConnectionWorks()
        {
            MySQLDataConnection c = GetConnection();
            //Why does this work? MySQL Driver allows to set a database
            //Database == Schema in MySQL
            //And it seems i can connect without specifying an existing database
            c.CheckConnection();
        }
    }
}
