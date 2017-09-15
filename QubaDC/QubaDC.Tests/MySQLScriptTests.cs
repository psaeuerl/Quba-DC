using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests
{
    public class MySQLScriptTests
    {
        [Fact]
        public void canConnect()
        {
            MySQLDataConnection c = new MySQLDataConnection()
            {
                Credentials = new System.Net.NetworkCredential("root", "rootpw"),
                Server = "localhost",
                DataBase = "development"
            };
            DataTable result = new DataTable();

            c.AquiereOpenConnection(oc =>
            {
                String lockTables = "LOCK TABLES development.basetable READ;";
                MySqlCommand lockTablesCommand = new MySqlCommand(lockTables, (MySqlConnection)oc);
                int locktablesStatus = lockTablesCommand.ExecuteNonQuery();


                String select = "SELECT (`basetable_ref`.`ID`), (`basetable_ref`.`someString`) FROM `development`.`basetable` AS `basetable_ref`  ORDER BY `basetable_ref`.`ID` ASC, `basetable_ref`.`someString` ASC;";
         //       MySqlScript m = new MySqlScript((MySql.Data.MySqlClient.MySqlConnection)oc, select);
                MySqlCommand cmd = new MySqlCommand(select, (MySqlConnection)oc);
                var Reader = cmd.ExecuteReader();
                result.Load(Reader);
            });
            ;

        }
    }
}
