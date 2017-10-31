using QubaDC.Evaluation.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Evaluation
{
    public class DBCopier
    {
        public void CopyTable(SystemSetup system, String sourceDB, String targetDB, Boolean skipFixingSchema)
        {
            var con = (MySQLDataConnection)system.quba.DataConnection;
            //sourceDB = sourceDB.Replace("SimpleReference", "simple");
            Output.WriteLine(String.Format("Copying: {0} to {1}", sourceDB.ToLowerInvariant(), targetDB));
            CreateEmptyDB(con, targetDB);
            string strCmdText = "/C mysqldump.exe -u root --password=rootpw {0} | mysql.exe -u root --password=rootpw {1}";
            
            String actualText = String.Format(strCmdText, sourceDB, targetDB);

            //System.Diagnostics.Process.Start("CMD.exe", actualText);

            System.Diagnostics.Process cmd = new System.Diagnostics.Process();
            cmd.StartInfo.RedirectStandardInput = true;
            cmd.StartInfo.RedirectStandardOutput = true;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.UseShellExecute = false;
            cmd.StartInfo.FileName = "cmd.exe";
            cmd.StartInfo.Arguments = actualText; // copy /b Image1.jpg + Archive.rar Image2.jpg";
            cmd.Start();
            cmd.WaitForExit();
            Console.WriteLine(cmd.StandardOutput.ReadToEnd());

            if (!skipFixingSchema)
            {
                Output.WriteLine("Copied Tables - fixxing SchemaTable");
                SchemaInfo current = system.quba.SchemaManager.GetCurrentSchema();
                //modify current
                foreach (var table in current.Schema.Tables)
                {
                    table.HistTableSchema = targetDB;
                    table.Table.Schema = targetDB;
                    table.MetaTableSchema = targetDB;
                }
                foreach (var hist in current.Schema.HistTables)
                {
                    hist.Schema = targetDB;
                }

                String insert = system.quba.SchemaManager.GetInsertSchemaStatement(current.Schema, current.SMO, false);
                String actualInsert = insert.Replace("NOW(3)", MySQLDialectHelper.RenderDateTime(current.TimeOfCreation));

                String deletefromsmotable = String.Format("DELETE FROM {0}.qubadcsmotable", targetDB);
                con.ExecuteNonQuerySQL(deletefromsmotable);
                con.ExecuteNonQuerySQL(actualInsert);
            }
        }

        private void CreateEmptyDB(MySQLDataConnection dataConnection, string dbName)
        {
            try
            {
                dataConnection.ExecuteNonQuerySQL("DROP DATABASE " + dbName);
            }
            catch (InvalidOperationException ex)
            {
                var e = ex.InnerException.Message;
                if (!(e.Contains("Can't drop database '") && e.Contains("'; database doesn't exist")))
                {
                    throw ex;
                };
            }
            dataConnection.ExecuteNonQuerySQL("CREATE DATABASE " + dbName);
            dataConnection.UseDatabase(dbName);
        }
    }
}
