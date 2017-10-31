using System;
using QubaDC.CRUD;
using System.Linq;

namespace QubaDC.Evaluation
{
    internal class InsertGenerator
    {

        internal static InsertOperation[] GenerateFor(int phaseNumber, int inserts, String dbName)
        {
            Random r = new Random(10);
            var res = Enumerable.Range(0, inserts).Select(x => new InsertOperation()
            {
                ColumnNames = EvaluationCreateTable.GetTable(dbName).Columns.Select(y => y.ColumName).ToArray(),
                 InsertTable = EvaluationCreateTable.GetTable(dbName).ToTable(),
                  ValueLiterals = new string[]
                  {
                       phaseNumber.ToString(),
                       x.ToString(),
                       r.Next(3).ToString(),
                       5.ToString(),
                       "'"+RandomString(10000)+"'"
                  }              

            });
            var all = res.ToArray();
            return all;
        }
        private static Random random = new Random(5);
        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }


    }
}